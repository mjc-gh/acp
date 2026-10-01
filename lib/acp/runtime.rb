# frozen_string_literal: true

require "async"
require "async/limited_queue"
require "async/queue"
require "async/semaphore"
require "digest"

module Acp
  # Monotonic time and cooperative sleeping used by the local runtime.
  class MonotonicClock
    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def sleep(duration)
      Kernel.sleep(duration) if duration.positive?
    end

    def wait(duration, &block)
      Async::Task.current.with_timeout(duration, &block)
    rescue Async::TimeoutError
      nil
    end
  end

  # In-memory ownership gate for a single runtime process.
  class LocalOwnership
    def acquire(_tenant_id)
      true
    end

    def release(_tenant_id, _token)
      nil
    end
  end

  # Bounded local polling engine. The progress adapter must implement
  # #read(tenant_id), #initialize_cursor(tenant_id, cursor), and
  # #acknowledge(tenant_id, poll_id, cursor). Initialization and acknowledgement
  # must be idempotent; acknowledgement must deduplicate by poll_id.
  # Runtime keeps these coupled pipeline lifecycles together by design.
  # rubocop:disable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
  # rubocop:disable Metrics/ParameterLists, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Metrics/BlockLength
  class Runtime
    State = Struct.new(:tenant_id, :due, :sequence, :in_flight, keyword_init: true)
    IngestMessage = Struct.new(:tenant_id, :batch, :context, :reply, keyword_init: true)

    def initialize(configuration:, progress:, ownership: LocalOwnership.new,
                   clock: MonotonicClock.new, on_error: nil, random: Random)
      @configuration = configuration
      @progress = progress
      @ownership = ownership
      @clock = clock
      @on_error = on_error
      @random = random
      @metrics = {
        active_cycles: 0,
        max_active_cycles: 0,
        active_fetches: 0,
        max_active_fetches: 0,
        queued_batches: 0,
        max_queued_batches: 0,
        active_ingests: 0,
        max_active_ingests: 0
      }
      @running = false
    end

    def metrics
      @metrics.dup.freeze
    end

    # Run until the surrounding Async task is cancelled. Discovery is performed
    # once; tenants remain scheduled in this runtime until that task stops.
    def run
      running = false
      raise Acp::RuntimeError, "runtime is already running" if @running

      @running = true
      running = true
      task = Async::Task.current
      queue = Async::LimitedQueue.new(@configuration.pipeline_capacity)
      @ingest_queue = queue
      events = Async::LimitedQueue.new(@configuration.pipeline_capacity)
      worker_count = [@configuration.ingest_concurrency, @configuration.pipeline_capacity].min
      worker_count.times { task.async { ingestion_worker(queue) } }
      states = discover
      @ownership.register_tenants(states.map(&:tenant_id)) if @ownership.respond_to?(:register_tenants)
      if @ownership.respond_to?(:heartbeat_worker)
        @ownership.heartbeat_worker(capacity: @configuration.pipeline_capacity, active: 0)
      end
      worker_heartbeat = start_worker_heartbeat
      heap = DueHeap.new
      states.each { |state| heap.push(state) }
      active = 0
      sequence = states.length

      loop do
        while active < @configuration.pipeline_capacity && !heap.empty? && heap.peek.due <= @clock.now
          state = heap.pop
          next if state.in_flight

          state.in_flight = true
          active += 1
          @metrics[:active_cycles] += 1
          @metrics[:max_active_cycles] = [@metrics[:max_active_cycles], @metrics[:active_cycles]].max
          task.async do
            due = @clock.now + @configuration.interval
            begin
              due = run_cycle(state.tenant_id)
            ensure
              @metrics[:active_cycles] -= 1
              begin
                Async::Task.current.defer_stop { events.push([state, due]) }
              rescue Async::Queue::ClosedError
                nil
              end
            end
          end
        end

        now = @clock.now
        delay = heap.empty? ? nil : [heap.peek.due - now, 0].max
        event = if delay&.positive?
                  @clock.wait(delay) { events.pop }
                else
                  events.pop
                end
        next unless event

        state, due = event
        state.in_flight = false
        state.due = due
        state.sequence = sequence
        sequence += 1
        heap.push(state)
        active -= 1
      end
    ensure
      if running
        worker_heartbeat&.stop
        queue&.close
        drain_ingest_queue(queue) if queue
        @running = false
      end
    end

    private

    def discover
      discovered = {}
      emit = lambda do |tenant_id|
        raise ArgumentError, "tenant_id cannot be nil" if tenant_id.nil?
        next if discovered.key?(tenant_id)

        now = @clock.now
        offset = initial_offset(tenant_id)
        state = State.new(tenant_id: tenant_id, due: now + offset, sequence: discovered.length, in_flight: false)
        discovered[tenant_id] = state
      end
      @configuration.callback(:tenants).call(emit)
      discovered.values
    end

    def initial_offset(tenant_id)
      identity = "#{@configuration.name}\0#{tenant_id.class.name}\0#{tenant_id}"
      sample = Digest::SHA256.digest(identity).unpack1("Q>")
      sample.fdiv(2**64) * @configuration.interval
    end

    def run_cycle(tenant_id)
      started_at = @clock.now
      ownership_token = @ownership.acquire(tenant_id)
      return @clock.now + @configuration.interval unless ownership_token

      lease_state = { lost: false }
      heartbeat = start_lease_heartbeat(tenant_id, ownership_token, lease_state)
      progress_state = read_or_initialize_cursor(tenant_id)
      cursor = progress_state.cursor
      poll_id = Context.new_poll_id
      tenant = @configuration.callback(:resolve).call(tenant_id)
      ensure_ownership!(tenant_id, ownership_token, lease_state)
      batch = retry_stage(@configuration.fetch_retry, :fetch, tenant_id) do |attempt|
        context = FetchContext.new(tenant_id: tenant_id, cursor: cursor, poll_id: poll_id, attempt: attempt)
        fetch(tenant, context)
      end
      batch.validate_after!(cursor)
      ensure_ownership!(tenant_id, ownership_token, lease_state)
      ingest(batch, tenant_id, cursor, poll_id)
      ensure_ownership!(tenant_id, ownership_token, lease_state)
      acknowledge(tenant_id, poll_id, batch.next_cursor, progress_state.revision, ownership_token)
      [started_at + @configuration.interval, @clock.now].max
    rescue LeaseLostError, ProgressConflictError => e
      report_error(tenant_id, e, :ownership)
      @clock.now + @configuration.interval
    rescue Acp::RuntimeError
      raise
    rescue StandardError => e
      report_error(tenant_id, e, :cycle)
      @clock.now + @configuration.interval
    ensure
      heartbeat&.stop
      release_ownership(tenant_id, ownership_token) if ownership_token
    end

    def read_or_initialize_cursor(tenant_id)
      if @progress.respond_to?(:read_state)
        state = @progress.read_state(tenant_id)
        return state if state

        initial = Timestamp.normalize(@configuration.callback(:initial_cursor).call(tenant_id))
        initialized = @progress.initialize_state(tenant_id, initial)
        raise Acp::RuntimeError, "progress initialization returned no cursor" if initialized.nil?

        return initialized
      end

      cursor = @progress.read(tenant_id)
      return ProgressSnapshot.new(cursor: Timestamp.normalize(cursor), revision: nil) unless cursor.nil?

      initial = Timestamp.normalize(@configuration.callback(:initial_cursor).call(tenant_id))
      initialized = @progress.initialize_cursor(tenant_id, initial)
      raise Acp::RuntimeError, "progress initialization returned no cursor" if initialized.nil?

      ProgressSnapshot.new(cursor: Timestamp.normalize(initialized), revision: nil)
    end

    def fetch(tenant, context)
      semaphore = (@fetch_semaphore ||= Async::Semaphore.new(@configuration.effective_fetch_concurrency))
      semaphore.acquire
      acquired = true
      @metrics[:active_fetches] += 1
      @metrics[:max_active_fetches] = [@metrics[:max_active_fetches], @metrics[:active_fetches]].max
      result = @configuration.callback(:fetch).call(tenant, context)
      raise InvalidBatchError, "fetch must return an Acp::Batch" unless result.is_a?(Batch)

      result
    ensure
      if acquired
        @metrics[:active_fetches] -= 1
        semaphore.release
      end
    end

    def ingest(batch, tenant_id, cursor, poll_id)
      attempt = 1
      started_at = @clock.now
      loop do
        reply = Async::Queue.new
        context = IngestContext.new(tenant_id: tenant_id, cursor: cursor, poll_id: poll_id, attempt: attempt)
        message = IngestMessage.new(tenant_id: tenant_id, batch: batch, context: context, reply: reply)
        record_queued_batch
        begin
          @ingest_queue.push(message)
        rescue Async::Queue::ClosedError
          @metrics[:queued_batches] -= 1
          raise
        end
        error = reply.pop
        return if error.nil?
        raise error if fatal_error?(error)

        elapsed = @clock.now - started_at
        policy = @configuration.ingest_retry
        raise error unless policy.retryable?(error, attempt: attempt, elapsed: elapsed)

        @clock.sleep(policy.delay(attempt: attempt, random: @random))
        attempt += 1
      end
    end

    def ingestion_worker(queue)
      while (message = queue.pop)
        @metrics[:queued_batches] -= 1
        @metrics[:active_ingests] += 1
        @metrics[:max_active_ingests] = [@metrics[:max_active_ingests], @metrics[:active_ingests]].max
        begin
          @configuration.callback(:ingest).call(message.batch, message.context)
          message.reply.push(nil)
        rescue StandardError => e
          raise if fatal_error?(e)

          message.reply.push(e)
        ensure
          @metrics[:active_ingests] -= 1
        end
      end
    end

    # Once ingestion has returned, its commit is confirmed. Retrying only this
    # idempotent progress operation avoids replaying already-committed batches.
    def acknowledge(tenant_id, poll_id, cursor, revision, ownership_token)
      attempt = 1
      started_at = @clock.now
      loop do
        Async::Task.current.defer_stop do
          if @progress.respond_to?(:advance)
            @progress.advance(tenant_id, poll_id, cursor, expected_revision: revision, lease: ownership_token)
          else
            @progress.acknowledge(tenant_id, poll_id, cursor)
          end
        end
        return
      rescue StandardError => e
        raise if fatal_error?(e)

        report_error(tenant_id, e, :acknowledgement)
        elapsed = @clock.now - started_at
        policy = @configuration.ingest_retry
        delay = if policy.retryable?(e, attempt: attempt, elapsed: elapsed)
                  policy.delay(attempt: attempt, random: @random)
                else
                  @configuration.interval
                end
        @clock.sleep([delay, @configuration.interval].min)
        attempt += 1
      end
    end

    def retry_stage(policy, stage, tenant_id)
      attempt = 1
      started_at = @clock.now
      loop do
        return yield(attempt)
      rescue StandardError => e
        raise if fatal_error?(e)

        elapsed = @clock.now - started_at
        raise e unless policy.retryable?(e, attempt: attempt, elapsed: elapsed)

        report_error(tenant_id, e, stage)
        @clock.sleep(policy.delay(attempt: attempt, random: @random))
        attempt += 1
      end
    end

    def fatal_error?(error)
      error.is_a?(Acp::RuntimeError) ||
        error.is_a?(LeaseLostError) || error.is_a?(ProgressConflictError) ||
        error.is_a?(CancellationError) ||
        [Interrupt, SystemExit, SignalException, NoMemoryError].any? { |klass| error.is_a?(klass) } ||
        error.class.ancestors.any? { |ancestor| ancestor.name&.match?(/\AAsync::(?:.*::)?(?:Stop|Cancel)\z/) }
    end

    def report_error(tenant_id, error, stage)
      @on_error&.call(tenant_id, error, stage)
    end

    def release_ownership(tenant_id, token)
      Async::Task.current.defer_stop { @ownership.release(tenant_id, token) }
    rescue Acp::RuntimeError
      raise
    rescue StandardError => e
      report_error(tenant_id, e, :ownership_release)
    end

    ProgressSnapshot = Struct.new(:cursor, :revision, keyword_init: true)

    def start_lease_heartbeat(tenant_id, token, lease_state)
      return unless @ownership.respond_to?(:renewal_interval) && @ownership.respond_to?(:renew)

      cycle_task = Async::Task.current
      Async::Task.current.async do
        loop do
          @clock.sleep(@ownership.renewal_interval(token))
          next if @ownership.renew(tenant_id, token)

          lease_state[:lost] = true
          cycle_task.stop
        end
      rescue StandardError => e
        lease_state[:lost] = true
        report_error(tenant_id, e, :ownership_renewal)
        cycle_task.stop
      end
    end

    def start_worker_heartbeat
      return unless @ownership.respond_to?(:heartbeat_worker)

      interval = @ownership.respond_to?(:renewal_interval) ? @ownership.renewal_interval(nil) : 10
      Async::Task.current.async do
        loop do
          @ownership.heartbeat_worker(
            capacity: @configuration.pipeline_capacity,
            active: @metrics[:active_cycles]
          )
          @clock.sleep(interval)
        rescue StandardError => e
          report_error(nil, e, :worker_heartbeat)
          @clock.sleep([interval, 1].max)
        end
      end
    end

    def ensure_ownership!(tenant_id, token, lease_state)
      raise LeaseLostError, "ownership lease was lost for tenant #{tenant_id}" if lease_state[:lost]
      return unless @ownership.respond_to?(:valid?) && !@ownership.valid?(tenant_id, token)

      lease_state[:lost] = true
      raise LeaseLostError, "ownership lease was lost for tenant #{tenant_id}"
    end

    def record_queued_batch
      @metrics[:queued_batches] += 1
      @metrics[:max_queued_batches] = [@metrics[:max_queued_batches], @metrics[:queued_batches]].max
    end

    def drain_ingest_queue(queue)
      @metrics[:queued_batches] -= 1 while queue.pop(timeout: 0)
    end

    # Binary min-heap ordered by due-time and then insertion sequence. A
    # sequence tie-breaker ensures sustained due-time ties are fair.
    class DueHeap
      def initialize
        @items = []
      end

      def empty?
        @items.empty?
      end

      def peek
        @items.first
      end

      def push(state)
        @items << state
        index = @items.length - 1
        while index.positive?
          parent = (index - 1) / 2
          break unless before?(@items[index], @items[parent])

          @items[index], @items[parent] = @items[parent], @items[index]
          index = parent
        end
      end

      def pop
        first = @items.first
        last = @items.pop
        unless @items.empty?
          @items[0] = last
          index = 0
          loop do
            left = (index * 2) + 1
            right = left + 1
            smallest = index
            smallest = left if left < @items.length && before?(@items[left], @items[smallest])
            smallest = right if right < @items.length && before?(@items[right], @items[smallest])
            break if smallest == index

            @items[index], @items[smallest] = @items[smallest], @items[index]
            index = smallest
          end
        end
        first
      end

      private

      def before?(left, right)
        ([left.due, left.sequence] <=> [right.due, right.sequence]).negative?
      end
    end
  end
  # rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
  # rubocop:enable Metrics/ParameterLists, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Metrics/BlockLength
end
