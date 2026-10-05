# frozen_string_literal: true

require "async"
require "async/limited_queue"
require "async/queue"
require "async/semaphore"
require "async/notification"
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
    State = Struct.new(:tenant_id, :due, :sequence, :in_flight, :enabled, :scheduled, keyword_init: true)
    IngestMessage = Struct.new(:tenant_id, :batch, :context, :reply, :queued_at,
                               :task, :queued, :cancelled, :committed, :token, :lease_state, keyword_init: true)
    POLLING_LAG_BUCKETS = [0.0, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300].freeze

    def initialize(configuration:, progress:, ownership: LocalOwnership.new,
                   clock: MonotonicClock.new, on_error: nil, random: Random,
                   drain_timeout: 30, worker_id: nil)
      @configuration = configuration
      @progress = progress
      @ownership = ownership
      @clock = clock
      @on_error = on_error
      @random = random
      @drain_timeout = validate_duration(drain_timeout, "drain_timeout")
      @worker_id = worker_id || (ownership.worker_id if ownership.respond_to?(:worker_id))
      @metrics = {
        active_cycles: 0,
        max_active_cycles: 0,
        active_fetches: 0,
        max_active_fetches: 0,
        queued_batches: 0,
        max_queued_batches: 0,
        active_ingests: 0,
        max_active_ingests: 0,
        assigned_tenants: 0,
        discovered_tenants: 0,
        pipeline_utilization: 0.0,
        queue_wait: 0.0,
        ingestion_duration: 0.0,
        cursor_update_delay: 0.0,
        retry_count: 0,
        completed_cycles: 0,
        ownership_acquisitions: 0,
        ownership_skips: 0,
        polling_lag: 0.0,
        polling_lag_p50: 0.0,
        polling_lag_p95: 0.0,
        polling_lag_p99: 0.0,
        renewal_deadline_misses: 0,
        discovery_successes: 0,
        discovery_failures: 0
      }
      @polling_lag_histogram = Array.new(POLLING_LAG_BUCKETS.length, 0)
      @running = false
      @shutdown_requested = false
      @paused_tenants = {}
      @cycle_poll_ids = {}
    end

    def metrics
      @metrics.dup.freeze
    end

    def health
      { running: @running, stopping: @shutdown_requested, metrics: metrics }.freeze
    end

    # Request a bounded graceful shutdown. New cycles stop immediately; active
    # cycles are allowed to finish until the configured drain deadline.
    def request_shutdown(timeout: @drain_timeout)
      @shutdown_requested = true
      @shutdown_deadline = @clock.now + validate_duration(timeout, "shutdown timeout")
      emit(:shutdown_requested, stage: :scheduling)
      @events&.push([:shutdown])
      nil
    end

    def pause_tenant(tenant_id)
      @paused_tenants[tenant_id] = true
      @ownership.pause_tenant(tenant_id) if @ownership.respond_to?(:pause_tenant)
      emit(:tenant_paused, tenant_id: tenant_id, stage: :scheduling)
      nil
    end

    def resume_tenant(tenant_id)
      @paused_tenants.delete(tenant_id)
      @ownership.resume_tenant(tenant_id) if @ownership.respond_to?(:resume_tenant)
      @events&.push([:resume, tenant_id])
      emit(:tenant_resumed, tenant_id: tenant_id, stage: :scheduling)
      nil
    end

    # Run until shutdown is requested or the surrounding Async task is cancelled.
    def run
      running = false
      raise Acp::RuntimeError, "runtime is already running" if @running

      @running = true
      running = true
      task = Async::Task.current
      queue = IngestQueue.new(@configuration.pipeline_capacity)
      @ingest_queue = queue
      events = Async::LimitedQueue.new(@configuration.pipeline_capacity)
      @events = events
      @cycle_tasks = {}
      worker_count = [@configuration.ingest_concurrency, @configuration.pipeline_capacity].min
      @ingestion_tasks = worker_count.times.map { task.async { ingestion_worker(queue) } }
      states = if @ownership.respond_to?(:acquire_discovery)
                 perform_discovery(initial: true)
               else
                 found = discover
                 @ownership.register_tenants(found.map(&:tenant_id)) if @ownership.respond_to?(:register_tenants)
                 found
               end
      if @ownership.respond_to?(:heartbeat_worker)
        @ownership.heartbeat_worker(capacity: @configuration.pipeline_capacity, active: 0)
      end
      @metrics[:discovered_tenants] = states.length
      if @ownership.respond_to?(:scheduling_tenants)
        assigned = scheduling_tenants.to_h { |id| [id, true] }
        states.select! { |state| assigned.key?(state.tenant_id) }
      end
      worker_heartbeat = start_worker_heartbeat
      discovery_task = start_discovery(events)
      assignment_task = start_assignment_refresh(events)
      heap = DueHeap.new
      states.each { |state| heap.push(state) }
      @states = states.to_h { |state| [state.tenant_id, state] }
      @metrics[:assigned_tenants] = states.length
      active = 0
      sequence = states.length

      loop do
        while !@shutdown_requested && active < @configuration.pipeline_capacity &&
              !heap.empty? && heap.peek.due <= @clock.now
          cycle_state = heap.pop
          if cycle_state.in_flight || !cycle_state.enabled ||
             !@states[cycle_state.tenant_id].equal?(cycle_state)
            next
          end

          if paused?(cycle_state.tenant_id)
            cycle_state.due = @clock.now + [@configuration.interval, @configuration.discovery_interval].min
            heap.push(cycle_state)
            next
          end

          cycle_state.in_flight = true
          active += 1
          @metrics[:active_cycles] += 1
          @metrics[:max_active_cycles] = [@metrics[:max_active_cycles], @metrics[:active_cycles]].max
          @metrics[:pipeline_utilization] = @metrics[:active_cycles].fdiv(@configuration.pipeline_capacity)
          cycle_task = task.async(cycle_state) do |_cycle_task, state; next_due|
            next_due = @clock.now + @configuration.interval
            begin
              next_due = run_cycle(state.tenant_id)
            ensure
              @metrics[:active_cycles] -= 1
              begin
                events.push([:cycle, state, next_due])
              rescue Async::Queue::ClosedError
                nil
              end
              @cycle_tasks.delete(state.tenant_id)
            end
          end
          @cycle_tasks[cycle_state.tenant_id] = cycle_task unless cycle_task.finished?
        end

        break if @shutdown_requested && active.zero?

        if @shutdown_requested && @clock.now >= @shutdown_deadline
          @ingestion_tasks.each(&:stop)
          @cycle_tasks.each_value(&:stop)
          @shutdown_deadline = Float::INFINITY
        end

        now = @clock.now
        delay = if @shutdown_requested
                  0.1
                elsif heap.empty?
                  nil
                else
                  [heap.peek.due - now, 0].max
                end
        event = if delay&.positive?
                  @clock.wait(delay) { events.pop }
                else
                  events.pop
                end
        next unless event

        case event.first
        when :cycle
          _kind, state, due = event
          state.in_flight = false
          state.due = due
          state.sequence = sequence
          sequence += 1
          heap.push(state) if state.enabled
          active -= 1
          @metrics[:pipeline_utilization] = @metrics[:active_cycles].fdiv(@configuration.pipeline_capacity)
        when :discovery
          apply_discovery(event.last, heap, sequence)
          sequence += event.last.length
        when :shutdown
          next
        else
          resumed_state = @states[event.last]
          if resumed_state&.enabled && !resumed_state.in_flight
            heap.delete(resumed_state)
            resumed_state.due = @clock.now
            resumed_state.sequence = sequence
            sequence += 1
            heap.push(resumed_state)
          end
        end
      end
    ensure
      if running
        discovery_task&.stop
        assignment_task&.stop
        worker_heartbeat&.stop
        events&.close
        @cycle_tasks&.each_value(&:stop)
        @ingestion_tasks&.each(&:stop)
        queue&.close
        drain_ingest_queue(queue) if queue
        @running = false
      end
    end

    private

    def discover
      discovered = {}
      emit_tenant = lambda do |tenant_id|
        raise ArgumentError, "tenant_id cannot be nil" if tenant_id.nil?
        next if discovered.key?(tenant_id)

        now = @clock.now
        offset = initial_offset(tenant_id)
        state = State.new(tenant_id: tenant_id, due: now + offset, sequence: discovered.length,
                          in_flight: false, enabled: true)
        discovered[tenant_id] = state
        yield tenant_id if block_given?
      end
      @configuration.callback(:tenants).call(emit_tenant)
      @metrics[:discovery_successes] += 1
      emit(:discovery_completed, stage: :discovery, attributes: { tenant_count: discovered.length })
      discovered.values
    rescue StandardError
      @metrics[:discovery_failures] += 1
      raise
    end

    def start_discovery(events)
      return unless @ownership.respond_to?(:acquire_discovery)

      Async::Task.current.async do
        loop do
          @clock.sleep(@configuration.discovery_interval)
          next if @shutdown_requested

          discovered = if @ownership.respond_to?(:acquire_discovery)
                         perform_discovery
                       else
                         discover
                       end
          next unless discovered

          tenant_ids = if @ownership.respond_to?(:scheduling_tenants)
                         scheduling_tenants
                       else
                         discovered.map(&:tenant_id)
                       end
          events.push([:discovery, tenant_ids])
        rescue StandardError => e
          report_error(nil, e, :discovery)
          @clock.sleep([@configuration.discovery_interval, 1].min)
        end
      end
    end

    def start_assignment_refresh(events)
      return unless @ownership.respond_to?(:scheduling_tenants)

      interval = [@configuration.discovery_interval, @ownership.renewal_interval(nil)].min
      Async::Task.current.async do
        loop do
          @clock.sleep(interval)
          break if @shutdown_requested

          events.push([:discovery, scheduling_tenants])
        rescue StandardError => e
          report_error(nil, e, :assignment)
        end
      end
    end

    def scheduling_tenants
      ids = @ownership.scheduling_tenants
      if @ownership.respond_to?(:discovered_tenant_count)
        @metrics[:discovered_tenants] = @ownership.discovered_tenant_count
      end
      ids
    end

    def perform_discovery(initial: false)
      token = @ownership.acquire_discovery
      unless token
        return nil unless initial

        return @ownership.enabled_tenants.map do |tenant_id|
          State.new(tenant_id: tenant_id, due: @clock.now + initial_offset(tenant_id),
                    sequence: 0, in_flight: false, enabled: true)
        end
      end

      generation = @ownership.begin_discovery(token)
      heartbeat = Async::Task.current.async do
        loop do
          @clock.sleep(@ownership.discovery_renewal_interval)
          raise LeaseLostError, "discovery lease was lost" unless @ownership.renew_discovery(token)
        end
      end
      states = discover do |tenant_id|
        @ownership.register_discovered_tenant(tenant_id, generation, token)
      end
      @ownership.complete_discovery(generation, token)
      states
    ensure
      heartbeat&.stop
      @ownership.release_discovery(token) if token
    end

    def apply_discovery(tenant_ids, heap, sequence)
      live = tenant_ids.to_h { |tenant_id| [tenant_id, true] }
      @states.each do |tenant_id, state|
        next if live.key?(tenant_id)

        state.enabled = false
      end
      tenant_ids.each do |tenant_id|
        state = @states[tenant_id]
        if state
          unless state.enabled
            state.enabled = true
            next if state.scheduled || state.in_flight

            state.due = @clock.now
            state.sequence = sequence
            heap.push(state) unless state.in_flight
          end
          next
        end

        state = State.new(tenant_id: tenant_id, due: @clock.now, sequence: sequence,
                          in_flight: false, enabled: true)
        @states[tenant_id] = state
        heap.push(state)
      end
      @metrics[:assigned_tenants] = live.length
    end

    def paused?(tenant_id)
      return true if @paused_tenants.key?(tenant_id)
      return false if @ownership.respond_to?(:scheduling_tenants)
      return false unless @ownership.respond_to?(:tenant_paused?)

      @ownership.tenant_paused?(tenant_id)
    end

    def initial_offset(tenant_id)
      identity = "#{@configuration.name}\0#{tenant_id.class.name}\0#{tenant_id}"
      sample = Digest::SHA256.digest(identity).unpack1("Q>")
      sample.fdiv(2**64) * @configuration.interval
    end

    def run_cycle(tenant_id)
      started_at = @clock.now
      ownership_token = @ownership.acquire(tenant_id)
      unless ownership_token
        @metrics[:ownership_skips] += 1
        return @clock.now + @configuration.interval
      end
      @metrics[:ownership_acquisitions] += 1

      lease_state = { lost: false }
      heartbeat = start_lease_heartbeat(tenant_id, ownership_token, lease_state)
      progress_state = read_or_initialize_cursor(tenant_id)
      cursor = progress_state.cursor
      poll_id = Context.new_poll_id
      @cycle_poll_ids[tenant_id] = poll_id
      emit(:poll_started, tenant_id: tenant_id, poll_id: poll_id, stage: :scheduling)
      tenant = resolve_stage { @configuration.callback(:resolve).call(tenant_id) }
      ensure_ownership!(tenant_id, ownership_token, lease_state)
      batch = retry_stage(@configuration.fetch_retry, :fetch, tenant_id) do |attempt|
        context = FetchContext.new(tenant_id: tenant_id, cursor: cursor, poll_id: poll_id, attempt: attempt)
        fetch(tenant, context)
      end
      batch.validate_after!(cursor)
      ensure_ownership!(tenant_id, ownership_token, lease_state)
      ingest(batch, tenant_id, cursor, poll_id, ownership_token, lease_state)
      ensure_ownership!(tenant_id, ownership_token, lease_state)
      cursor_update_started = @clock.now
      acknowledge(tenant_id, poll_id, batch.next_cursor, progress_state.revision, ownership_token)
      @metrics[:cursor_update_delay] = @clock.now - cursor_update_started
      @metrics[:polling_lag] = [@clock.now - (started_at + @configuration.interval), 0].max
      record_cycle_success(@metrics[:polling_lag])
      emit(:poll_completed, tenant_id: tenant_id, poll_id: poll_id, stage: :scheduling,
                            duration: @clock.now - started_at)
      [started_at + @configuration.interval, @clock.now].max
    rescue LeaseLostError, ProgressConflictError => e
      report_error(tenant_id, e, :ownership)
      @clock.now + @configuration.interval
    rescue Acp::RuntimeError
      raise
    rescue StandardError => e
      report_error(tenant_id, e, :cycle)
      @clock.now + @configuration.retry_cooldown
    ensure
      heartbeat&.stop
      release_ownership(tenant_id, ownership_token) if ownership_token
      @cycle_poll_ids.delete(tenant_id)
    end

    def read_or_initialize_cursor(tenant_id)
      if @progress.respond_to?(:read_state)
        state = @progress.read_state(tenant_id)
        return state if state

        initial = Timestamp.normalize(resolve_stage { @configuration.callback(:initial_cursor).call(tenant_id) })
        initialized = @progress.initialize_state(tenant_id, initial)
        raise Acp::RuntimeError, "progress initialization returned no cursor" if initialized.nil?

        return initialized
      end

      cursor = @progress.read(tenant_id)
      return ProgressSnapshot.new(cursor: Timestamp.normalize(cursor), revision: nil) unless cursor.nil?

      initial = Timestamp.normalize(resolve_stage { @configuration.callback(:initial_cursor).call(tenant_id) })
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
      started_at = @clock.now
      emit(:fetch_started, tenant_id: context.tenant_id, poll_id: context.poll_id,
                           stage: :fetch, attempt: context.attempt)
      result = @configuration.callback(:fetch).call(tenant, context)
      raise InvalidBatchError, "fetch must return an Acp::Batch" unless result.is_a?(Batch)

      emit(:fetch_completed, tenant_id: context.tenant_id, poll_id: context.poll_id,
                             stage: :fetch, attempt: context.attempt, duration: @clock.now - started_at)
      result
    ensure
      if acquired
        @metrics[:active_fetches] -= 1
        semaphore.release
      end
    end

    def resolve_stage(&block)
      @resolve_semaphore ||= Async::Semaphore.new(@configuration.effective_resolve_concurrency)
      @resolve_semaphore.acquire(&block)
    end

    def ingest(batch, tenant_id, cursor, poll_id, token, lease_state)
      attempt = 1
      started_at = @clock.now
      policy = @configuration.ingest_retry
      message = nil
      loop do
        reply = Async::Queue.new
        context = IngestContext.new(tenant_id: tenant_id, cursor: cursor, poll_id: poll_id, attempt: attempt)
        message = IngestMessage.new(tenant_id: tenant_id, batch: batch, context: context, reply: reply,
                                    queued_at: @clock.now, queued: true, token: token, lease_state: lease_state)
        record_queued_batch
        begin
          @ingest_queue.push(message)
        rescue Async::Queue::ClosedError
          @metrics[:queued_batches] -= 1
          raise
        end
        error = begin
          within_retry_budget(policy, started_at) { reply.pop }
        rescue StageTimeoutError => e
          cancel_ingestion(message)
          message.committed ? nil : e
        end
        return if error.nil?
        raise error if fatal_error?(error)

        elapsed = @clock.now - started_at
        raise error unless policy.retryable?(error, attempt: attempt, elapsed: elapsed)

        @metrics[:retry_count] += 1
        emit(:retry_scheduled, tenant_id: tenant_id, poll_id: poll_id, stage: :ingest,
                               attempt: attempt, error: classify_error(:ingestion, error))
        retry_sleep(policy, started_at, attempt, error)
        attempt += 1
      end
    ensure
      cancel_ingestion(message) if message
    end

    def ingestion_worker(queue)
      while (message = queue.pop)
        message.queued = false
        @metrics[:queued_batches] -= 1
        message.task = Async::Task.new(Async::Task.current) do
          perform_ingestion(message)
        end
        message.task.run
        message.reply.push(message.task.wait)
      end
    end

    def perform_ingestion(message)
      active = false
      raise CancellationError, "ingestion was cancelled" if message.cancelled

      ensure_ownership!(message.tenant_id, message.token, message.lease_state)
      @metrics[:active_ingests] += 1
      active = true
      @metrics[:max_active_ingests] = [@metrics[:max_active_ingests], @metrics[:active_ingests]].max
      @metrics[:queue_wait] = @clock.now - message.queued_at
      started_at = @clock.now
      emit(
        :ingestion_started,
        tenant_id: message.tenant_id, poll_id: message.context.poll_id,
        stage: :ingest, attempt: message.context.attempt
      )
      @configuration.callback(:ingest).call(message.batch, message.context)
      message.committed = true
      @metrics[:ingestion_duration] = @clock.now - started_at
      emit(
        :ingestion_completed,
        tenant_id: message.tenant_id, poll_id: message.context.poll_id,
        stage: :ingest, attempt: message.context.attempt,
        duration: @metrics[:ingestion_duration]
      )
      nil
    rescue StandardError => e
      report_error(message.tenant_id, e, :ingestion, attempt: message.context.attempt)
      e
    ensure
      if active
        @metrics[:ingestion_duration] = @clock.now - started_at
        @metrics[:active_ingests] -= 1
      end
    end

    def cancel_ingestion(message)
      Async::Task.current.defer_stop do
        message.cancelled = true
        if message.queued && @ingest_queue.delete(message)
          message.queued = false
          @metrics[:queued_batches] -= 1
        end
        message.task&.stop
        message.task&.wait
        message.batch = nil
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

        report_error(tenant_id, e, :acknowledgement, attempt: attempt)
        elapsed = @clock.now - started_at
        policy = @configuration.ingest_retry
        delay = if policy.retryable?(e, attempt: attempt, elapsed: elapsed)
                  policy.delay(attempt: attempt, random: @random)
                else
                  @configuration.retry_cooldown
                end
        @metrics[:retry_count] += 1
        @clock.sleep(delay)
        attempt += 1
      end
    end

    def retry_stage(policy, stage, tenant_id)
      attempt = 1
      started_at = @clock.now
      loop do
        return within_retry_budget(policy, started_at) { yield(attempt) }
      rescue StandardError => e
        raise if fatal_error?(e)

        elapsed = @clock.now - started_at
        raise e unless policy.retryable?(e, attempt: attempt, elapsed: elapsed)

        @metrics[:retry_count] += 1
        emit(:retry_scheduled, tenant_id: tenant_id, poll_id: @cycle_poll_ids[tenant_id], stage: stage,
                               attempt: attempt, error: classify_error(stage, e))
        report_error(tenant_id, e, stage, attempt: attempt)
        retry_sleep(policy, started_at, attempt, e)
        attempt += 1
      end
    end

    def within_retry_budget(policy, started_at, &block)
      remaining = policy.max_elapsed && policy.max_elapsed - (@clock.now - started_at)
      raise StageTimeoutError, "retry deadline exceeded" if remaining && remaining <= 0

      duration = [remaining, policy.timeout].compact.min
      return yield unless duration

      Async::Task.current.with_timeout(duration, StageTimeoutError, &block)
    end

    def retry_sleep(policy, started_at, attempt, error)
      delay = policy.delay(attempt: attempt, random: @random)
      remaining = policy.max_elapsed && policy.max_elapsed - (@clock.now - started_at)
      raise error if remaining && remaining <= 0

      @clock.sleep(remaining ? [delay, remaining].min : delay)
      raise error if policy.max_elapsed && @clock.now - started_at >= policy.max_elapsed
    end

    def fatal_error?(error)
      error.is_a?(Acp::RuntimeError) ||
        error.is_a?(LeaseLostError) || error.is_a?(ProgressConflictError) ||
        error.is_a?(CancellationError) ||
        [Interrupt, SystemExit, SignalException, NoMemoryError].any? { |klass| error.is_a?(klass) } ||
        error.class.ancestors.any? { |ancestor| ancestor.name&.match?(/\AAsync::(?:.*::)?(?:Stop|Cancel)\z/) }
    end

    def report_error(tenant_id, error, stage, attempt: nil)
      emit(:failure, tenant_id: tenant_id, poll_id: @cycle_poll_ids[tenant_id], stage: stage, attempt: attempt,
                     error: classify_error(stage, error))
      @on_error&.call(tenant_id, error, stage)
    end

    def emit(name, tenant_id: nil, poll_id: nil, stage: nil, attempt: nil, duration: nil,
             error: nil, attributes: {})
      return unless defined?(::ActiveSupport::Notifications)

      payload = {
        program: @configuration.name,
        worker: @worker_id,
        tenant_id: tenant_id,
        poll_id: poll_id,
        stage: stage&.to_s,
        attempt: attempt,
        duration: duration,
        error: error,
        attributes: attributes
      }.compact
      ::ActiveSupport::Notifications.instrument("#{name}.acp", payload)
    rescue StandardError
      nil
    end

    def classify_error(stage, error)
      redis_error = error.class.ancestors.any? { |ancestor| ancestor.name&.start_with?("Redis::") }
      database_error = error.class.ancestors.any? do |ancestor|
        ancestor.name&.start_with?("ActiveRecord::", "PG::")
      end
      category = if redis_error || %i[ownership ownership_renewal ownership_release
                                      worker_heartbeat].include?(stage.to_sym)
                   "redis"
                 elsif database_error
                   "ingestion"
                 else
                   case stage.to_sym
                   when :fetch then "api"
                   when :ingest, :ingestion, :acknowledgement then "ingestion"
                   else "scheduling"
                   end
                 end
      { category: category, class: error.class.name }
    end

    def validate_duration(value, name)
      valid = value.is_a?(Numeric) && value.finite? && value.positive?
      return value if valid

      raise ConfigurationError, "#{name} must be a finite positive number"
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

          @metrics[:renewal_deadline_misses] += 1
          lease_state[:lost] = true
          cycle_task.stop
        end
      rescue StandardError => e
        @metrics[:renewal_deadline_misses] += 1
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

    def record_cycle_success(lag)
      @metrics[:completed_cycles] += 1
      bucket = POLLING_LAG_BUCKETS.index { |boundary| lag <= boundary } || POLLING_LAG_BUCKETS.length - 1
      @polling_lag_histogram[bucket] += 1
      [50, 95, 99].each do |percentile|
        target = (@metrics[:completed_cycles] * percentile / 100.0).ceil
        cumulative = 0
        index = @polling_lag_histogram.index do |count|
          cumulative += count
          cumulative >= target
        end
        @metrics["polling_lag_p#{percentile}".to_sym] = POLLING_LAG_BUCKETS.fetch(index)
      end
    end

    def drain_ingest_queue(queue)
      @metrics[:queued_batches] -= 1 while queue.pop(timeout: 0)
    end

    # Reactor-local queue with removal so cancelled batches never outlive their
    # pipeline reservation. Pipeline admission already bounds all producers.
    class IngestQueue
      def initialize(capacity)
        @capacity = capacity
        @items = []
        @ready = Async::Notification.new
        @closed = false
      end

      def push(message)
        raise Async::Queue::ClosedError if @closed
        raise Acp::RuntimeError, "ingestion queue exceeded pipeline capacity" if @items.length >= @capacity

        @items << message
        @ready.signal
      end

      def pop(timeout: nil)
        @ready.wait while @items.empty? && !@closed && timeout != 0
        @items.shift
      end

      def delete(message)
        @items.delete(message)
      end

      def close
        @closed = true
        @ready.signal
      end
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
        return if state.scheduled

        state.scheduled = true
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
        first.scheduled = false
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

      def delete(state)
        remaining = @items.reject { |item| item.equal?(state) }
        @items = []
        state.scheduled = false
        remaining.each do |item|
          item.scheduled = false
          push(item)
        end
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
