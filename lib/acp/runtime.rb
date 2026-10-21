# frozen_string_literal: true

require "async"
require "async/limited_queue"
require "async/queue"
require "async/semaphore"
require "async/notification"
require "digest"
require_relative "pressure_metrics"
require_relative "admission"
require_relative "deadline_adapter"
require_relative "discovery_page"
require_relative "byte_budget"
require_relative "cooperative_work"

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
    REACTOR_SAMPLE_INTERVAL = 0.1
    PRESSURE_NAMES = %i[admission_lag cycle_duration cycle_overrun resolve_wait resolve_service
                        fetch_wait fetch_service ingest_wait ingest_service acknowledge_wait acknowledge_service
                        cursor_read_service cursor_initialize_service cursor_update_delay
                        fetch_backoff ingest_backoff acknowledge_backoff
                        reactor_lag renewal_wakeup_lag renewal_service retry_admission_wait byte_budget_wait
                        pipeline_residence notification_service error_handler_service].freeze

    def initialize(configuration:, progress:, ownership: LocalOwnership.new,
                   clock: MonotonicClock.new, on_error: nil, random: Random,
                   drain_timeout: 30, worker_id: nil, exporter: nil)
      @configuration = configuration
      @shared_advance = progress.equal?(ownership) && progress.respond_to?(:advance)
      @progress = DeadlineAdapter.new(progress, configuration.coordination_timeout)
      @ownership = DeadlineAdapter.new(ownership, configuration.coordination_timeout)
      @clock = clock
      @on_error = on_error
      @exporter = exporter
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
      @pressure = PressureMetrics.new(PRESSURE_NAMES, clock: @clock)
      @running = false
      @shutdown_requested = false
      @paused_tenants = {}
      @cycle_poll_ids = {}
      @admission = Admission.new(@configuration, @metrics)
      @byte_budget = ByteBudget.new(@configuration.retained_byte_capacity, @metrics)
      @byte_wait_started = {}
      @metrics[:active_resolves] = 0
      @metrics[:max_active_resolves] = 0
      @metrics[:pending_acknowledgements] = 0
      @metrics[:max_pending_acknowledgements] = 0
      @metrics[:retained_batches] = 0
      @metrics[:max_retained_batches] = 0
      @metrics[:retry_overflows] = 0
      @metrics[:payload_rejections] = 0
    end

    def metrics
      pressure = @pressure.snapshot
      lag = pressure.fetch(:admission_lag)
      snapshot = @metrics.merge(polling_lag: lag[:last], polling_lag_p50: lag[:p50],
                                polling_lag_p95: lag[:p95], polling_lag_p99: lag[:p99],
                                byte_budget_waiters: @metrics[:byte_budget_waiters] + @byte_wait_started.length,
                                **overdue_snapshot, pressure: pressure)
      snapshot[:redis] = @ownership.metrics if @ownership.respond_to?(:metrics)
      snapshot[:telemetry] = @exporter.metrics if @exporter
      snapshot.freeze
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
      @cancelling_ingestion = false
      running = true
      task = Async::Task.current
      reactor_sampler = start_reactor_sampler
      queue = IngestQueue.new(@configuration.pipeline_capacity)
      @ingest_queue = queue
      events = Async::LimitedQueue.new(2 * (@configuration.pipeline_capacity + @configuration.retry_capacity) + 1)
      @events = events
      @cycle_tasks = {}
      worker_count = [@configuration.ingest_concurrency, @configuration.pipeline_capacity].min
      @ingestion_tasks = worker_count.times.map { task.async { ingestion_worker(queue) } }
      states = initial_states
      if @ownership.respond_to?(:heartbeat_worker)
        @ownership.heartbeat_worker(capacity: @configuration.pipeline_capacity, active: 0)
      end
      @metrics[:discovered_tenants] = states.length
      if @ownership.respond_to?(:scheduling_tenants)
        assigned = CooperativeWork.each(scheduling_tenants).to_h { |id| [id, true] }
        states = CooperativeWork.each(states).select { |state| assigned.key?(state.tenant_id) }
      end
      worker_heartbeat = start_worker_heartbeat
      discovery_task = start_discovery(events)
      assignment_task = start_assignment_refresh(events)
      heap = DueHeap.new
      CooperativeWork.each(states) { |state| heap.push(state) }
      @states = CooperativeWork.each(states).to_h { |state| [state.tenant_id, state] }
      @metrics[:assigned_tenants] = states.length
      active = 0
      sequence = states.length

      admission_work = CooperativeWork.new
      loop do
        while @admission.available? && (!@shutdown_requested || @clock.now < @shutdown_deadline)
          admission_work.checkpoint
          retry_reservation = @admission.retry_candidate
          cycle_state, group, estimate = @shutdown_requested ? nil : next_admissible_state(heap)
          if retry_reservation && (@admission.prefer_retry? || !cycle_state)
            heap.push(cycle_state) if cycle_state
            @admission.resume(retry_reservation)
            next
          end
          break unless cycle_state

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
          reservation = @admission.fresh(cycle_state.tenant_id, group)
          bytes = (@byte_budget.acquire(estimate) if estimate)
          active += 1
          @metrics[:active_cycles] += 1
          @metrics[:max_active_cycles] = [@metrics[:max_active_cycles], @metrics[:active_cycles]].max
          cycle_task = task.async(cycle_state, reservation, bytes) do |_, state, held, size; next_due, admitted|
            admitted = @clock.now
            next_due = @clock.now + @configuration.interval
            begin
              next_due = run_cycle(state.tenant_id, state.due, size)
            ensure
              begin
                events.push([:cycle, state, next_due])
              rescue Async::Queue::ClosedError
                nil
              ensure
                @metrics[:active_cycles] -= 1
                @pressure.observe(:pipeline_residence, @clock.now - admitted,
                                  outcome: PressureMetrics.outcome($ERROR_INFO))
                @admission.release(held)
                @cycle_tasks.delete(state.tenant_id)
              end
            end
          end
          @cycle_tasks[cycle_state.tenant_id] = cycle_task unless cycle_task.finished?
        end

        break if @shutdown_requested && active.zero?

        # Cleanup closes the completion queue before joining callbacks, so a
        # cancelled producer cannot deadlock on a full event queue at the deadline.
        break if @shutdown_requested && @clock.now >= @shutdown_deadline

        now = @clock.now
        delay = if @shutdown_requested
                  [@shutdown_deadline - now, 0.1].min
                elsif !@next_admission_due
                  nil
                else
                  [@next_admission_due - now, 0].max
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
        when :retry
          @admission.enqueue(event.last)
        when :capacity
          next
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
        @cleanup_deadline = @shutdown_requested ? @shutdown_deadline : @clock.now + @configuration.cleanup_timeout
        discovery_task&.stop
        assignment_task&.stop
        worker_heartbeat&.stop
        reactor_sampler&.stop
        events&.close
        stop_pipeline
        CooperativeWork.each(@byte_wait_started.values) do |started|
          @pressure.observe(:byte_budget_wait, @clock.now - started, outcome: :cancelled)
        end
        @byte_wait_started.clear
        queue&.close
        drain_ingest_queue(queue) if queue
        @running = false
        @metrics[:pipeline_utilization] = 0.0
      end
    end

    private

    def initial_states
      return perform_discovery(initial: true) if @ownership.respond_to?(:acquire_discovery)

      found = discover
      if @ownership.respond_to?(:register_tenants)
        @ownership.register_tenants(CooperativeWork.each(found).map(&:tenant_id))
      end
      found
    end

    # Stop producers and join callback cleanup before stopping consumer parents.
    # Repeated parent/child stops can otherwise interrupt deferred cleanup.
    def stop_pipeline
      @cancelling_ingestion = true
      Async::Task.current.defer_stop do
        cycles = @cycle_tasks ? @cycle_tasks.values : []
        CooperativeWork.each(cycles) do |cycle|
          cycle.stop
          cycle.wait
        end
        CooperativeWork.each(@ingestion_tasks || []) do |worker|
          worker.stop
          worker.wait
        end
      end
    end

    # Scan scheduler states, never spawning a task for a saturated group.
    def next_admissible_state(heap)
      @next_admission_due = nil
      blocked = []
      work = CooperativeWork.new
      until heap.empty? || heap.peek.due > @clock.now
        work.checkpoint
        state = heap.pop
        next if state.in_flight || !state.enabled || !@states[state.tenant_id].equal?(state)

        group = @admission.group_for(state.tenant_id)
        if @admission.available?(group)
          estimate = admission_estimate(state.tenant_id)
          if estimate && !@byte_budget.available?(estimate)
            record_byte_wait(state.tenant_id)
            blocked << state
            break # Oldest eligible weighted request cannot be overtaken by smaller work.
          end
          if (wait_started = @byte_wait_started.delete(state.tenant_id))
            @pressure.observe(:byte_budget_wait, @clock.now - wait_started)
          end
          return [state, group, estimate]
        end

        blocked << state
      end
      @next_admission_due = heap.peek&.due
      nil
    ensure
      CooperativeWork.each(blocked) { |state| heap.push(state) }
    end

    def record_byte_wait(tenant_id)
      return if @byte_wait_started.key?(tenant_id)

      @metrics[:byte_budget_waits] += 1
      @byte_wait_started[tenant_id] = @clock.now
      @metrics[:max_byte_budget_waiters] = [@metrics[:max_byte_budget_waiters], @byte_wait_started.length].max
    end

    # Invalid/impossible estimates run as ordinary failing cycles, never wait.
    def admission_estimate(tenant_id)
      return unless @configuration.retained_byte_capacity

      estimate = payload_estimate(tenant_id)
      estimate if estimate.is_a?(Integer) && estimate.positive? && estimate <= @configuration.retained_byte_capacity
    rescue StandardError
      nil
    end

    def overdue_snapshot
      now = @clock.now
      count = 0
      oldest = 0.0
      CooperativeWork.each(@states&.values || []) do |state|
        next unless @running && state.enabled && state.scheduled && !state.in_flight
        next if @paused_tenants.key?(state.tenant_id) || state.due > now

        count += 1
        oldest = [oldest, now - state.due].max
      end
      { overdue_tenants: count, oldest_overdue_age: oldest }
    end

    def start_reactor_sampler
      Async::Task.current.async do
        loop do
          expected = @clock.now + REACTOR_SAMPLE_INTERVAL
          # Use reactor time for the timer; injected clocks must not be advanced by instrumentation.
          Kernel.sleep(REACTOR_SAMPLE_INTERVAL)
          @pressure.observe(:reactor_lag, [@clock.now - expected, 0].max)
        end
      end
    end

    def discover(record_result: true)
      discovered = {}
      work = CooperativeWork.new
      emit_tenant = lambda do |tenant_id|
        work.checkpoint
        raise ArgumentError, "tenant_id cannot be nil" if tenant_id.nil?
        next if discovered.key?(tenant_id)

        now = @clock.now
        offset = initial_offset(tenant_id)
        state = State.new(tenant_id: tenant_id, due: now + offset, sequence: discovered.length,
                          in_flight: false, enabled: true)
        discovered[tenant_id] = state
        yield tenant_id if block_given?
      end
      enumerate_tenants(emit_tenant)
      record_discovery_success(discovered.length) if record_result
      discovered.values
    rescue StandardError
      @metrics[:discovery_failures] += 1 if record_result
      raise
    end

    def record_discovery_success(count)
      @metrics[:discovery_successes] += 1
      emit(:discovery_completed, stage: :discovery, attributes: { tenant_count: count })
    end

    def enumerate_tenants(emit_tenant)
      return @configuration.callback(:tenants).call(emit_tenant) unless @configuration.callbacks.key?(:tenants_page)

      cursor = nil
      loop do
        result = @configuration.callback(:tenants_page).call(cursor, @configuration.discovery_batch_size)
        ids, next_cursor = DiscoveryPage.materialize(result, @configuration.discovery_batch_size)
        CooperativeWork.each(ids) { |id| emit_tenant.call(id) }
        break if next_cursor.nil?

        raise ArgumentError, "tenants_page cursor must advance" if next_cursor == cursor

        cursor = next_cursor
        Async::Task.current.yield
      end
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

        return CooperativeWork.each(@ownership.enabled_tenants).map do |tenant_id|
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
      buffer = []
      states = discover(record_result: false) do |tenant_id|
        buffer << tenant_id
        flush_discovery(buffer, generation, token) if buffer.length == @configuration.discovery_batch_size
      end
      flush_discovery(buffer, generation, token) unless buffer.empty?
      @ownership.complete_discovery(generation, token)
      record_discovery_success(states.length)
      states
    rescue StandardError
      @metrics[:discovery_failures] += 1
      raise
    ensure
      heartbeat&.stop
      release_discovery(token) if token
    end

    def flush_discovery(buffer, generation, token)
      if @ownership.respond_to?(:register_discovered_tenants)
        @ownership.register_discovered_tenants(buffer, generation, token)
      else
        buffer.each { |id| @ownership.register_discovered_tenant(id, generation, token) }
      end
      buffer.clear
      Async::Task.current.yield
    end

    def apply_discovery(tenant_ids, heap, sequence)
      live = CooperativeWork.each(tenant_ids).to_h { |tenant_id| [tenant_id, true] }
      CooperativeWork.each(@states) do |tenant_id, state|
        next if live.key?(tenant_id)

        state.enabled = false
      end
      CooperativeWork.each(tenant_ids) do |tenant_id|
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

    def run_cycle(tenant_id, scheduled_due = @clock.now, reserved_bytes = nil)
      pending_acknowledgement = false
      retained_batch = false
      started_at = @clock.now
      @pressure.observe(:admission_lag, [started_at - scheduled_due, 0].max)
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
      emit(:poll_started, tenant_id: tenant_id, poll_id: poll_id, stage: :scheduling,
                          attributes: { admission_lag: [started_at - scheduled_due, 0].max })
      tenant = resolve_stage { @configuration.callback(:resolve).call(tenant_id) }
      reserved_bytes ||= reserve_payload(tenant_id)
      batch = retry_stage(@configuration.fetch_retry, :fetch, tenant_id) do |attempt|
        ensure_ownership!(tenant_id, ownership_token, lease_state)
        context = FetchContext.new(tenant_id: tenant_id, cursor: cursor, poll_id: poll_id, attempt: attempt,
                                   max_response_bytes: @configuration.max_response_bytes,
                                   reserved_bytes: reserved_bytes)
        fetched = fetch(tenant, context)
        validate_payload!(fetched, reserved_bytes)
        fetched
      end
      retained_bytes = batch.size_bytes || 0
      @metrics[:retained_bytes] += retained_bytes
      @metrics[:max_retained_bytes] = [@metrics[:max_retained_bytes], @metrics[:retained_bytes]].max
      @metrics[:retained_batches] += 1
      retained_batch = true
      @metrics[:max_retained_batches] = [@metrics[:max_retained_batches], @metrics[:retained_batches]].max
      batch.validate_after!(cursor)
      ingest(batch, tenant_id, cursor, poll_id, ownership_token, lease_state)
      # Redis advancement fences ownership atomically. Legacy progress adapters
      # still need the separate ownership gate before acknowledgement.
      ensure_ownership!(tenant_id, ownership_token, lease_state) unless @shared_advance
      cursor_update_started = @clock.now
      @pressure.measure(:cursor_update_delay) do
        @metrics[:pending_acknowledgements] += 1
        pending_acknowledgement = true
        pending_peak = [@metrics[:max_pending_acknowledgements], @metrics[:pending_acknowledgements]].max
        @metrics[:max_pending_acknowledgements] = pending_peak
        acknowledge(tenant_id, poll_id, batch.next_cursor, progress_state.revision, ownership_token)
      end
      @metrics[:cursor_update_delay] = @clock.now - cursor_update_started
      @metrics[:completed_cycles] += 1
      emit(:poll_completed, tenant_id: tenant_id, poll_id: poll_id, stage: :scheduling,
                            duration: @clock.now - started_at)
      [started_at + @configuration.interval, @clock.now].max
    rescue LeaseLostError, ProgressConflictError => e
      cycle_error = e
      report_error(tenant_id, e, :ownership)
      @clock.now + @configuration.interval
    rescue Acp::RuntimeError
      raise
    rescue PayloadLimitError => e
      cycle_error = e
      @metrics[:payload_rejections] += 1
      report_error(tenant_id, e, :payload)
      @clock.now + @configuration.retry_cooldown
    rescue StandardError => e
      cycle_error = e
      report_error(tenant_id, e, :cycle)
      @clock.now + @configuration.retry_cooldown
    ensure
      outcome = PressureMetrics.outcome($ERROR_INFO || cycle_error)
      if started_at
        @pressure.observe(:cycle_duration, [@clock.now - started_at, 0].max, outcome: outcome)
        @pressure.observe(:cycle_overrun, [@clock.now - started_at - @configuration.interval, 0].max,
                          outcome: outcome)
      end
      heartbeat&.stop
      begin
        release_ownership(tenant_id, ownership_token) if ownership_token
      ensure
        @cycle_poll_ids.delete(tenant_id)
        @metrics[:retained_batches] -= 1 if retained_batch
        @metrics[:pending_acknowledgements] -= 1 if pending_acknowledgement
        batch = nil
        @metrics[:retained_bytes] -= retained_bytes if retained_bytes
        @byte_budget.release(reserved_bytes) if reserved_bytes
      end
    end

    def reserve_payload(tenant_id)
      return unless @configuration.retained_byte_capacity

      estimate = payload_estimate(tenant_id)
      unless estimate.is_a?(Integer) && estimate.positive?
        raise PayloadLimitError, "payload_estimate must return a positive integer"
      end

      @pressure.measure(:byte_budget_wait) { @byte_budget.acquire(estimate) }
    end

    def payload_estimate(tenant_id)
      return @configuration.max_batch_bytes unless @configuration.callbacks.key?(:payload_estimate)

      @configuration.callback(:payload_estimate).call(tenant_id)
    end

    def validate_payload!(batch, reserved_bytes)
      limit = @configuration.max_batch_bytes
      return unless limit || reserved_bytes

      raise PayloadLimitError, "budgeted fetch must return Batch size_bytes" unless batch.size_bytes

      if limit && batch.size_bytes > limit
        @metrics[:oversized_batches] += 1
        raise PayloadLimitError, "batch exceeds max_batch_bytes"
      end
      return unless reserved_bytes && batch.size_bytes > reserved_bytes

      @metrics[:estimate_overflows] += 1
      raise PayloadLimitError, "batch exceeds its pre-fetch byte reservation"
    end

    def read_or_initialize_cursor(tenant_id)
      deadline(@configuration.initial_cursor_timeout) { cursor_state(tenant_id) }
    end

    def cursor_state(tenant_id)
      if @progress.respond_to?(:read_state)
        state = @pressure.measure(:cursor_read_service) { @progress.read_state(tenant_id) }
        return state if state

        initial = Timestamp.normalize(resolve_stage { @configuration.callback(:initial_cursor).call(tenant_id) })
        initialized = @pressure.measure(:cursor_initialize_service) { @progress.initialize_state(tenant_id, initial) }
        raise Acp::RuntimeError, "progress initialization returned no cursor" if initialized.nil?

        return initialized
      end

      cursor = @pressure.measure(:cursor_read_service) { @progress.read(tenant_id) }
      return ProgressSnapshot.new(cursor: Timestamp.normalize(cursor), revision: nil) unless cursor.nil?

      initial = Timestamp.normalize(resolve_stage { @configuration.callback(:initial_cursor).call(tenant_id) })
      initialized = @pressure.measure(:cursor_initialize_service) { @progress.initialize_cursor(tenant_id, initial) }
      raise Acp::RuntimeError, "progress initialization returned no cursor" if initialized.nil?

      ProgressSnapshot.new(cursor: Timestamp.normalize(initialized), revision: nil)
    end

    def fetch(tenant, context)
      semaphore = (@fetch_semaphore ||= Async::Semaphore.new(@configuration.effective_fetch_concurrency))
      @pressure.measure(:fetch_wait) { semaphore.acquire }
      acquired = true
      @metrics[:active_fetches] += 1
      @metrics[:max_active_fetches] = [@metrics[:max_active_fetches], @metrics[:active_fetches]].max
      started_at = @clock.now
      emit(:fetch_started, tenant_id: context.tenant_id, poll_id: context.poll_id,
                           stage: :fetch, attempt: context.attempt)
      result = @pressure.measure(:fetch_service) { @configuration.callback(:fetch).call(tenant, context) }
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
      deadline(@configuration.resolve_timeout) { resolve_callback(&block) }
    end

    def resolve_callback(&block)
      @resolve_semaphore ||= Async::Semaphore.new(@configuration.effective_resolve_concurrency)
      @pressure.measure(:resolve_wait) { @resolve_semaphore.acquire }
      acquired = true
      @metrics[:active_resolves] += 1
      @metrics[:max_active_resolves] = [@metrics[:max_active_resolves], @metrics[:active_resolves]].max
      @pressure.measure(:resolve_service, &block)
    ensure
      if acquired
        @metrics[:active_resolves] -= 1
        @resolve_semaphore.release
      end
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
          cancel_ingestion(message, outcome: :timeout)
          message.committed ? nil : e
        end
        return if error.nil?
        raise error if fatal_error?(error)

        elapsed = @clock.now - started_at
        raise error unless policy.retryable?(error, attempt: attempt, elapsed: elapsed)

        @metrics[:retry_count] += 1
        emit(:retry_scheduled, tenant_id: tenant_id, poll_id: poll_id, stage: :ingest,
                               attempt: attempt, error: classify_error(:ingestion, error))
        retry_sleep(policy, started_at, attempt, error, :ingest, tenant_id)
        attempt += 1
      end
    ensure
      cancel_ingestion(message) if message
    end

    def ingestion_worker(queue)
      while (message = queue.pop)
        @pressure.observe(:ingest_wait, [@clock.now - message.queued_at, 0].max)
        message.queued = false
        @metrics[:queued_batches] -= 1
        message.task = Async::Task.new(Async::Task.current) do
          perform_ingestion(message)
        end
        message.task.run
        message.reply.push(message.task.wait)
        break if @cancelling_ingestion
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
      @pressure.measure(:ingest_service) { @configuration.callback(:ingest).call(message.batch, message.context) }
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

    def cancel_ingestion(message, outcome: :cancelled)
      Async::Task.current.defer_stop do
        message.cancelled = true
        if message.queued && @ingest_queue.delete(message)
          @pressure.observe(:ingest_wait, [@clock.now - message.queued_at, 0].max, outcome: outcome)
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
        @pressure.observe(:acknowledge_wait, 0.0)
        @pressure.measure(:acknowledge_service) do
          deadline(@configuration.acknowledge_retry.timeout) do
            if @progress.respond_to?(:advance)
              @progress.advance(tenant_id, poll_id, cursor, expected_revision: revision, lease: ownership_token)
            else
              @progress.acknowledge(tenant_id, poll_id, cursor)
            end
          end
        end
        return
      rescue StandardError => e
        raise if fatal_error?(e)

        report_error(tenant_id, e, :acknowledgement, attempt: attempt)
        elapsed = @clock.now - started_at
        policy = @configuration.acknowledge_retry
        delay = if policy.retryable?(e, attempt: attempt, elapsed: elapsed)
                  policy.delay(attempt: attempt, random: @random)
                else
                  @configuration.retry_cooldown
                end
        @metrics[:retry_count] += 1
        @pressure.measure(:acknowledge_backoff) { @clock.sleep(delay) }
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
        retry_sleep(policy, started_at, attempt, e, stage, tenant_id)
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

    def retry_sleep(policy, started_at, attempt, error, stage, tenant_id)
      delay = policy.delay(attempt: attempt, random: @random)
      remaining = policy.max_elapsed && policy.max_elapsed - (@clock.now - started_at)
      raise error if remaining && remaining <= 0

      reservation = (@admission.park(tenant_id) if @configuration.retry_capacity.positive? && @running)
      if @configuration.retry_capacity.positive? && @running && !reservation
        @metrics[:retry_overflows] += 1
        raise error
      end
      @events.push([:capacity]) if reservation
      @pressure.measure(:"#{stage}_backoff") { @clock.sleep(remaining ? [delay, remaining].min : delay) }
      raise error if policy.max_elapsed && @clock.now - started_at >= policy.max_elapsed

      return unless reservation

      @events.push([:retry, reservation])
      remaining = policy.max_elapsed && policy.max_elapsed - (@clock.now - started_at)
      @pressure.measure(:retry_admission_wait) do
        deadline(remaining) { reservation.ready.wait until reservation.active }
      end
      raise error if policy.max_elapsed && @clock.now - started_at >= policy.max_elapsed
    end

    def fatal_error?(error)
      error.is_a?(Acp::RuntimeError) || error.is_a?(PayloadLimitError) ||
        error.is_a?(LeaseLostError) || error.is_a?(ProgressConflictError) ||
        error.is_a?(CancellationError) ||
        [Interrupt, SystemExit, SignalException, NoMemoryError].any? { |klass| error.is_a?(klass) } ||
        error.class.ancestors.any? { |ancestor| ancestor.name&.match?(/\AAsync::(?:.*::)?(?:Stop|Cancel)\z/) }
    end

    def report_error(tenant_id, error, stage, attempt: nil)
      emit(:failure, tenant_id: tenant_id, poll_id: @cycle_poll_ids[tenant_id], stage: stage, attempt: attempt,
                     error: classify_error(stage, error))
      @pressure.measure(:error_handler_service) { @on_error&.call(tenant_id, error, stage) }
    end

    def emit(name, tenant_id: nil, poll_id: nil, stage: nil, attempt: nil, duration: nil,
             error: nil, attributes: {})
      return unless @exporter || defined?(::ActiveSupport::Notifications)

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
      @exporter&.offer(payload.merge(event: "#{name}.acp"))
      if defined?(::ActiveSupport::Notifications)
        @pressure.measure(:notification_service) do
          ::ActiveSupport::Notifications.instrument("#{name}.acp", payload)
        end
      end
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
      cleanup { @ownership.release(tenant_id, token) }
    rescue Acp::RuntimeError
      raise
    rescue StandardError => e
      report_error(tenant_id, e, :ownership_release)
    end

    def release_discovery(token)
      cleanup { @ownership.release_discovery(token) }
    rescue StandardError => e
      report_error(nil, e, :discovery_release)
    end

    def cleanup(&block)
      budget = @configuration.cleanup_timeout
      budget = [budget, @shutdown_deadline - @clock.now].min if @shutdown_requested
      budget = [budget, @cleanup_deadline - @clock.now].min if @cleanup_deadline
      Async::Task.current.defer_stop do
        Async::Task.current.with_timeout([budget, 0].max, StageTimeoutError, &block)
      end
    end

    ProgressSnapshot = Struct.new(:cursor, :revision, keyword_init: true)

    def deadline(duration, &block)
      return yield unless duration

      raise StageTimeoutError, "operation deadline exceeded" unless duration.positive?

      Async::Task.current.with_timeout(duration, StageTimeoutError, &block)
    end

    def start_lease_heartbeat(tenant_id, token, lease_state)
      return unless @ownership.respond_to?(:renewal_interval) && @ownership.respond_to?(:renew)

      cycle_task = Async::Task.current
      Async::Task.current.async do
        loop do
          interval = @ownership.renewal_interval(token)
          expected = @clock.now + interval
          @clock.sleep(interval)
          @pressure.observe(:renewal_wakeup_lag, [@clock.now - expected, 0].max)
          next if @pressure.measure(:renewal_service) { @ownership.renew(tenant_id, token) }

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

    def drain_ingest_queue(queue)
      work = CooperativeWork.new
      while (message = queue.pop(timeout: 0))
        @pressure.observe(:ingest_wait, [@clock.now - message.queued_at, 0].max, outcome: :cancelled)
        @metrics[:queued_batches] -= 1
        work.checkpoint
      end
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
        @indices = {}.compare_by_identity
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
        @indices[state] = index
        sift_up(index)
      end

      def pop
        delete(@items.first)
      end

      def delete(state)
        index = @indices.delete(state)
        return unless index

        state.scheduled = false
        last = @items.pop
        if index < @items.length
          @items[index] = last
          @indices[last] = index
          sift_down(sift_up(index))
        end
        state
      end

      private

      def swap(left, right)
        @items[left], @items[right] = @items[right], @items[left]
        @indices[@items[left]] = left
        @indices[@items[right]] = right
      end

      def sift_up(index)
        while index.positive?
          parent = (index - 1) / 2
          break unless before?(@items[index], @items[parent])

          swap(index, parent)
          index = parent
        end
        index
      end

      def sift_down(index)
        loop do
          left = (index * 2) + 1
          right = left + 1
          smallest = index
          smallest = left if left < @items.length && before?(@items[left], @items[smallest])
          smallest = right if right < @items.length && before?(@items[right], @items[smallest])
          break if smallest == index

          swap(index, smallest)
          index = smallest
        end
      end

      def before?(left, right)
        ([left.due, left.sequence] <=> [right.due, right.sequence]).negative?
      end
    end
  end
  # rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
  # rubocop:enable Metrics/ParameterLists, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Metrics/BlockLength
end
