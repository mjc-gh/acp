# frozen_string_literal: true

require "test_helper"
require "async"
require "active_support/notifications"

# Each test exercises a complete Async interleaving; keep its setup and observed
# invariants next to each other rather than extracting opaque test machinery.
# rubocop:disable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize, Metrics/ParameterLists
class TestRuntime < Minitest::Test
  class MemoryProgress
    attr_reader :cursors, :acknowledgements

    def initialize
      @cursors = {}
      @acknowledgements = {}
    end

    def read(tenant_id)
      cursors[tenant_id]
    end

    def initialize_cursor(tenant_id, cursor)
      cursors[tenant_id] ||= cursor
    end

    def acknowledge(tenant_id, poll_id, cursor)
      acknowledgements[[tenant_id, poll_id]] ||= cursor
      cursors[tenant_id] = acknowledgements.fetch([tenant_id, poll_id])
    end
  end

  class FakeClock
    attr_reader :time

    def initialize
      @time = 0.0
    end

    def now
      time
    end

    def sleep(duration)
      @time += duration
      Async::Task.current.yield
    end

    def wait(duration)
      @time += duration
      Async::Task.current.yield
      nil
    end
  end

  class MemoryDiscoveryOwnership < Acp::LocalOwnership
    def initialize
      super
      @tenant_ids = []
    end

    def acquire_discovery
      true
    end

    def discovery_renewal_interval
      1
    end

    def renew_discovery(_token)
      true
    end

    def begin_discovery(_token)
      true
    end

    def register_discovered_tenant(tenant_id, _generation, _token)
      @tenant_ids << tenant_id unless @tenant_ids.include?(tenant_id)
    end

    def complete_discovery(_generation, _token); end

    def release_discovery(_token); end

    def enabled_tenants
      @tenant_ids.dup
    end
  end

  # Tests explicitly advance time; a scheduler wait must not jump to the next due date.
  class ControlledClock < FakeClock
    def wait(_duration)
      yield
    end
  end

  def build_program(tenants: ["a"], interval: 0.05, fetch: nil, ingest: nil, fetch_retry: {}, ingest_retry: {})
    tenant_ids = tenants
    fetch_callback = fetch || lambda do |_tenant, context|
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end
    ingest_callback = ingest || ->(_batch, _context) {}

    Class.new(Acp::Program) do
      define_singleton_method(:name) { "RuntimeTestProgram" }
      self.interval interval
      fetch_concurrency 2
      ingest_concurrency 2
      pipeline_capacity 3
      tenants { |emit| tenant_ids.each { |tenant_id| emit.call(tenant_id) } }
      initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
      resolve { |tenant_id| tenant_id }
      fetch(&fetch_callback)
      ingest(&ingest_callback)
      self.fetch_retry(**fetch_retry) unless fetch_retry.empty?
      self.ingest_retry(**ingest_retry) unless ingest_retry.empty?
    end
  end

  def run_for(runtime, duration: 0.1)
    Async do |root|
      task = root.async { runtime.run }
      Kernel.sleep(duration)
      task.stop
    end
  end

  def test_bounds_pipeline_fetch_and_ingestion_work
    active_ingests = 0
    program = build_program(
      tenants: %w[a b c d e],
      ingest: lambda do |_batch, _context|
        active_ingests += 1
        begin
          Kernel.sleep(0.01)
        ensure
          active_ingests -= 1
        end
      end
    )
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)

    run_for(runtime)

    assert_operator runtime.metrics[:max_active_cycles], :<=, 3
    assert_operator runtime.metrics[:max_active_fetches], :<=, 2
    assert_operator runtime.metrics[:max_active_ingests], :<=, 2
    assert_operator runtime.metrics[:max_queued_batches], :<=, 3
    assert_operator runtime.metrics[:completed_cycles], :>, 0
    assert_operator runtime.metrics[:polling_lag_p50], :<=, 300
    assert_operator runtime.metrics[:polling_lag_p95], :<=, 300
    assert_operator runtime.metrics[:polling_lag_p99], :<=, 300
    assert_equal 0, runtime.metrics[:active_cycles]
    assert_equal 0, runtime.metrics[:active_fetches]
  end

  def test_cancelling_a_budgeted_saturated_scan_restores_heap_and_reservations
    visits = 0
    program = build_program(tenants: [])
    program.upstream_capacity 1
    program.upstream_group do |_id|
      visits += 1
      "shared"
    end
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)
    admission = runtime.instance_variable_get(:@admission)
    held = admission.fresh("held", admission.group_for("held"))
    states = Array.new(500) do |id|
      Acp::Runtime::State.new(tenant_id: id, due: 0, sequence: id, enabled: true)
    end
    runtime.instance_variable_set(:@states, states.to_h { |state| [state.tenant_id, state] })
    heap = Acp::Runtime::DueHeap.new
    states.each { |state| heap.push(state) }
    constructor = Acp::CooperativeWork.method(:new)
    clock = Object.new
    ticks = 0
    clock.define_singleton_method(:now) { ticks += 0.006 }
    Acp::CooperativeWork.stub(:new, -> { constructor.call(clock: clock) }) do
      Async do |root|
        scanner = root.async { runtime.send(:next_admissible_state, heap) }
        root.yield until visits > 1
        scanner.stop
        scanner.wait
      end
    end
    restored = []
    restored << heap.pop until heap.empty?

    assert_operator visits, :<, 500
    assert_equal states, restored
    assert_equal 1, runtime.metrics[:pipeline_reservations]
    admission.release(held)
    assert_equal 0, runtime.metrics[:pipeline_reservations]
  end

  def test_byte_reservations_cover_fetch_ingestion_retry_and_acknowledgement
    progress = MemoryProgress.new
    gate = Async::Queue.new
    stage = nil
    attempts = 0
    program = build_program(
      interval: 0.001,
      fetch: lambda do |_tenant, context|
        stage = :fetch
        gate.pop
        context.validate_response_bytes!(10)
        Acp::Batch.new(data: "payload", next_cursor: context.cursor + 1, size_bytes: 30)
      end,
      ingest: lambda do |_batch, _context|
        attempts += 1
        stage = :ingest
        gate.pop
        raise IOError, "retry" if attempts == 1
      end,
      ingest_retry: { on: IOError, max_attempts: 2, backoff: 0.001, max_backoff: 0.001, jitter: 0 }
    )
    program.retained_byte_capacity 50
    program.max_batch_bytes 50
    program.max_response_bytes 20
    program.payload_estimate { |_id| 40 }
    progress.define_singleton_method(:acknowledge) do |*args|
      stage = :acknowledge
      gate.pop
      super(*args)
    end
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress)
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(2) do
        Kernel.sleep(0.001) until stage == :fetch
        assert_equal 40, runtime.metrics[:reserved_bytes]
        assert_equal 0, runtime.metrics[:retained_bytes]
        gate.push(true)
        Kernel.sleep(0.001) until stage == :ingest
        assert_equal 30, runtime.metrics[:retained_bytes]
        gate.push(true)
        Kernel.sleep(0.001) until attempts == 2
        assert_equal 40, runtime.metrics[:reserved_bytes]
        gate.push(true)
        Kernel.sleep(0.001) until stage == :acknowledge
        assert_equal 30, runtime.metrics[:retained_bytes]
        runtime.request_shutdown
        gate.push(true)
        runner.wait
      end
    end
    assert_equal 1, runtime.metrics[:completed_cycles]
    assert_equal 0, runtime.metrics[:reserved_bytes]
    assert_equal 0, runtime.metrics[:retained_bytes]
  end

  def test_payload_failures_never_ingest_or_advance_and_release_bytes
    [[60, 40, 100, :oversized_batches], [80, 40, 60, :oversized_batches], [45, 40, 100, :estimate_overflows],
     [30, 110, 100, nil], [nil, 40, 100, nil]].each do |size, estimate, capacity, metric|
      progress = MemoryProgress.new
      errors = []
      program = build_program(interval: 0.001, fetch: lambda do |_tenant, context|
        Acp::Batch.new(data: [], next_cursor: context.cursor + 1, size_bytes: size)
      end, ingest: ->(*) { flunk "oversized payload was ingested" })
      program.max_batch_bytes 50
      program.retained_byte_capacity capacity
      program.payload_estimate { |_| estimate }
      runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress,
                                 on_error: ->(_id, error, _stage) { errors << error })
      run_for(runtime, duration: 0.015)
      assert_instance_of Acp::PayloadLimitError, errors.first
      assert_empty progress.acknowledgements
      assert_equal 0, runtime.metrics[:reserved_bytes]
      assert_equal 0, runtime.metrics[:retained_bytes]
      assert_operator runtime.metrics[metric], :>, 0 if metric
    end
  end

  def test_cancellation_holds_bytes_until_ingestion_cleanup_returns
    entered = false
    cleanup_bytes = nil
    runtime = nil
    program = build_program(interval: 0.001, fetch: lambda do |_tenant, context|
      Acp::Batch.new(data: "x", next_cursor: context.cursor, size_bytes: 20)
    end, ingest: lambda do |*|
      entered = true
      begin
        Kernel.sleep(60)
      ensure
        Async::Task.current.defer_stop do
          Kernel.sleep(0.005)
          cleanup_bytes = runtime.metrics.values_at(:reserved_bytes, :retained_bytes, :pipeline_reservations)
        end
      end
    end)
    program.max_batch_bytes 30
    program.retained_byte_capacity 30
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(2) { Kernel.sleep(0.001) until entered }
      runner.stop
      runner.wait
    end
    assert_equal [30, 20, 1], cleanup_bytes
    assert_equal 0, runtime.metrics[:reserved_bytes]
    assert_equal 0, runtime.metrics[:retained_bytes]
  end

  # Gates and arrivals deliberately stay together to make FIFO ordering visible.
  # rubocop:disable Metrics/BlockLength
  def test_weighted_byte_admission_is_fifo_and_cancellation_unblocks_followers
    metrics = {}
    budget = Acp::ByteBudget.new(10, metrics)
    order = []
    Async do |root|
      budget.acquire(6)
      large = root.async do
        budget.acquire(8)
        order << :large
        budget.release(8)
      end
      small = root.async do
        budget.acquire(2)
        order << :small
        budget.release(2)
      end
      assert_empty order
      large.stop
      small.wait
      assert_equal [:small], order
      budget.release(6)
      first = root.async do
        budget.acquire(10)
        Kernel.sleep(0.001)
        budget.release(10)
      end
      second = root.async do
        budget.acquire(9)
        order << :second
        budget.release(9)
      end
      third = root.async do
        budget.acquire(1)
        order << :third
        budget.release(1)
      end
      [first, second, third].each(&:wait)
    end
    assert_equal %i[small second third], order
    assert_equal 0, metrics[:reserved_bytes]
    assert_equal 0, metrics[:byte_budget_waiters]
    assert_equal 10, metrics[:max_reserved_bytes]
  end
  # rubocop:enable Metrics/BlockLength

  def test_byte_blocked_fresh_work_cannot_deadlock_a_parked_retry
    attempts = Hash.new(0)
    progress = MemoryProgress.new
    program = build_program(tenants: %w[a b c], interval: 60, fetch: lambda do |_tenant, context|
      Acp::Batch.new(data: "payload", next_cursor: context.cursor + 1, size_bytes: 10)
    end, ingest: lambda do |_batch, context|
      attempts[context.tenant_id] += 1
      raise IOError, "retry" if context.tenant_id == "a" && attempts["a"] == 1
    end, ingest_retry: { on: IOError, max_attempts: 2, backoff: 0.01, max_backoff: 0.01 })
    program.pipeline_capacity 1
    program.retry_capacity 1
    program.max_batch_bytes 10
    program.retained_byte_capacity 10
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress)
    runtime.define_singleton_method(:initial_offset) { |_| 0 }
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(2) { Kernel.sleep(0.001) until progress.acknowledgements.length == 3 }
      runtime.request_shutdown
      runner.wait
    end
    assert_equal 2, attempts["a"]
    assert_equal 3, runtime.metrics[:completed_cycles]
    assert_operator runtime.metrics[:byte_budget_waits], :>, 0
    assert_equal 10, runtime.metrics[:max_reserved_bytes]
    assert_equal 0, runtime.metrics[:reserved_bytes]
  end

  def test_byte_accounting_returns_to_baseline_after_exhaustion_and_lease_loss
    %i[exhaustion lease_loss].each do |failure|
      progress = MemoryProgress.new
      ownership = Acp::LocalOwnership.new
      if failure == :lease_loss
        ownership.define_singleton_method(:renewal_interval) { |_| 0.01 }
        ownership.define_singleton_method(:renew) { |*| false }
      end
      program = build_program(interval: 60, fetch: lambda do |_tenant, context|
        Acp::Batch.new(data: "payload", next_cursor: context.cursor + 1, size_bytes: 20)
      end, ingest: lambda do |*|
        Kernel.sleep(60) if failure == :lease_loss
        raise IOError, "exhausted"
      end, ingest_retry: { on: IOError, max_attempts: 2 })
      program.max_batch_bytes 20
      program.retained_byte_capacity 20
      runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress, ownership: ownership)
      runtime.define_singleton_method(:initial_offset) { |_| 0 }
      run_for(runtime, duration: 0.04)
      assert_empty progress.acknowledgements
      assert_equal 20, runtime.metrics[:max_retained_bytes]
      assert_equal 0, runtime.metrics[:reserved_bytes]
      assert_equal 0, runtime.metrics[:retained_bytes]
    end
  end

  def test_response_limit_rejects_cumulative_wire_bytes
    context = Acp::FetchContext.new(tenant_id: "a", cursor: Time.utc(2025), poll_id: "poll", max_response_bytes: 20)
    assert_equal 20, context.validate_response_bytes!(20)
    assert_raises(Acp::PayloadLimitError) { context.validate_response_bytes!(21) }
    assert_raises(ArgumentError) { context.validate_response_bytes!(-1) }
  end

  def test_scheduler_weighted_admission_does_not_spawn_or_overtake_byte_waiters
    started = []
    gate = Async::Queue.new
    sizes = { "a" => 6, "b" => 8, "c" => 2 }
    program = build_program(tenants: sizes.keys, interval: 60, fetch: lambda do |tenant, context|
      started << tenant
      gate.pop
      Acp::Batch.new(data: "payload", next_cursor: context.cursor, size_bytes: sizes.fetch(tenant))
    end)
    program.pipeline_capacity 3
    program.max_batch_bytes 10
    program.retained_byte_capacity 10
    program.payload_estimate { |tenant| sizes.fetch(tenant) }
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)
    runtime.define_singleton_method(:initial_offset) { |_| 0 }
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(2) do
        Kernel.sleep(0.001) until started == ["a"]
        assert_equal 1, runtime.metrics[:active_cycles]
        assert_equal 6, runtime.metrics[:reserved_bytes]
        gate.push(true)
        Kernel.sleep(0.001) until started.length == 3
        assert_equal %w[a b c], started
        assert_equal 10, runtime.metrics[:reserved_bytes]
        runtime.request_shutdown
        2.times { gate.push(true) }
        runner.wait
      end
    end
    assert_equal 0, runtime.metrics[:reserved_bytes]
  end

  def test_successful_cycle_uses_max_of_interval_and_acknowledgement_time
    [10, 80].each do |fetch_duration|
      clock = FakeClock.new
      starts = []
      fetch = lambda do |_tenant, context|
        starts << clock.now
        if starts.length == 1
          clock.sleep(fetch_duration)
        else
          Kernel.sleep(60)
        end
        Acp::Batch.new(data: [], next_cursor: context.cursor)
      end
      program = build_program(interval: 60, fetch: fetch)
      runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new, clock: clock)

      Async do |root|
        task = root.async { runtime.run }
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
        Kernel.sleep(0.001) while starts.length < 2 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        task.stop
      end

      assert_in_delta(starts[0] + [60, fetch_duration].max, starts[1], 0.001)
    end
  end

  def test_slow_ingestion_postpones_the_next_poll
    fetch_starts = []
    ingestion_finishes = []
    fetch = lambda do |_tenant, context|
      fetch_starts << Process.clock_gettime(Process::CLOCK_MONOTONIC)
      Kernel.sleep(60) if fetch_starts.length > 1
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end
    ingest = lambda do |_batch, _context|
      Kernel.sleep(0.04)
      ingestion_finishes << Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    program = build_program(interval: 0.001, fetch: fetch, ingest: ingest)
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)

    Async do |root|
      task = root.async { runtime.run }
      Kernel.sleep(0.001) while fetch_starts.length < 2
      task.stop
    end

    assert_operator fetch_starts[1], :>=, ingestion_finishes.first
  end

  def test_failed_tenant_does_not_block_others_and_fetch_retry_reuses_identity
    attempts = Hash.new(0)
    contexts = []
    completed = []
    fetch = lambda do |tenant, context|
      attempts[tenant] += 1
      contexts << [tenant, context.poll_id, context.attempt]
      raise IOError, "temporary" if tenant == "a" && attempts[tenant] == 1

      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end
    program = build_program(
      tenants: %w[a b],
      fetch: fetch,
      ingest: ->(_batch, context) { completed << context.tenant_id },
      fetch_retry: { on: IOError, max_attempts: 2, max_elapsed: 5 }
    )
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)

    Async do |root|
      task = root.async { runtime.run }
      Kernel.sleep(0.1)
      task.stop
    end

    a_contexts = contexts.select { |tenant, _poll_id, _attempt| tenant == "a" }
    assert_equal [1, 2], a_contexts.map(&:last).first(2).sort
    assert_equal a_contexts.first[1], a_contexts[1][1]
    refute_empty a_contexts.first[1]
    assert_includes completed, "b"
  end

  def test_ingestion_is_not_replayed_when_acknowledgement_retries
    progress = MemoryProgress.new
    acknowledgements = 0
    progress.define_singleton_method(:acknowledge) do |tenant_id, poll_id, cursor|
      acknowledgements += 1
      raise IOError, "ack unavailable" if acknowledgements == 1

      super(tenant_id, poll_id, cursor)
    end
    batches = []
    program = build_program(
      ingest: lambda do |batch, context|
        batches << [batch.object_id, context.poll_id, context.attempt]
        raise IOError, "ingest unavailable" if batches.length == 1
      end,
      ingest_retry: { on: IOError, max_attempts: 3, max_elapsed: 5 }
    )
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress)

    Async do |root|
      task = root.async { runtime.run }
      Kernel.sleep(0.001) while acknowledgements < 2
      task.stop
    end

    assert_equal [1, 2], batches.map(&:last)
    assert_equal batches.first.first(2), batches.last.first(2)
    assert_operator acknowledgements, :>=, 2
  end

  def test_cancellation_releases_fetch_and_pipeline_reservations
    fetch = lambda do |_tenant, _context|
      Kernel.sleep(60)
      Acp::Batch.new(data: [], next_cursor: Time.utc(2025, 1, 1))
    end
    program = build_program(fetch: fetch)
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)

    run_for(runtime, duration: 0.01)

    assert_equal 0, runtime.metrics[:active_cycles]
    assert_equal 0, runtime.metrics[:active_fetches]
    assert_equal 0, runtime.metrics[:queued_batches]
    assert_equal 0, runtime.metrics[:active_ingests]
  end

  def test_lost_lease_cancels_outstanding_fetch_before_ingestion
    fetch_stopped = []
    ingestions = []
    ownership = Object.new
    ownership.define_singleton_method(:acquire) { |_tenant_id| "lease" }
    ownership.define_singleton_method(:release) { |_tenant_id, _token| nil }
    ownership.define_singleton_method(:renewal_interval) { |_token| 0.01 }
    ownership.define_singleton_method(:renew) { |_tenant_id, _token| false }
    program = build_program(
      fetch: lambda do |_tenant, _context|
        Kernel.sleep(60)
      ensure
        fetch_stopped << true
      end,
      ingest: ->(_batch, _context) { ingestions << true }
    )
    runtime = Acp::Runtime.new(
      configuration: program.configuration,
      progress: MemoryProgress.new,
      ownership: ownership
    )

    Async do |root|
      task = root.async { runtime.run }
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
      Kernel.sleep(0.005) while fetch_stopped.empty? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      task.stop
    end

    refute_empty fetch_stopped
    assert_empty ingestions
    assert_equal 0, runtime.metrics[:active_cycles]
    assert_equal 0, runtime.metrics[:active_fetches]
  end

  def test_periodic_discovery_adds_tenants_without_restarting_runtime
    tenant_ids = ["a"]
    fetches = Hash.new(0)
    program = Class.new(Acp::Program) do
      define_singleton_method(:name) { "DynamicDiscoveryTest" }
      interval 60
      discovery_interval 0.02
      fetch_concurrency 2
      ingest_concurrency 1
      pipeline_capacity 2
      tenants { |emit| tenant_ids.each { |tenant_id| emit.call(tenant_id) } }
      initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
      resolve { |tenant_id| tenant_id }
      fetch do |_tenant, context|
        fetches[context.tenant_id] += 1
        Acp::Batch.new(data: [], next_cursor: context.cursor)
      end
      ingest { |_batch, _context| }
    end
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new,
                               ownership: MemoryDiscoveryOwnership.new)

    Async do |root|
      task = root.async { runtime.run }
      Kernel.sleep(0.03)
      tenant_ids << "b"
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
      Kernel.sleep(0.005) while fetches["b"].zero? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      runtime.request_shutdown(timeout: 1)
      task.wait
    end

    assert_operator fetches["b"], :>=, 1
    assert_equal 0, runtime.metrics[:active_cycles]
  end

  def test_lease_loss_removes_queued_ingestion_before_admitting_more_work
    ingested = []
    ownership = Acp::LocalOwnership.new
    ownership.define_singleton_method(:renewal_interval) { |_token| 0.01 }
    ownership.define_singleton_method(:renew) { |tenant_id, _token| tenant_id != "b" }
    program = build_program(tenants: %w[a b c], interval: 1, ingest: lambda do |_batch, context|
      ingested << context.tenant_id
      Kernel.sleep(0.05) if context.tenant_id == "a"
    end)
    program.pipeline_capacity 2
    program.ingest_concurrency 1
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new,
                               ownership: ownership)
    runtime.define_singleton_method(:initial_offset) { |_tenant_id| 0 }

    run_for(runtime, duration: 0.1)

    assert_equal %w[a c], ingested
    assert_equal 1, runtime.metrics[:renewal_deadline_misses]
    assert_operator runtime.metrics[:max_queued_batches], :<=, 2
    assert_equal 0, runtime.metrics[:queued_batches]
    assert_equal 0, runtime.metrics[:active_ingests]
  end

  def test_lease_loss_unwinds_active_ingestion_before_releasing_ownership
    order = []
    progress = MemoryProgress.new
    ownership = Acp::LocalOwnership.new
    ownership.define_singleton_method(:renewal_interval) { |_token| 0.01 }
    ownership.define_singleton_method(:renew) { |*| false }
    ownership.define_singleton_method(:release) { |*| order << :released }
    program = build_program(interval: 1, ingest: lambda do |*|
      Kernel.sleep(60)
    ensure
      order << :unwound
    end)
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress, ownership: ownership)
    runtime.define_singleton_method(:initial_offset) { |_tenant_id| 0 }

    run_for(runtime, duration: 0.05)

    assert_equal %i[unwound released], order
    assert_empty progress.acknowledgements
    assert_equal 0, runtime.metrics[:active_ingests]
  end

  def test_remote_resume_restores_a_paused_tenants_schedule
    paused = true
    fetches = 0
    ownership = Acp::LocalOwnership.new
    ownership.define_singleton_method(:tenant_paused?) { |_id| paused }
    program = build_program(interval: 0.01, fetch: lambda do |_tenant, context|
      fetches += 1
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end)
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new,
                               ownership: ownership)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    Async do |root|
      runner = root.async { runtime.run }
      Kernel.sleep(0.03)
      assert_equal 0, fetches
      paused = false
      Kernel.sleep(0.04)
      runtime.request_shutdown(timeout: 1)
      runner.wait
    end

    assert_operator fetches, :>, 0
  end

  def test_assignment_refresh_consumes_membership_without_discovery_leadership
    assigned = ["a"]
    fetched = []
    ownership = Acp::LocalOwnership.new
    ownership.define_singleton_method(:acquire_discovery) { nil }
    ownership.define_singleton_method(:enabled_tenants) { assigned.dup }
    ownership.define_singleton_method(:scheduling_tenants) { assigned.dup }
    ownership.define_singleton_method(:renewal_interval) { |_token| 0.01 }
    program = build_program(interval: 1, fetch: lambda do |_tenant, context|
      fetched << context.tenant_id
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end)
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new,
                               ownership: ownership)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    Async do |root|
      runner = root.async { runtime.run }
      Kernel.sleep(0.02)
      assigned.replace(["b"])
      Kernel.sleep(0.04)
      runtime.request_shutdown(timeout: 1)
      runner.wait
    end

    assert_equal %w[a b], fetched
    assert_equal 1, runtime.metrics[:assigned_tenants]
  end

  def test_resolution_is_bounded_independently_of_fetch_concurrency
    active = 0
    maximum = 0
    program = build_program(tenants: (1..8).to_a, interval: 1, fetch: lambda do |_tenant, context|
      Kernel.sleep(0.05)
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end)
    program.fetch_concurrency 6
    program.pipeline_capacity 6
    program.resolve_concurrency 2
    program.resolve do |id|
      active += 1
      maximum = [maximum, active].max
      Kernel.sleep(0.01)
      id
    ensure
      active -= 1
    end
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    run_for(runtime, duration: 0.15)

    assert_equal 2, maximum
    assert_operator runtime.metrics[:max_active_fetches], :>, 2
    assert_equal 0, active
  end

  def test_overlapping_cycles_reschedule_their_own_tenant
    fetches = Hash.new(0)
    program = build_program(tenants: %w[a b c], interval: 0.03, fetch: lambda do |tenant, context|
      fetches[tenant] += 1
      Kernel.sleep(tenant == "a" ? 0.02 : 0.005)
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end)
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    run_for(runtime, duration: 0.12)

    %w[a b c].each { |id| assert_operator fetches[id], :>=, 3 }
    assert_equal 0, runtime.metrics[:active_cycles]
  end

  def test_fetch_timeout_retries_within_a_total_deadline
    attempts = []
    program = build_program(interval: 1, fetch: lambda do |_tenant, context|
      attempts << context
      Kernel.sleep(60)
    end, fetch_retry: { on: Timeout::Error, max_attempts: 5, timeout: 0.01, max_elapsed: 0.025 })
    progress = MemoryProgress.new
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    run_for(runtime, duration: 0.06)

    assert_operator attempts.length, :>=, 2
    assert_operator attempts.length, :<=, 3
    assert_equal 1, attempts.map(&:poll_id).uniq.length
    assert_empty progress.acknowledgements
    assert_equal 0, runtime.metrics[:active_fetches]
  end

  def test_backoff_cannot_start_an_attempt_after_the_deadline
    attempts = 0
    program = build_program(interval: 1, fetch: lambda do |*|
      attempts += 1
      raise IOError
    end, fetch_retry: { on: IOError, max_attempts: 3, max_elapsed: 0.02, backoff: 1, max_backoff: 1 })
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    run_for(runtime, duration: 0.05)

    assert_equal 1, attempts
    assert_equal 0, runtime.metrics[:active_cycles]
  end

  def test_ingestion_deadline_unwinds_work_without_acknowledging
    unwound = false
    progress = MemoryProgress.new
    program = build_program(interval: 1, ingest: lambda do |*|
      Kernel.sleep(60)
    ensure
      unwound = true
    end, ingest_retry: { max_elapsed: 0.02 })
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    run_for(runtime, duration: 0.05)

    assert unwound
    assert_empty progress.acknowledgements
    assert_equal 0, runtime.metrics[:active_ingests]
    assert_equal 0, runtime.metrics[:queued_batches]
  end

  def test_ingestion_attempt_timeout_can_retry_the_same_batch
    attempts = []
    progress = MemoryProgress.new
    program = build_program(interval: 1, ingest: lambda do |batch, context|
      attempts << [batch.object_id, context.poll_id, context.attempt]
      Kernel.sleep(60) if context.attempt == 1
    end, ingest_retry: { on: Timeout::Error, timeout: 0.01, max_elapsed: 0.1, max_attempts: 2 })
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    run_for(runtime, duration: 0.05)

    assert_equal [1, 2], attempts.map(&:last)
    assert_equal attempts.first.first(2), attempts.last.first(2)
    assert_equal 1, progress.acknowledgements.length
    assert_equal 0, runtime.metrics[:active_ingests]
  end

  def test_timeout_after_confirmed_commit_does_not_replay_ingestion
    committed = 0
    progress = MemoryProgress.new
    subscription = ActiveSupport::Notifications.subscribe("ingestion_completed.acp") { |*| Kernel.sleep(60) }
    program = build_program(interval: 1, ingest: ->(*) { committed += 1 },
                            ingest_retry: { on: Timeout::Error, timeout: 0.01, max_elapsed: 0.1, max_attempts: 2 })
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    run_for(runtime, duration: 0.05)

    assert_equal 1, committed
    assert_equal 1, progress.acknowledgements.length
    assert_equal 0, runtime.metrics[:active_ingests]
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription) if subscription
  end

  def test_graceful_shutdown_finishes_active_ingestion_before_returning
    fetch_started = false
    ingested = false
    program = build_program(
      interval: 0.01,
      fetch: lambda do |_tenant, context|
        fetch_started = true
        Kernel.sleep(0.03)
        Acp::Batch.new(data: [], next_cursor: context.cursor)
      end,
      ingest: ->(_batch, _context) { ingested = true }
    )
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new, drain_timeout: 1)

    Async do |root|
      task = root.async { runtime.run }
      Kernel.sleep(0.005) until fetch_started
      runtime.request_shutdown
      task.wait
    end

    assert ingested
    assert_equal 0, runtime.metrics[:active_cycles]
    assert_equal 0, runtime.metrics[:active_ingests]
  end

  def test_drain_deadline_unwinds_ingestion_before_releasing_ownership
    order = []
    ingest_started = false
    ownership = Object.new
    ownership.define_singleton_method(:acquire) { |_tenant_id| "lease" }
    ownership.define_singleton_method(:release) { |_tenant_id, _token| order << :released }
    program = build_program(
      interval: 0.001,
      ingest: lambda do |_batch, _context|
        ingest_started = true
        begin
          Kernel.sleep(60)
        ensure
          order << :ingestion_unwound
        end
      end
    )
    runtime = Acp::Runtime.new(
      configuration: program.configuration,
      progress: MemoryProgress.new,
      ownership: ownership,
      drain_timeout: 0.03
    )

    Async do |root|
      task = root.async { runtime.run }
      Kernel.sleep(0.001) until ingest_started
      runtime.request_shutdown(timeout: 0.03)
      task.wait
    end

    assert_equal %i[ingestion_unwound released], order
    assert_equal 0, runtime.metrics[:active_ingests]
  end

  # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def test_notifications_include_diagnostics_without_batch_or_error_bodies
    payloads = []
    subscription = ActiveSupport::Notifications.subscribe(/\.acp\z/) do |name, _start, _finish, _id, payload|
      payloads << [name, payload]
    end
    secret = "private-credential-and-payload"
    program = build_program(
      interval: 0.001,
      fetch: ->(_tenant, context) { Acp::Batch.new(data: [secret], next_cursor: context.cursor) }
    )
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)

    Async do |root|
      task = root.async { runtime.run }
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
      Kernel.sleep(0.001) while payloads.none? { |name, _payload| name == "poll_completed.acp" } &&
                                Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      runtime.request_shutdown(timeout: 1)
      task.wait
    end

    event_payloads = payloads.map(&:last)
    refute_empty event_payloads
    assert(event_payloads.any? { |payload| payload[:program] == program.configuration.name })
    assert(event_payloads.any? { |payload| payload[:tenant_id] == "a" && payload[:poll_id] })
    refute_includes event_payloads.inspect, secret
    refute(event_payloads.any? { |payload| payload.key?(:data) })
    refute_includes runtime.metrics.keys, :tenant_id
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription) if subscription
  end
  # rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

  def test_admission_lateness_includes_failed_cycles_without_cycle_overrun
    clock = FakeClock.new
    starts = []
    waiting = []
    runtime = nil
    failed = Async::Queue.new
    program = build_program(tenants: %w[a b c], interval: 1, fetch: lambda do |_tenant, context|
      starts << clock.now
      waiting << runtime.metrics[:overdue_tenants]
      raise IOError, "last cycle fails" if starts.length == 3

      clock.sleep(0.03)
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end)
    program.pipeline_capacity 1
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new, clock: clock,
                               on_error: ->(*) { failed.push(true) })
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    Async do |root|
      runner = root.async { runtime.run }
      failed.pop
      runner.stop
    end

    assert_equal [0.0, 0.03, 0.06], starts
    assert_equal [2, 1, 0], waiting
    pressure = runtime.metrics[:pressure]
    assert_equal 3, pressure[:admission_lag][:count]
    assert_in_delta 0.09, pressure[:admission_lag][:sum]
    assert_in_delta 0.06, runtime.metrics[:polling_lag]
    assert_equal 0.05, runtime.metrics[:polling_lag_p50]
    assert_equal 0.1, runtime.metrics[:polling_lag_p95]
    assert_equal 0, pressure[:cycle_overrun][:max]
    assert_equal 1, pressure[:cycle_duration][:outcomes][:error]
    assert_equal 2, runtime.metrics[:completed_cycles]
  end

  def test_overdue_population_is_live_during_indefinite_work_and_membership_changes
    clock = ControlledClock.new
    gate = Async::Queue.new
    started = Async::Queue.new
    program = build_program(tenants: %w[a b c], interval: 60, fetch: lambda do |*|
      started.push(true)
      gate.pop
    end)
    program.pipeline_capacity 1
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new, clock: clock)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    Async do |root|
      runner = root.async { runtime.run }
      started.pop
      clock.sleep(4)
      assert_equal 0, runtime.metrics[:completed_cycles]
      assert_equal 2, runtime.health[:metrics][:overdue_tenants]
      assert_equal 4, runtime.metrics[:oldest_overdue_age]
      runtime.pause_tenant("b")
      assert_equal 1, runtime.metrics[:overdue_tenants]
      runtime.send(:apply_discovery, %w[a b], Acp::Runtime::DueHeap.new, 3)
      assert_equal 0, runtime.metrics[:overdue_tenants]
      runtime.resume_tenant("b")
      root.yield
      assert_equal 1, runtime.metrics[:overdue_tenants]
      assert_equal 0, runtime.metrics[:oldest_overdue_age]
      clock.sleep(2)
      assert_equal 2, runtime.metrics[:oldest_overdue_age]
      runner.stop
    end

    assert_equal 0, runtime.metrics[:overdue_tenants]
    assert_equal 1, runtime.metrics[:pressure][:fetch_service][:outcomes][:cancelled]
    assert_equal 1, runtime.metrics[:pressure][:cycle_duration][:outcomes][:cancelled]
  end

  # Both stages exercise the same gated contention boundaries.
  # rubocop:disable Metrics/BlockLength
  def test_resolution_and_fetch_wait_are_separate_from_callback_service
    %i[resolve fetch].each do |stage|
      clock = FakeClock.new
      gate = Async::Queue.new
      callback = lambda do |tenant, context = nil|
        if tenant == "first"
          gate.pop
        else
          clock.sleep(3)
        end
        Acp::Batch.new(data: [], next_cursor: context.cursor) if context
      end
      program = build_program(fetch: callback)
      program.fetch_concurrency 1
      program.resolve_concurrency 1
      runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new, clock: clock)
      invoke = lambda do |tenant|
        if stage == :resolve
          runtime.send(:resolve_stage) { callback.call(tenant) }
        else
          context = Acp::FetchContext.new(tenant_id: tenant, cursor: Time.utc(2025), poll_id: "poll", attempt: 1)
          runtime.send(:fetch, tenant, context)
        end
      end

      Async do |root|
        first = root.async { invoke.call("first") }
        second = root.async { invoke.call("second") }
        clock.sleep(2)
        gate.push(true)
        [first, second].each(&:wait)
      end

      pressure = runtime.metrics[:pressure]
      assert_equal 2, pressure[:"#{stage}_wait"][:count]
      assert_equal 2, pressure[:"#{stage}_wait"][:sum]
      assert_equal 5, pressure[:"#{stage}_service"][:sum]
      assert_equal 3, pressure[:"#{stage}_service"][:max]
    end
  end
  # rubocop:enable Metrics/BlockLength

  def test_initial_cursor_acknowledgement_backoff_and_cycle_overrun_boundaries
    clock = FakeClock.new
    attempts = 0
    progress = MemoryProgress.new
    progress.define_singleton_method(:read) do |_id|
      clock.sleep(1)
      nil
    end
    progress.define_singleton_method(:initialize_cursor) do |_id, cursor|
      clock.sleep(2)
      cursor
    end
    progress.define_singleton_method(:acknowledge) do |*|
      clock.sleep(3)
      attempts += 1
      raise IOError, "retry" if attempts == 1
    end
    program = build_program(interval: 10, ingest_retry: { on: IOError, max_attempts: 2, backoff: 4, max_backoff: 4 })
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress, clock: clock)
    runtime.instance_variable_set(:@ingest_queue, Acp::Runtime::IngestQueue.new(1))

    Async do |root|
      worker = root.async { runtime.send(:ingestion_worker, runtime.instance_variable_get(:@ingest_queue)) }
      assert_equal 13, runtime.send(:run_cycle, "a", 0)
      worker.stop
    end

    pressure = runtime.metrics[:pressure]
    assert_equal 1, pressure[:cursor_read_service][:sum]
    assert_equal 2, pressure[:cursor_initialize_service][:sum]
    assert_equal 6, pressure[:acknowledge_service][:sum]
    assert_equal 1, pressure[:acknowledge_service][:outcomes][:error]
    assert_equal 0, pressure[:acknowledge_wait][:sum]
    assert_equal 4, pressure[:acknowledge_backoff][:sum]
    assert_equal 1, pressure[:acknowledge_backoff][:outcomes][:success]
    assert_equal 10, pressure[:cursor_update_delay][:sum]
    assert_equal 3, pressure[:cycle_overrun][:max]
    assert_equal 0, pressure[:admission_lag][:max]
  end

  def test_ingestion_queue_wait_and_cancelled_wait_are_recorded_once
    clock = ControlledClock.new
    gate = Async::Queue.new
    started = Async::Queue.new
    program = build_program(tenants: %w[a b c], interval: 60, ingest: lambda do |_batch, context|
      started.push(context.tenant_id)
      gate.pop
    end)
    program.ingest_concurrency 1
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new, clock: clock)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }

    Async do |root|
      runner = root.async { runtime.run }
      assert_equal "a", started.pop
      root.yield until runtime.metrics[:queued_batches] == 2
      clock.sleep(5)
      gate.push(true)
      assert_equal "b", started.pop
      root.yield until runtime.metrics[:completed_cycles] == 1
      clock.sleep(2)
      runner.stop
    end

    pressure = runtime.metrics[:pressure]
    assert_equal 3, pressure[:ingest_wait][:count]
    assert_equal 12, pressure[:ingest_wait][:sum] # a=0, b=5, c=7 (cancelled).
    assert_equal 1, pressure[:ingest_wait][:outcomes][:cancelled]
    assert_equal 7, pressure[:ingest_service][:sum] # a=5, b=2 (cancelled).
    assert_equal 1, pressure[:ingest_service][:outcomes][:cancelled]
  end

  def test_fetch_backoff_timeouts_and_cancellation_do_not_become_service_time
    clock = FakeClock.new
    attempts = 0
    program = build_program(fetch_retry: { on: IOError, max_attempts: 2, backoff: 4, max_backoff: 4 },
                            fetch: lambda do |_tenant, context|
                              clock.sleep(2)
                              attempts += 1
                              raise IOError, "retry" if attempts == 1

                              Acp::Batch.new(data: [], next_cursor: context.cursor)
                            end)
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new, clock: clock)
    context = Acp::FetchContext.new(tenant_id: "a", cursor: Time.utc(2025), poll_id: "poll", attempt: 1)
    Async do
      runtime.send(:retry_stage, program.configuration.fetch_retry, :fetch, "a") do
        runtime.send(:fetch, "a", context)
      end
      policy = Acp::RetryPolicy.new(timeout: 0.001)
      assert_raises(Acp::StageTimeoutError) do
        runtime.send(:retry_stage, policy, :fetch, "a") do
          runtime.send(:resolve_stage) { Kernel.sleep(60) }
        end
      end
    end.wait

    pressure = runtime.metrics[:pressure]
    assert_equal 4, pressure[:fetch_service][:sum]
    assert_equal 4, pressure[:fetch_backoff][:sum]
    assert_equal 1, pressure[:fetch_backoff][:outcomes][:success]
    assert_equal 1, pressure[:fetch_service][:outcomes][:error]
    assert_equal 1, pressure[:resolve_service][:outcomes][:timeout]
  end

  def test_reactor_and_renewal_wakeup_lag_have_explicit_monotonic_boundaries
    clock = FakeClock.new
    ownership = Acp::LocalOwnership.new
    ownership.define_singleton_method(:renewal_interval) { |_token| 1 }
    ownership.define_singleton_method(:renew) { |*| true }
    # The clock advances 0.25 beyond each requested heartbeat sleep.
    clock.define_singleton_method(:sleep) do |duration|
      @time += duration + 0.25
      Async::Task.current.yield
    end
    program = build_program
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new,
                               ownership: ownership, clock: clock)
    Async do |root|
      sampler = runtime.send(:start_reactor_sampler)
      clock.sleep(0.1) # moves monotonic time to 0.35 before the sampler wakes.
      Kernel.sleep(0.11)
      sampler.stop
      assert_in_delta 0.25, runtime.metrics[:pressure][:reactor_lag][:last]
      heartbeat = runtime.send(:start_lease_heartbeat, "a", true, { lost: false })
      root.yield # first wakeup is observed, then the next sleep yields.
      heartbeat.stop
    end
    assert_in_delta 0.25, runtime.metrics[:pressure][:renewal_wakeup_lag][:last]
  end

  def test_histograms_are_bounded_immutable_and_have_a_real_overflow_bucket
    clock = FakeClock.new
    pressure = Acp::PressureMetrics.new([:sample], clock: clock)
    10_000.times { |index| pressure.observe(:sample, index) }
    snapshot = pressure.snapshot
    assert_equal 10_000, snapshot[:sample][:count]
    assert_equal Acp::PressureMetrics::BOUNDARIES.length + 1, snapshot[:sample][:buckets].length
    assert_nil snapshot[:sample][:p99]
    assert_equal 9999, snapshot[:sample][:max]
    assert_raises(FrozenError) { snapshot[:sample][:buckets] << 1 }
    assert_raises(FrozenError) { snapshot[:sample][:outcomes][:error] = 1 }
    pressure.observe(:sample, 0)
    assert_equal 10_000, snapshot[:sample][:count]
    assert_equal 10_001, pressure.snapshot[:sample][:count]
    runtime = Acp::Runtime.new(configuration: build_program.configuration, progress: MemoryProgress.new)
    runtime.instance_variable_get(:@pressure).observe(:admission_lag, 301)
    assert_nil runtime.metrics.fetch(:polling_lag_p95)
  end

  def test_fetch_timeout_while_waiting_does_not_start_service_or_leak_capacity
    clock = ControlledClock.new
    gate = Async::Queue.new
    started = Async::Queue.new
    program = build_program(fetch: lambda do |_tenant, context|
      started.push(true)
      gate.pop
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end)
    program.fetch_concurrency 1
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new, clock: clock)
    context = Acp::FetchContext.new(tenant_id: "a", cursor: Time.utc(2025), poll_id: "poll", attempt: 1)
    Async do |root|
      first = root.async { runtime.send(:fetch, "a", context) }
      started.pop
      second = root.async do
        assert_raises(Acp::StageTimeoutError) do
          runtime.send(:retry_stage, Acp::RetryPolicy.new(timeout: 0.001), :fetch, "b") do
            runtime.send(:fetch, "b", context)
          end
        end
      end
      clock.sleep(2)
      second.wait
      gate.push(true)
      first.wait
    end
    pressure = runtime.metrics[:pressure]
    assert_equal 2, pressure[:fetch_wait][:sum]
    assert_equal 1, pressure[:fetch_wait][:outcomes][:timeout]
    assert_equal 1, pressure[:fetch_service][:count]
    assert_equal 2, pressure[:fetch_service][:sum]
    assert_equal 0, runtime.metrics[:active_fetches]
  end

  def isolated_runtime(program, offset: ->(_id) { 0 }, **options)
    program.pipeline_capacity 2
    program.retry_capacity 2
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new, **options)
    runtime.define_singleton_method(:initial_offset, &offset)
    runtime
  end

  def assert_reservations_released(runtime)
    %i[active_cycles active_fetches active_ingests queued_batches pipeline_reservations
       retry_reservations parked_cycles upstream_groups retained_batches pending_acknowledgements].each do |name|
      assert_equal 0, runtime.metrics.fetch(name), name.to_s
    end
  end

  def test_full_fetch_retry_saturation_admits_healthy_work_before_backoff_expires
    attempts = []
    healthy = Async::Queue.new
    program = build_program(tenants: %w[a b healthy], interval: 60, fetch: lambda do |tenant, context|
      attempts << [tenant, context.attempt]
      raise IOError if tenant != "healthy"

      healthy.push(true)
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end, fetch_retry: { on: IOError, max_attempts: 3, backoff: 10, max_backoff: 10 })
    runtime = isolated_runtime(program)
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(1) { healthy.pop }
      assert_equal [["a", 1], ["b", 1], ["healthy", 1]], attempts
      assert_equal 2, runtime.metrics[:parked_cycles]
      assert_equal 0, runtime.metrics[:active_fetches]
      runner.stop
    end.wait
    assert_operator runtime.metrics[:max_active_cycles], :<=, 4
    assert_operator runtime.metrics[:max_pipeline_reservations], :<=, 2
    assert_equal 2, runtime.metrics[:max_retry_reservations]
    assert_reservations_released(runtime)
  end

  # Follow identity across both independently retried stages and fresh arrivals.
  # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Metrics/BlockLength
  def test_retry_and_fresh_work_both_progress_with_stable_fetch_and_ingestion_identity
    %i[fetch ingest].each do |stage|
      calls = []
      resolved = []
      finished = Async::Queue.new
      callback = lambda do |value, context|
        calls << [context.tenant_id, context.attempt, context.poll_id, context.cursor, value.object_id]
        Kernel.sleep(0.002)
        raise IOError if context.tenant_id == "a" && context.attempt < 3

        finished.push(context.tenant_id)
        Acp::Batch.new(data: [], next_cursor: context.cursor) if stage == :fetch
      end
      program = build_program(tenants: ("a".."z").to_a, interval: 60,
                              **{ stage => callback, :"#{stage}_retry" => {
                                on: IOError, max_attempts: 3, backoff: 0.003, max_backoff: 0.003
                              } })
      program.resolve do |id|
        resolved << id
        id.dup
      end
      runtime = isolated_runtime(program)
      Async do |root|
        runner = root.async { runtime.run }
        root.with_timeout(1) { finished.pop until calls.any? { |id, attempt, *| id == "a" && attempt == 3 } }
        runner.stop
      end.wait
      retries = calls.select { |call| call.first == "a" }
      assert_equal([1, 2, 3], retries.map { |call| call[1] })
      assert_equal 1, retries.map { |call| call[2..] }.uniq.length
      assert_equal 1, resolved.count("a")
      assert_operator calls.index(retries.last), :<, 25
      assert(calls[1...calls.index(retries.last)].any? { |call| call.first != "a" })
      assert_operator runtime.metrics[:max_active_cycles], :<=, 4
      assert_reservations_released(runtime)
    end
  end
  # rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Metrics/BlockLength

  def test_saturated_group_leaves_pipeline_capacity_for_another_group
    started = Async::Queue.new
    gate = Async::Queue.new
    program = build_program(tenants: %w[bad-a bad-b good], interval: 60, fetch: lambda do |id, context|
      started.push(id)
      gate.pop
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end)
    program.upstream_group { |id| id.split("-").first }
    program.upstream_capacity 1
    runtime = isolated_runtime(program)
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(1) { assert_equal %w[bad-a good], [started.pop, started.pop] }
      assert_equal 2, runtime.metrics[:active_cycles]
      assert_equal 2, runtime.metrics[:upstream_groups]
      runner.stop
    end.wait
    assert_reservations_released(runtime)
  end

  def test_all_failed_upstreams_exhaust_a_finite_retry_budget_without_waiting_for_backoff
    failures = 0
    program = build_program(tenants: (1..20).to_a, interval: 60, fetch: lambda do |*|
      failures += 1
      raise IOError
    end, fetch_retry: { on: IOError, max_attempts: 3, backoff: 10, max_backoff: 10 })
    runtime = isolated_runtime(program)
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(1) { Kernel.sleep(0.001) until failures == 20 }
      assert_equal 2, runtime.metrics[:parked_cycles]
      assert_equal 18, runtime.metrics[:retry_overflows]
      runner.stop
    end.wait
    assert_operator runtime.metrics[:max_active_cycles], :<=, 4
    assert_reservations_released(runtime)
  end

  def test_ownership_loss_while_parked_discards_the_batch_without_resuming_ingestion
    calls = 0
    released = Async::Queue.new
    ownership = Acp::LocalOwnership.new
    ownership.define_singleton_method(:renewal_interval) { |_| 0.01 }
    ownership.define_singleton_method(:renew) { |*| false }
    ownership.define_singleton_method(:release) { |*| released.push(true) }
    program = build_program(interval: 60, ingest: lambda do |*|
      calls += 1
      raise IOError
    end, ingest_retry: { on: IOError, max_attempts: 3, backoff: 10, max_backoff: 10 })
    runtime = isolated_runtime(program, ownership: ownership)
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(1) { released.pop }
      assert_equal 1, calls
      assert_equal 1, runtime.metrics[:renewal_deadline_misses]
      runner.stop
    end.wait
    assert_reservations_released(runtime)
  end

  def test_resolution_and_initial_cursor_deadlines_include_queue_and_callback_work
    %i[resolve initial_cursor].each do |stage|
      unwound = 0
      program = build_program(tenants: %w[a b], interval: 60)
      program.public_send(:"#{stage}_timeout", 0.01)
      program.resolve_concurrency 1
      program.public_send(stage) do |_id|
        Kernel.sleep(60)
      ensure
        unwound += 1
      end
      runtime = isolated_runtime(program)
      run_for(runtime, duration: 0.04)
      assert_operator unwound, :>=, 1
      assert_operator unwound, :<=, 2
      assert_equal 0, runtime.metrics[:completed_cycles]
      assert_reservations_released(runtime)
    end
  end

  def test_lost_successful_acknowledgement_response_never_replays_confirmed_ingestion
    progress = MemoryProgress.new
    attempts = 0
    progress.define_singleton_method(:acknowledge) do |*args|
      super(*args)
      attempts += 1
      raise IOError if attempts == 1
    end
    ingestions = 0
    program = build_program(interval: 60, ingest: ->(*) { ingestions += 1 })
    program.acknowledge_retry on: IOError, max_attempts: 3
    program.retry_capacity 2
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress)
    runtime.define_singleton_method(:initial_offset) { |_| 0 }
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(1) { Kernel.sleep(0.001) until runtime.metrics[:completed_cycles] == 1 }
      runner.stop
    end.wait
    assert_equal 2, attempts
    assert_equal 1, ingestions
    assert_equal 1, progress.acknowledgements.length
    assert_reservations_released(runtime)
  end

  def test_acknowledgement_outage_stops_admission_and_drain_cancels_stalled_commands_and_cleanup
    ingestions = 0
    unwound = 0
    progress = MemoryProgress.new
    progress.define_singleton_method(:acknowledge) do |*|
      Kernel.sleep(60)
    ensure
      unwound += 1
    end
    ownership = Acp::LocalOwnership.new
    ownership.define_singleton_method(:release) { |*| Kernel.sleep(60) }
    program = build_program(tenants: (1..20).to_a, interval: 60, ingest: ->(*) { ingestions += 1 })
    program.coordination_timeout 0.01
    program.acknowledge_retry on: Timeout::Error, max_attempts: 3, backoff: 0.001, max_backoff: 0.001
    program.retry_capacity 2
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress, ownership: ownership)
    runtime.define_singleton_method(:initial_offset) { |_| 0 }
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(1) { Kernel.sleep(0.001) until unwound >= 3 }
      assert_equal 3, ingestions
      assert_equal 3, runtime.metrics[:pending_acknowledgements]
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      runtime.request_shutdown(timeout: 0.01)
      root.with_timeout(0.1) { runner.wait }
      assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 0.1
    end.wait
    assert_equal 3, runtime.metrics[:max_pending_acknowledgements]
    assert_reservations_released(runtime)
  end

  def test_retry_readmission_deadline_expires_while_pipeline_is_busy
    calls = []
    gate = Async::Queue.new
    policy = { on: IOError, max_attempts: 3, max_elapsed: 0.02, backoff: 0.001, max_backoff: 0.001 }
    program = build_program(tenants: %w[retry busy], interval: 60, fetch: lambda do |tenant, context|
      calls << [tenant, context.attempt]
      raise IOError if tenant == "retry"

      gate.pop
    end, fetch_retry: policy)
    program.pipeline_capacity 1
    program.retry_capacity 1
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)
    runtime.define_singleton_method(:initial_offset) { |_| 0 }
    run_for(runtime, duration: 0.04)
    assert_equal [["retry", 1], ["busy", 1]], calls
    assert_equal 1, runtime.metrics[:pressure][:retry_admission_wait][:outcomes][:timeout]
    assert_reservations_released(runtime)
  end

  def test_graceful_shutdown_cancels_parked_ingestion_and_eligible_retry_waits
    %i[backoff readmission].each do |wait_class|
      calls = 0
      policy = { on: IOError, max_attempts: 3, backoff: wait_class == :backoff ? 10 : 0.001, max_backoff: 10 }
      program = build_program(tenants: %w[retry busy], interval: 60, ingest: lambda do |_batch, context|
        calls += 1
        raise IOError if context.tenant_id == "retry"

        Kernel.sleep(60)
      end, ingest_retry: policy)
      program.pipeline_capacity 1
      program.retry_capacity 1
      runtime = Acp::Runtime.new(configuration: program.configuration, progress: MemoryProgress.new)
      runtime.define_singleton_method(:initial_offset) { |_| 0 }
      Async do |root|
        runner = root.async { runtime.run }
        root.with_timeout(1) { Kernel.sleep(0.001) until calls == 2 }
        Kernel.sleep(0.005) if wait_class == :readmission
        assert_equal 1, runtime.metrics[:parked_cycles]
        assert_equal 2, runtime.metrics[:retained_batches]
        runtime.request_shutdown(timeout: 0.01)
        root.with_timeout(0.1) { runner.wait }
      end.wait
      assert_equal 2, calls
      assert_reservations_released(runtime)
    end
  end

  def test_future_due_work_in_another_group_is_not_hidden_by_a_saturated_group
    started = Async::Queue.new
    program = build_program(tenants: %w[bad-a bad-b good], interval: 60, fetch: lambda do |id, context|
      if id == "good"
        started.push(true)
        Acp::Batch.new(data: [], next_cursor: context.cursor)
      else
        Kernel.sleep(60)
      end
    end)
    program.upstream_group { |id| id.split("-").first }
    program.upstream_capacity 1
    runtime = isolated_runtime(program, offset: ->(id) { id == "good" ? 0.01 : 0 })
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(0.1) { started.pop }
      runner.stop
    end.wait
    assert_reservations_released(runtime)
  end
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize, Metrics/ParameterLists
