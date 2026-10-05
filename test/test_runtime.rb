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
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize, Metrics/ParameterLists
