# frozen_string_literal: true

require "test_helper"
require "async"

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
        Kernel.sleep(0.01)
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
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize, Metrics/ParameterLists
