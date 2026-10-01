# frozen_string_literal: true

require "test_helper"
require "acp/redis"
require "open3"
require "rbconfig"
require "redis"

# Run with ACP_TEST_REDIS_URL pointing to a disposable Redis database.
# rubocop:disable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
class RedisCoordinationIntegrationTest < Minitest::Test
  # Test wrapper that drops ownership-acquisition commands during an outage.
  class AcquireOutageClient
    def initialize(client)
      @client = client
    end

    def zscore(*)
      raise Redis::CannotConnectError, "simulated network outage"
    end

    def method_missing(name, ...)
      @client.public_send(name, ...)
    end

    def respond_to_missing?(name, include_private = false)
      @client.respond_to?(name, include_private) || super
    end

    def close
      @client.close
    end
  end

  def setup
    @url = ENV.fetch("ACP_TEST_REDIS_URL")
    @application = "acp-test-#{Process.pid}-#{SecureRandom.hex(6)}"
    @store = build_store(worker_id: "worker-a")
    @store.register_tenants(["tenant", 12])
  end

  def teardown
    prefix = @store.instance_variable_get(:@prefix)
    redis = Redis.new(url: @url)
    keys = redis.scan_each(match: "#{prefix}:*").to_a
    redis.del(keys) unless keys.empty?
    redis.close
  end

  def test_competing_workers_observe_exclusive_token_checked_leases
    other = build_store(worker_id: "worker-b")
    lease = @store.acquire("tenant")

    refute_nil lease
    assert_nil other.acquire("tenant")
    refute other.renew("tenant", Acp::RedisCoordinator::Lease.new(worker_id: "worker-b", token: "stale"))
    other.release("tenant", Acp::RedisCoordinator::Lease.new(worker_id: "worker-b", token: "stale"))
    assert @store.valid?("tenant", lease)
    assert @store.renew("tenant", lease)
  ensure
    @store.release("tenant", lease) if lease
  end

  def test_discovery_only_disables_members_after_a_completed_generation
    token = @store.acquire_discovery
    generation = @store.begin_discovery(token)
    @store.register_discovered_tenant("tenant", generation, token)
    @store.register_discovered_tenant(12, generation, token)
    @store.complete_discovery(generation, token)
    @store.release_discovery(token)

    state = @store.initialize_state("tenant", Time.utc(2025, 1, 1))
    assert_equal [12, "tenant"], @store.enabled_tenants.sort_by(&:to_s)

    next_token = @store.acquire_discovery
    next_generation = @store.begin_discovery(next_token)
    @store.register_discovered_tenant(12, next_generation, next_token)
    assert_equal [12, "tenant"], @store.enabled_tenants.sort_by(&:to_s),
                 "an interrupted scan must not mass-disable prior members"
    @store.complete_discovery(next_generation, next_token)

    assert_equal [12], @store.enabled_tenants
    refute_nil @store.read_state("tenant")
    @store.release_discovery(next_token)

    reactivation = @store.acquire_discovery
    generation = @store.begin_discovery(reactivation)
    @store.register_discovered_tenant("tenant", generation, reactivation)
    @store.register_discovered_tenant(12, generation, reactivation)
    @store.complete_discovery(generation, reactivation)
    assert_equal [12, "tenant"], @store.enabled_tenants.sort_by(&:to_s)
    assert_equal state.cursor, @store.read_state("tenant").cursor
  ensure
    @store.release_discovery(token) if token
    @store.release_discovery(next_token) if next_token
    @store.release_discovery(reactivation) if reactivation
  end

  def test_paused_tenant_retains_progress_and_cannot_be_claimed
    @store.initialize_state("tenant", Time.utc(2025, 1, 1))
    @store.pause_tenant("tenant")

    assert @store.tenant_paused?("tenant")
    assert_nil @store.acquire("tenant")
    assert_equal [12, "tenant"], @store.enabled_tenants.sort_by(&:to_s)

    @store.resume_tenant("tenant")
    refute @store.tenant_paused?("tenant")
    assert_equal Time.utc(2025, 1, 1), @store.read_state("tenant").cursor
    refute_nil @store.acquire("tenant")
  end

  # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def test_consistent_assignment_respects_worker_capacity
    low_capacity = build_store(worker_id: "low-capacity")
    high_capacity = build_store(worker_id: "high-capacity")
    low_capacity.heartbeat_worker(capacity: 1, active: 0)
    high_capacity.heartbeat_worker(capacity: 3, active: 0)
    tenants = (1..90).map { |number| "tenant-#{number}" }
    @store.register_tenants(tenants)
    assignments = Hash.new(0)

    tenants.each do |tenant_id|
      lease = low_capacity.acquire(tenant_id)
      owner = low_capacity.worker_id if lease
      lease ||= high_capacity.acquire(tenant_id)
      owner ||= high_capacity.worker_id if lease
      refute_nil lease
      assignments[owner] += 1
      low_capacity.release(tenant_id, lease) if owner == low_capacity.worker_id
      high_capacity.release(tenant_id, lease) if owner == high_capacity.worker_id
    end

    assert_operator assignments.fetch(high_capacity.worker_id), :>, assignments.fetch(low_capacity.worker_id)
    assert_equal 1, low_capacity.worker_statuses.fetch(low_capacity.worker_id).fetch("capacity")
    assert_equal 3, low_capacity.worker_statuses.fetch(high_capacity.worker_id).fetch("capacity")
  end
  # rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

  def test_initialization_and_unchanged_advancement_are_revisioned_and_idempotent
    lease = @store.acquire("tenant")
    start = Time.utc(2025, 1, 1)
    state = @store.initialize_state("tenant", start)

    assert_equal 0, state.revision
    assert @store.advance("tenant", "poll-1", start, expected_revision: state.revision, lease: lease)
    assert @store.advance("tenant", "poll-1", start, expected_revision: state.revision, lease: lease)
    updated = @store.read_state("tenant")
    assert_equal start, updated.cursor
    assert_equal 1, updated.revision
    assert_raises(Acp::ProgressConflictError) do
      @store.advance("tenant", "poll-2", start, expected_revision: 0, lease: lease)
    end
    assert_raises(Acp::CursorRegressionError) do
      @store.advance("tenant", "poll-2", start - 1, expected_revision: 1, lease: lease)
    end
    progress_key = "#{@store.instance_variable_get(:@prefix)}:progress:#{@store.canonical_tenant_id("tenant")}"
    redis = Redis.new(url: @url)
    assert_equal(-1, redis.pttl(progress_key))
    redis.del(progress_key)
    assert_raises(Acp::MissingProgressError) { @store.read_state("tenant") }
    redis.close
  ensure
    @store.release("tenant", lease) if lease
  end

  # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def test_progress_survives_process_termination_and_expired_lease_recovery
    start = Time.utc(2025, 1, 1)
    first = build_store(worker_id: "child", lease_ttl: 3)
    first.register_tenants(["tenant"])
    lease = first.acquire("tenant")
    state = first.initialize_state("tenant", start)
    first.advance("tenant", "committed", start + 60, expected_revision: state.revision, lease: lease)
    first.release("tenant", lease)

    child = <<~RUBY
      require "acp/redis"
      store = Acp::RedisCoordinator.new(
        application: #{@application.inspect}, environment: "test", program: "sync",
        redis_url: ENV.fetch("ACP_TEST_REDIS_URL"), worker_id: "child", lease_ttl: 3
      )
      store.register_tenants(["tenant"])
      abort "lease unavailable" unless store.acquire("tenant")
      puts "CLAIMED"
      STDOUT.flush
      sleep 60
    RUBY
    stdin, stdout, stderr, wait_thread = Open3.popen3(
      { "ACP_TEST_REDIS_URL" => @url }, "bundle", "exec", "ruby", "-Ilib", "-e", child
    )
    assert_equal "CLAIMED", stdout.gets&.strip, stderr.read_nonblock(4096, exception: false).to_s
    Process.kill("TERM", wait_thread.pid)
    wait_thread.value
    sleep 3.1

    recovered = build_store(worker_id: "recovered", lease_ttl: 3)
    recovered.register_tenants(["tenant"])
    recovered_lease = recovered.acquire("tenant")
    refute_nil recovered_lease
    assert_equal start + 60, recovered.read_state("tenant").cursor
  ensure
    stdin&.close
    stdout&.close
    stderr&.close
    Process.kill("TERM", wait_thread.pid) if wait_thread&.alive?
    recovered&.release("tenant", recovered_lease) if recovered_lease
  end
  # rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

  def test_runtime_renews_a_lease_during_a_long_fetch
    competing = build_store(worker_id: "competitor", lease_ttl: 3)
    fetch_started = false
    fetch_finished = false
    program = Class.new(Acp::Program) do
      define_singleton_method(:name) { "RedisLeaseRuntimeTest" }
      interval 60
      fetch_concurrency 1
      ingest_concurrency 1
      pipeline_capacity 1
      tenants { |emit| emit.call("tenant") }
      initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
      resolve { |tenant_id| tenant_id }
      fetch do |_tenant, context|
        fetch_started = true
        Kernel.sleep(4)
        fetch_finished = true
        Acp::Batch.new(data: [], next_cursor: context.cursor)
      end
      ingest { |_batch, _context| }
    end
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: @store, ownership: @store)

    Async do |root|
      task = root.async { runtime.run }
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
      lease_observed = nil
      until lease_observed || fetch_finished || Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        lease_observed = competing.acquire("tenant") if fetch_started
        Kernel.sleep(0.1) unless lease_observed
      end
      task.stop
      assert_nil lease_observed
    end
  end

  def test_redis_outage_does_not_accumulate_cycles_or_payloads
    outage = build_store(
      worker_id: "outage-worker",
      connection_factory: -> { AcquireOutageClient.new(Redis.new(url: @url)) }
    )
    errors = []
    fetches = 0
    program = Class.new(Acp::Program) do
      define_singleton_method(:name) { "RedisUnavailableRuntimeTest" }
      interval 0.01
      fetch_concurrency 1
      ingest_concurrency 1
      pipeline_capacity 1
      tenants { |emit| emit.call("tenant") }
      initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
      resolve { |tenant_id| tenant_id }
      fetch do |_tenant, context|
        fetches += 1
        Acp::Batch.new(data: [], next_cursor: context.cursor)
      end
      ingest { |_batch, _context| }
    end
    runtime = Acp::Runtime.new(
      configuration: program.configuration,
      progress: outage,
      ownership: outage,
      on_error: ->(tenant_id, error, stage) { errors << [tenant_id, error, stage] }
    )

    Async do |root|
      task = root.async { runtime.run }
      Kernel.sleep(0.15)
      task.stop
    end

    refute_empty errors
    assert_equal 0, fetches
    assert_operator runtime.metrics[:max_active_cycles], :<=, 1
    assert_equal 0, runtime.metrics[:queued_batches]
    assert_equal 0, runtime.metrics[:active_cycles]
  end

  private

  def build_store(worker_id:, lease_ttl: 6, connection_factory: nil)
    Acp::RedisCoordinator.new(
      application: @application,
      environment: "test",
      program: "sync",
      redis_url: @url,
      worker_id: worker_id,
      lease_ttl: lease_ttl,
      connection_factory: connection_factory
    )
  end
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
