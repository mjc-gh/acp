# frozen_string_literal: true

require "test_helper"
require "acp/redis"

# Keep controlled pool/lease interleavings beside the protocol tests.
# rubocop:disable Metrics/ClassLength
class TestRedisCoordinator < Minitest::Test
  WORKER_DATA = {
    "worker-a" => { expires_at: 1_000_060_000, capacity: 2, active: 1 },
    "worker-b" => { expires_at: 1_000_060_000, capacity: 4, active: 3 },
    "stale-worker" => { expires_at: 999_999_999, capacity: 8, active: 0 }
  }.freeze

  def coordinator(**options)
    Acp::RedisCoordinator.new(
      application: "test-app",
      environment: "test",
      program: "sync",
      **options
    )
  end

  def test_tenant_ids_are_typed_and_collision_free
    store = coordinator

    refute_equal store.canonical_tenant_id(12), store.canonical_tenant_id("12")
    refute_equal store.canonical_tenant_id("a:b"), store.canonical_tenant_id("a/b")
    assert_raises(ArgumentError) { store.canonical_tenant_id(nil) }
    assert_raises(ArgumentError) { store.canonical_tenant_id(12.0) }
  end

  def test_namespaces_separate_app_environment_and_program
    first = Acp::RedisCoordinator.new(application: "a", environment: "prod", program: "sync")
    second = Acp::RedisCoordinator.new(application: "a", environment: "stage", program: "sync")

    refute_equal first.instance_variable_get(:@prefix), second.instance_variable_get(:@prefix)
  end

  def test_lease_configuration_is_validated
    assert_raises(Acp::ConfigurationError) { coordinator(lease_ttl: 2) }
    assert_raises(Acp::ConfigurationError) { coordinator(claim_scan_limit: 0) }
    assert_raises(Acp::ConfigurationError) { coordinator(worker_scan_limit: 0) }
  end

  def test_scripts_keep_leases_and_progress_atomic
    assert_includes Acp::RedisCoordinator::CLAIM_SCRIPT, "TIME"
    assert_includes Acp::RedisCoordinator::RENEW_SCRIPT, 'HGET", KEYS[1], "token'
    assert_includes Acp::RedisCoordinator::RELEASE_SCRIPT, 'HGET", KEYS[1], "token'
    assert_includes Acp::RedisCoordinator::ADVANCE_SCRIPT, 'HGET", KEYS[1], "poll_id'
    assert_includes Acp::RedisCoordinator::ADVANCE_SCRIPT, "tonumber(revision) ~= tonumber(ARGV[3])"
  end

  def test_worker_assignments_count_tenants_only_for_live_workers
    redis = FakeWorkerRegistry.new
    store = coordinator(connection_factory: -> { redis })
    assignments = store.worker_assignments((1..100).to_a)

    assert_equal %w[worker-a worker-b], assignments.keys.sort
    assert_tenant_coverage(assignments)
    assert_equal 2, assignments.fetch("worker-a").fetch("capacity")
    assert_equal 3, assignments.fetch("worker-b").fetch("active")
  end

  def test_weighted_assignments_are_proportional_to_capacity
    store = coordinator
    store.define_singleton_method(:live_worker_records) do
      [[Base64.urlsafe_encode64("small", padding: false), { "capacity" => 1 }],
       [Base64.urlsafe_encode64("large", padding: false), { "capacity" => 3 }]]
    end
    assignments = store.worker_assignments((1..10_000).to_a)

    assert_in_delta 2_500, assignments.fetch("small").fetch("assigned_tenants"), 150
  end

  # The simultaneous waits and observed pool bounds form one interleaving.
  # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
  def test_connection_reuse_is_bounded_and_renewals_have_independent_capacity
    clients = []
    factory = lambda do
      client = Struct.new(:closed) do
        def close
          self.closed = true
        end
      end.new(false)
      clients << client
      client
    end
    store = coordinator(connection_factory: factory, connection_pool_size: 2, renewal_pool_size: 1)
    active = 0
    maximum = 0
    renewed = false

    Async do |root|
      tasks = 6.times.map do
        root.async do
          store.send(:with_client) do
            active += 1
            maximum = [maximum, active].max
            Kernel.sleep(0.02)
            active -= 1
          end
        end
      end
      store.send(:with_client, lane: :renewal) { renewed = active == 2 }
      tasks.each(&:wait)
    end
    store.close

    assert renewed
    assert_equal 2, maximum
    assert_equal 3, clients.length
    assert(clients.all?(&:closed))
  end
  # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

  Clock = Struct.new(:now)

  # The gates fix checkout and service boundaries independently of wall-clock speed.
  # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
  def test_deadline_covers_checkout_and_discards_timed_out_command_connections
    clients = []
    factory = lambda do
      client = Struct.new(:closed) { def close = self.closed = true }.new(false)
      clients << client
      client
    end
    store = coordinator(connection_factory: factory, connection_pool_size: 1, operation_timeout: 0.005)
    gate = Async::Queue.new
    started = Async::Queue.new
    Async do |root|
      holder = root.async do
        store.send(:checkout_client, lane: :commands) do
          started.push(true)
          gate.pop
        end
      end
      started.pop
      assert_raises(Acp::StageTimeoutError) do
        store.send(:with_client) { flunk "timed out checkout ran a command" }
      end
      assert_equal 1, clients.length
      refute clients.first.closed
      gate.push(true)
      holder.wait
      assert_raises(Acp::StageTimeoutError) { store.send(:with_client) { Kernel.sleep(60) } }
      assert clients.first.closed
      store.send(:with_client) { |client| refute client.closed }
      assert_equal 2, clients.length
    end.wait
    pressure = store.metrics[:pressure]
    assert_equal 1, pressure[:commands_checkout][:outcomes][:timeout]
    assert_equal 1, pressure[:commands_service][:outcomes][:timeout]
  ensure
    store&.close
  end

  def test_checkout_contention_and_cancellation_have_separate_service_distributions
    clock = Clock.new(0.0)
    store = coordinator(clock: clock, connection_pool_size: 1,
                        connection_factory: -> { Struct.new(:closed) { def close = self.closed = true }.new })
    gate = Async::Queue.new
    started = Async::Queue.new
    Async do |root|
      first = root.async do
        store.send(:with_client) do
          started.push(true)
          gate.pop
        end
      end
      started.pop
      second = root.async { store.send(:with_client) { clock.now += 3 } }
      cancelled = root.async { store.send(:with_client) { flunk "cancelled checkout ran a command" } }
      root.yield
      clock.now = 1
      cancelled.stop
      clock.now = 2
      gate.push(true)
      [first, second].each(&:wait)
    end
    pressure = store.metrics[:pressure]
    assert_equal 3, pressure[:commands_checkout][:count]
    assert_equal 3, pressure[:commands_checkout][:sum] # first=0, second=2, cancelled=1.
    assert_equal 1, pressure[:commands_checkout][:outcomes][:cancelled]
    assert_equal 2, pressure[:commands_service][:count]
    assert_equal 5, pressure[:commands_service][:sum]
    assert_equal 0, pressure[:renewal_checkout][:count]
  ensure
    store&.close
  end

  def test_renewal_slack_includes_checkout_and_command_latency_and_only_confirmed_success_resets_deadline
    clock = Clock.new(1000.0)
    delay = 0.4
    response = 1
    client = Object.new
    client.define_singleton_method(:evalsha) do |*|
      clock.now += delay
      response
    end
    client.define_singleton_method(:close) {}
    store = coordinator(clock: clock, lease_ttl: 3, connection_factory: -> { client }, renewal_pool_size: 1)
    lease = store.acquire("a") # worker heartbeat takes .4, claim takes .4.
    assert_in_delta 1003.4, lease.timing[:deadline]
    gate = Async::Queue.new
    started = Async::Queue.new
    Async do |root|
      holder = root.async do
        store.send(:with_client, lane: :renewal) do
          started.push(true)
          gate.pop
        end
      end
      started.pop
      clock.now = 1002.4
      renewal = root.async { store.renew("a", lease) }
      root.yield
      clock.now = 1003.0
      delay = 0.2
      gate.push(true)
      holder.wait
      assert renewal.wait
    end
    pressure = store.metrics[:pressure]
    assert_in_delta 0.6, pressure[:renewal_checkout][:max]
    assert_in_delta 0.2, pressure[:lease_renewal_slack][:last]
    assert_in_delta 1006.0, lease.timing[:deadline] # command start + TTL, not reply + TTL.
    clock.now = 1006.1
    response = 0
    refute store.renew("a", lease)
    assert_in_delta(-0.3, store.metrics[:pressure][:lease_renewal_slack][:last])
    assert_in_delta 1006.0, lease.timing[:deadline]
    assert_equal 1, store.metrics[:renewal_successes]
    assert_equal 1, store.metrics[:renewal_failures]
    assert_equal 1, store.metrics[:renewal_deadline_misses]
    assert_equal 1, store.metrics[:pressure][:lease_renewal_slack][:outcomes][:error]
  ensure
    store&.close
  end

  def test_failed_and_cancelled_commands_return_pool_capacity_and_record_elapsed_service
    clock = Clock.new(0.0)
    store = coordinator(clock: clock, connection_pool_size: 1, connection_factory: -> { Object.new })
    Async do |root|
      assert_raises(IOError) do
        store.send(:with_client) do
          clock.now = 2
          raise IOError, "unavailable"
        end
      end
      started = Async::Queue.new
      cancelled = root.async do
        store.send(:with_client) do
          started.push(true)
          Kernel.sleep(60)
        end
      end
      started.pop
      clock.now = 5
      cancelled.stop
      store.send(:with_client) { clock.now = 6 }
      assert_raises(Redis::TimeoutError) do
        store.send(:with_client) do
          clock.now = 8
          raise Redis::TimeoutError, "command timed out"
        end
      end
    end
    service = store.metrics[:pressure][:commands_service]
    assert_equal 8, service[:sum]
    assert_equal({ success: 1, error: 1, timeout: 1, cancelled: 1 }, service[:outcomes])
  end

  def test_cancelled_renewal_checkout_records_partial_slack_without_moving_the_deadline
    clock = Clock.new(0.0)
    client = Object.new
    client.define_singleton_method(:evalsha) { |*| 1 }
    client.define_singleton_method(:close) {}
    store = coordinator(clock: clock, lease_ttl: 3, renewal_pool_size: 1, connection_factory: -> { client })
    lease = store.acquire("a")
    gate = Async::Queue.new
    started = Async::Queue.new
    Async do |root|
      holder = root.async do
        store.send(:with_client, lane: :renewal) do
          started.push(true)
          gate.pop
        end
      end
      started.pop
      renewal = root.async { store.renew("a", lease) }
      root.yield
      clock.now = 1
      renewal.stop
      gate.push(true)
      holder.wait
    end
    metrics = store.metrics
    assert_equal 3, lease.timing[:deadline]
    assert_equal 1, metrics[:pressure][:renewal_checkout][:outcomes][:cancelled]
    assert_equal 1, metrics[:pressure][:lease_renewal_slack][:outcomes][:cancelled]
    assert_equal 2, metrics[:pressure][:lease_renewal_slack][:last]
    assert_equal 0, metrics[:renewal_successes]
    assert_equal 0, metrics[:renewal_failures]
  ensure
    store&.close
  end
  # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

  def assert_tenant_coverage(assignments)
    assigned_count = assignments.values.map { |status| status.fetch("assigned_tenants") }.sum
    assert_equal 100, assigned_count
  end

  # These tests gate cooperative operations to exercise cancellation and revision races.
  # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/BlockLength
  def test_scripts_recover_only_noscript_and_do_not_replay_ambiguous_mutations
    calls = []
    failure = Redis::CommandError.new("NOSCRIPT No matching script")
    client = Object.new
    client.define_singleton_method(:evalsha) do |sha, **options|
      calls << [:evalsha, sha, options]
      raise failure if failure

      1
    end
    client.define_singleton_method(:eval) do |script, **options|
      calls << [:eval, script, options]
      1
    end
    store = coordinator
    script = Acp::RedisCoordinator::ADVANCE_SCRIPT
    options = { keys: %w[progress initialized lease], argv: ["id", "poll", 0, "token", "cursor"] }
    assert_equal 1, store.send(:execute_script, client, script, **options)
    assert_equal [[:evalsha, Digest::SHA1.hexdigest(script), options], [:eval, script, options]], calls
    failure = IOError.new("reply lost after execution")
    assert_raises(IOError) { store.send(:execute_script, client, script, **options) }
    assert_equal(1, calls.count { |call| call.first == :eval })
    failure = Redis::CommandError.new("ERR wrong key type")
    assert_raises(Redis::CommandError) { store.send(:execute_script, client, script, **options) }
    assert_equal(1, calls.count { |call| call.first == :eval })
  end

  def test_membership_refresh_is_single_flight_and_retries_mixed_revisions
    revision = "1"
    client = Object.new
    client.define_singleton_method(:get) { |_| revision }
    store = coordinator(connection_factory: -> { client })
    gate = Async::Queue.new
    started = Async::Queue.new
    scans = 0
    store.define_singleton_method(:scan_membership) do
      scans += 1
      value = revision
      started.push(true)
      gate.pop
      [[value, { "generation" => "0" }, 0]]
    end
    Async do |root|
      first = root.async { store.send(:membership_records) }
      started.pop
      followers = 5.times.map { root.async { store.send(:membership_records) } }
      root.yield
      revision = "2"
      gate.push(true)
      started.pop
      gate.push(true)
      snapshots = [first, *followers].map(&:wait)
      assert_equal 2, scans
      assert_equal 1, snapshots.map(&:object_id).uniq.length
      assert_equal "2", snapshots.first.revision
      assert_equal "2", snapshots.first.records.first.first
      assert_same snapshots.first, store.send(:membership_records)
    end.wait
  end

  def test_snapshot_leader_cancellation_and_failure_wake_waiters_and_allow_retry
    store = coordinator
    %i[membership assignment].each do |name|
      Async do |root|
        started = Async::Queue.new
        leader = root.async do
          store.send(:single_flight, name) do
            started.push(true)
            Kernel.sleep(60)
          end
        end
        started.pop
        follower = root.async do
          assert_raises(Acp::CancellationError) { store.send(:single_flight, name) { flunk "duplicate work" } }
        end
        cancelled_waiter = root.async { store.send(:single_flight, name) { flunk "duplicate work" } }
        root.yield
        cancelled_waiter.stop
        leader.stop
        root.with_timeout(1) { follower.wait }
        assert_equal :recovered, store.send(:single_flight, name) { :recovered }
        gate = Async::Queue.new
        failed = root.async do
          assert_raises(IOError) do
            store.send(:single_flight, name) do
              gate.pop
              raise IOError, "snapshot failed"
            end
          end
        end
        waiter = root.async { assert_raises(IOError) { store.send(:single_flight, name) { flunk } } }
        root.yield
        gate.push(true)
        root.with_timeout(1) { [failed, waiter].each(&:wait) }
      end.wait
    end
    assert_empty store.instance_variable_get(:@snapshot_flights)
  end

  def test_continuous_membership_churn_is_bounded_and_does_not_publish
    revision = 0
    client = Object.new
    client.define_singleton_method(:get) { |_| (revision += 1).to_s }
    store = coordinator(connection_factory: -> { client })
    scans = 0
    store.define_singleton_method(:scan_membership) do
      scans += 1
      []
    end
    assert_raises(Acp::ProgressConflictError) { store.send(:membership_records) }
    assert_equal 3, scans
    assert_nil store.instance_variable_get(:@membership_records)
    assert_empty store.instance_variable_get(:@snapshot_flights)
  end

  def test_simultaneous_assignment_callers_share_registry_scan_and_computation
    store = coordinator(worker_id: "worker-a")
    scans = 0
    gate = Async::Queue.new
    started = Async::Queue.new
    store.define_singleton_method(:live_worker_records) do
      scans += 1
      started.push(true)
      gate.pop
      [[Base64.urlsafe_encode64("worker-a", padding: false), { "capacity" => 1 }]]
    end
    snapshot = Acp::RedisCoordinator::Membership.new(
      records: [[store.canonical_tenant_id(1), { "generation" => "0" }, 0]], revision: "1"
    )
    store.define_singleton_method(:membership_records) { snapshot }
    Async do |root|
      leader = root.async { store.scheduling_tenants }
      started.pop
      followers = 5.times.map { root.async { store.scheduling_tenants } }
      root.yield
      gate.push(true)
      assert_equal [[1]] * 6, [leader, *followers].map(&:wait)
      assert_equal 1, scans
    end.wait
  end

  def discovery_runtime(ids, batch_size: 3, &callback)
    program = Class.new(Acp::Program) do
      program_name "BoundedDiscoveryTest"
      interval 60
      pipeline_capacity 1
      fetch_concurrency 1
      ingest_concurrency 1
      discovery_batch_size batch_size
      tenants(&callback || ->(emit) { ids.each { |id| emit.call(id) } })
      initial_cursor { |_| Time.utc(2025, 1, 1) }
      resolve { |id| id }
      fetch { |_, context| Acp::Batch.new(data: [], next_cursor: context.cursor) }
      ingest { |*| nil }
    end
    store = coordinator
    batches = []
    completed = []
    store.define_singleton_method(:acquire_discovery) { "token" }
    store.define_singleton_method(:begin_discovery) { |_| 1 }
    store.define_singleton_method(:release_discovery) { |_| nil }
    store.define_singleton_method(:complete_discovery) { |*| completed << true }
    store.define_singleton_method(:register_discovered_tenants) { |batch, *| batches << batch.dup }
    runtime = Acp::Runtime.new(configuration: program.configuration, ownership: store, progress: store)
    [runtime, batches, completed, program]
  end

  def test_discovery_flushes_bounded_chunks_and_final_partial_chunk_and_deduplicates
    [[], [1], [1, 2, 3], [1, 2, 3, 4, 4, 5, 6, 7]].each do |ids|
      runtime, batches, completed = discovery_runtime(ids)
      states = Async { runtime.send(:perform_discovery) }.wait
      assert_equal ids.uniq, states.map(&:tenant_id)
      assert_equal ids.uniq.each_slice(3).to_a, batches
      assert_equal [true], completed
    end
  end

  def test_discovery_failure_and_cancellation_never_flush_partial_buffer_or_complete
    runtime, batches, completed = discovery_runtime([]) do |emit|
      4.times { |id| emit.call(id) }
      raise IOError, "partial enumeration"
    end
    Async { assert_raises(IOError) { runtime.send(:perform_discovery) } }.wait
    assert_equal [[0, 1, 2]], batches
    assert_empty completed
    started = Async::Queue.new
    runtime, batches, completed = discovery_runtime([]) do |emit|
      4.times { |id| emit.call(id) }
      started.push(true)
      Kernel.sleep(60)
    end
    Async do |root|
      task = root.async { runtime.send(:perform_discovery) }
      started.pop
      task.stop
    end.wait
    assert_equal [[0, 1, 2]], batches
    assert_empty completed
  end

  def test_paged_discovery_materializes_lazy_pages_with_a_bounded_limit
    runtime, batches, completed, program = discovery_runtime([])
    cursors = []
    program.tenants_page do |cursor, limit|
      cursors << [cursor, limit]
      [((cursor || 0)...[(cursor || 0) + limit, 7].min).lazy, cursor == 6 ? nil : (cursor || 0) + limit]
    end
    runtime = Acp::Runtime.new(configuration: program.configuration,
                               ownership: runtime.instance_variable_get(:@ownership), progress: coordinator)
    Async { runtime.send(:perform_discovery) }.wait
    assert_equal [[nil, 3], [3, 3], [6, 3]], cursors
    assert_equal [[0, 1, 2], [3, 4, 5], [6]], batches
    assert_equal [true], completed
    assert_raises(ArgumentError) { Acp::DiscoveryPage.materialize([(1..4).lazy, nil], 3) }
  end

  def test_failed_final_chunk_cannot_complete_discovery_or_report_success
    runtime, batches, completed = discovery_runtime([1, 2, 3, 4])
    ownership = runtime.instance_variable_get(:@ownership)
    ownership.define_singleton_method(:register_discovered_tenants) do |batch, *|
      raise Acp::LeaseLostError, "expired before final registration" if batch.length == 1

      batches << batch.dup
    end
    Async { assert_raises(Acp::LeaseLostError) { runtime.send(:perform_discovery) } }.wait
    assert_equal [[1, 2, 3]], batches
    assert_empty completed
    assert_equal 0, runtime.metrics[:discovery_successes]
    assert_equal 1, runtime.metrics[:discovery_failures]
  end

  def test_discovery_batch_limit_is_validated
    runtime, _, _, program = discovery_runtime([])
    assert_equal 3, runtime.instance_variable_get(:@configuration).discovery_batch_size
    [0, 1025, 1.5, nil].each do |limit|
      program.discovery_batch_size limit
      assert_raises(Acp::ConfigurationError) { program.configuration }
    end
  end
  # rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/BlockLength

  class FakeWorkerRegistry
    def initialize
      @workers = {}
      @statuses = {}
      TestRedisCoordinator::WORKER_DATA.each do |worker_id, values|
        encoded_id = Base64.urlsafe_encode64(worker_id, padding: false)
        @workers[encoded_id] = values.fetch(:expires_at)
        @statuses[encoded_id] = values.slice(:capacity, :active).transform_keys(&:to_s).transform_values(&:to_s)
      end
    end

    def time
      [1_000_000, 0]
    end

    def zremrangebyscore(_key, _minimum, maximum)
      @workers.delete_if { |_id, expires_at| expires_at <= maximum.to_i }
    end

    def zrange(_key, start_index, stop_index)
      @workers.sort_by { |encoded_id, score| [score, encoded_id] }
              .map(&:first)
              .slice(start_index..stop_index) || []
    end

    def hgetall(key)
      encoded_id = key.split(":worker:").last
      result = @statuses.fetch(encoded_id, {})
      @pipeline_results << result if @pipeline_results
      result
    end

    def pipelined
      @pipeline_results = []
      yield self
      @pipeline_results
    ensure
      @pipeline_results = nil
    end
  end
end
# rubocop:enable Metrics/ClassLength
