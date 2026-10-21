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
    client.define_singleton_method(:eval) do |*|
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
    client.define_singleton_method(:eval) { |*| 1 }
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
