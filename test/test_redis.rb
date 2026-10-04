# frozen_string_literal: true

require "test_helper"
require "acp/redis"

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
      @statuses.fetch(encoded_id, {})
    end
  end
end
