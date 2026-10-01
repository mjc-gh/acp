# frozen_string_literal: true

require "test_helper"
require "acp/redis"

class TestRedisCoordinator < Minitest::Test
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
end
