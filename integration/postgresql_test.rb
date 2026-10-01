# frozen_string_literal: true

require "test_helper"
ENV["RAILS_ENV"] ||= "test"
require "rails"
require "active_record"
require_relative "../test/rails_app/config/application"
require_relative "../examples/versioned_importer"
require "acp/redis"

AcpRailsTestApp::Application.initialize!
require "acp/rails"

# Consumer-owned destination model used by the PostgreSQL test application.
class AcpPostgresEvent < ActiveRecord::Base
  self.table_name = "acp_postgres_events"
end

# Second consumer-owned table used to force a real opposing-row deadlock.
class AcpPostgresLockRow < ActiveRecord::Base
  self.table_name = "acp_postgres_lock_rows"
end

# Model for testing the example's version-aware consumer destination.
class AcpPostgresVersionedRecord < ActiveRecord::Base
  self.table_name = "acp_postgres_versioned_records"
end

# Fiber-local application state that Rails' executor must reset after callbacks.
class AcpPostgresCurrent < ActiveSupport::CurrentAttributes
  attribute :tenant_id
end

# Run against the PostgreSQL service configured by test:postgres. These tables
# belong to the test consumer; Acp itself creates no schema.
# rubocop:disable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
class TestAcpPostgresIngestion < Minitest::Test
  # Redis client wrapper that drops one successful advancement response.
  class LostAdvanceReplyClient
    def initialize(client, fault)
      @client = client
      @fault = fault
    end

    def eval(script, **options)
      if script == Acp::RedisCoordinator::ADVANCE_SCRIPT && @fault[:before]
        @fault[:before] = false
        @fault[:occurred] = true
        raise IOError, "simulated Redis outage before progress advancement"
      end
      result = @client.eval(script, **options)
      if script == Acp::RedisCoordinator::ADVANCE_SCRIPT && @fault[:pending]
        @fault[:pending] = false
        @fault[:occurred] = true
        raise IOError, "simulated lost Redis response after committed advancement"
      end
      result
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

  # Small progress adapter keeps integration assertions on real SQL destinations.
  class MemoryProgress
    attr_reader :acknowledgements

    def initialize
      @acknowledgements = []
    end

    def read(_tenant_id)
      @initialize_cursor
    end

    def initialize_cursor(_tenant_id, value)
      @initialize_cursor ||= value
    end

    def acknowledge(_tenant_id, poll_id, value)
      @acknowledgements << [poll_id, value]
      @initialize_cursor = value
    end
  end

  def setup
    connection = ActiveRecord::Base.connection
    connection.drop_table(:acp_postgres_events, if_exists: true)
    connection.create_table(:acp_postgres_events) do |table|
      table.string :event_key, null: false
      table.timestamps
    end
    connection.add_index(:acp_postgres_events, :event_key, unique: true)
    connection.create_table(:acp_postgres_lock_rows) do |table|
      table.string :lock_key, null: false
      table.integer :touches, null: false, default: 0
    end
    connection.create_table(:acp_postgres_versioned_records) do |table|
      table.string :external_id, null: false
      table.datetime :source_updated_at, null: false, precision: 6
      table.string :value, null: false
    end
    connection.add_index(
      :acp_postgres_versioned_records,
      :external_id,
      unique: true,
      name: "index_acp_versioned_records_on_external_id"
    )
    connection.add_index(:acp_postgres_lock_rows, :lock_key, unique: true)
    %w[a b].each { |key| AcpPostgresLockRow.create!(lock_key: key) }
  end

  def teardown
    ActiveRecord::Base.connection.drop_table(:acp_postgres_events, if_exists: true)
    ActiveRecord::Base.connection.drop_table(:acp_postgres_lock_rows, if_exists: true)
    ActiveRecord::Base.connection.drop_table(:acp_postgres_versioned_records, if_exists: true)
  end

  def build_program(tenants:, ingest:, interval: 1)
    tenant_ids = tenants
    ingest_callback = ingest
    Class.new(Acp::Program) do
      define_singleton_method(:name) { "PostgresIngestionTestProgram" }
      self.interval interval
      fetch_concurrency 2
      ingest_concurrency 2
      pipeline_capacity 2
      tenants { |emit| tenant_ids.each { |tenant_id| emit.call(tenant_id) } }
      initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
      resolve { |tenant_id| tenant_id }
      fetch { |_tenant, context| Acp::Batch.new(data: [], next_cursor: context.cursor) }
      self.ingest(&ingest_callback)
    end
  end

  def configuration(program)
    Acp::Rails.configuration_for(program, transaction_owner: ActiveRecord::Base)
  end

  def run_until(runtime, timeout: 5)
    Async do |root|
      task = root.async { runtime.run }
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      begin
        until yield
          break if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

          Kernel.sleep(0.005)
        end
      ensure
        task.stop
      end
    end
  end

  def test_concurrent_batches_use_independent_connections_and_commit_rows
    backend_pids = []
    started_ingestions = 0
    program = build_program(tenants: %w[one two], ingest: lambda do |_batch, context|
      backend_pids << ActiveRecord::Base.connection.select_value("SELECT pg_backend_pid()").to_i
      started_ingestions += 1
      Kernel.sleep(0.005) while started_ingestions < 2
      AcpPostgresEvent.create!(event_key: context.tenant_id)
    end)
    progress = MemoryProgress.new
    runtime = Acp::Runtime.new(configuration: configuration(program), progress: progress)

    run_until(runtime) { progress.acknowledgements.length >= 2 }

    assert_equal 2, AcpPostgresEvent.count
    assert_equal 2, backend_pids.uniq.length
    assert_equal 2, progress.acknowledgements.length
    assert_operator ActiveRecord::Base.connection_pool.stat[:busy], :<=, 1
  end

  def test_executor_clears_fiber_local_current_attributes_between_callbacks
    program = build_program(tenants: ["fiber"], ingest: ->(_tenant_id, _context) {})
    program.resolve do |tenant_id|
      AcpPostgresCurrent.tenant_id = tenant_id
      tenant_id
    end
    integrated = configuration(program)

    integrated.callback(:resolve).call("fiber-tenant")

    assert_nil AcpPostgresCurrent.tenant_id
  end

  def test_rollback_never_acknowledges_or_leaves_partial_rows
    ingestions = 0
    program = build_program(tenants: ["rollback"], interval: 0.2, ingest: lambda do |_tenant_id, _context|
      ingestions += 1
      AcpPostgresEvent.create!(event_key: "rolled-back")
      raise ActiveRecord::Rollback
    end)
    progress = MemoryProgress.new
    runtime = Acp::Runtime.new(configuration: configuration(program), progress: progress)

    run_until(runtime, timeout: 1) { ingestions.positive? }

    assert_operator ingestions, :>=, 1
    assert_empty progress.acknowledgements
    assert_equal 0, AcpPostgresEvent.count
  end

  def test_cancellation_rolls_back_and_releases_the_ingestion_connection
    started = []
    program = build_program(tenants: ["cancel"], ingest: lambda do |tenant_id, _context|
      AcpPostgresEvent.create!(event_key: tenant_id)
      started << true
      Kernel.sleep(60)
    end)
    progress = MemoryProgress.new
    runtime = Acp::Runtime.new(configuration: configuration(program), progress: progress)
    busy_before = ActiveRecord::Base.connection_pool.stat[:busy]

    run_until(runtime, timeout: 3) { !started.empty? }

    assert_equal 0, AcpPostgresEvent.count
    assert_empty progress.acknowledgements
    assert_operator ActiveRecord::Base.connection_pool.stat[:busy], :<=, busy_before
  end

  def test_database_retry_reuses_batch_without_refetching
    fetches = 0
    ingestions = []
    program = build_program(tenants: ["retry"], ingest: lambda do |batch, context|
      ingestions << [batch.object_id, context.poll_id, context.attempt]
      raise ActiveRecord::Deadlocked, "simulated deadlock" if ingestions.length == 1

      AcpPostgresEvent.create!(event_key: context.tenant_id)
    end)
    program.fetch do |_tenant, context|
      fetches += 1
      Acp::Batch.new(data: [Object.new], next_cursor: context.cursor)
    end
    progress = MemoryProgress.new
    runtime = Acp::Runtime.new(configuration: configuration(program), progress: progress)

    run_until(runtime) { !progress.acknowledgements.empty? }

    assert_equal 1, fetches
    assert_equal [1, 2], ingestions.map(&:last)
    assert_equal ingestions.first.first(2), ingestions.last.first(2)
    assert_equal 1, AcpPostgresEvent.count
    assert_equal 1, progress.acknowledgements.length
  end

  def test_real_postgresql_deadlock_retries_the_same_batch
    attempts = Hash.new(0)
    first_locks = 0
    program = build_program(tenants: %w[a b], ingest: lambda do |_batch, context|
      tenant_id = context.tenant_id
      attempts[tenant_id] += 1
      AcpPostgresLockRow.where(lock_key: tenant_id).update_all("touches = touches + 1")
      if context.attempt == 1
        first_locks += 1
        Kernel.sleep(0.005) while first_locks < 2
      end
      other_id = tenant_id == "a" ? "b" : "a"
      AcpPostgresLockRow.where(lock_key: other_id).update_all("touches = touches + 1")
    end)
    progress = MemoryProgress.new
    runtime = Acp::Runtime.new(configuration: configuration(program), progress: progress)

    run_until(runtime) { progress.acknowledgements.length >= 2 }

    assert_equal [1, 2], attempts.values.sort
    assert_equal 2, progress.acknowledgements.length
  end

  def test_pg_io_wait_does_not_block_async_timers
    ticks = 0

    Async do |root|
      query = root.async do
        ActiveRecord::Base.connection_pool.with_connection(prevent_permanent_checkout: true) do |connection|
          connection.select_value("SELECT 1 FROM pg_sleep(0.15)")
        end
      end
      timer = root.async do
        until query.finished?
          ticks += 1
          Kernel.sleep(0.005)
        end
      end
      query.wait
      timer.stop
    end

    assert_operator ticks, :>=, 3
  end

  def test_consumer_unique_constraints_are_enforced_inside_the_batch_transaction
    AcpPostgresEvent.create!(event_key: "duplicate")
    attempts = 0
    errors = []
    program = build_program(tenants: ["duplicate"], ingest: lambda do |_batch, context|
      attempts += 1
      AcpPostgresEvent.create!(event_key: context.tenant_id)
    end)
    progress = MemoryProgress.new
    runtime = Acp::Runtime.new(
      configuration: configuration(program),
      progress: progress,
      on_error: ->(_tenant_id, error, _stage) { errors << error }
    )

    run_until(runtime, timeout: 1) { !errors.empty? }

    assert_operator attempts, :>=, 1
    assert_equal 1, AcpPostgresEvent.count
    assert_empty progress.acknowledgements
  end

  def test_versioned_example_importer_deduplicates_and_ignores_stale_replays
    current = Time.utc(2025, 1, 2)
    old = Time.utc(2025, 1, 1)
    VersionedImporter.call(
      model: AcpPostgresVersionedRecord,
      records: [{ external_id: "event-1", source_updated_at: current, value: "new" }]
    )
    VersionedImporter.call(
      model: AcpPostgresVersionedRecord,
      records: [
        { external_id: "event-1", source_updated_at: old, value: "stale" },
        { external_id: "event-1", source_updated_at: current, value: "duplicate" }
      ]
    )

    record = AcpPostgresVersionedRecord.find_by!(external_id: "event-1")
    assert_equal 1, AcpPostgresVersionedRecord.count
    assert_equal current, record.source_updated_at
    assert_equal "new", record.value
  end

  def test_expired_owner_can_commit_but_cannot_overwrite_newer_data_or_redis_progress
    url = ENV.fetch("ACP_TEST_REDIS_URL")
    application = "acp-stale-db-#{Process.pid}-#{SecureRandom.hex(6)}"
    options = {
      application: application,
      environment: "test",
      program: "stale-database-effect",
      redis_url: url,
      worker_id: "same-logical-worker",
      lease_ttl: 3
    }
    coordinator = Acp::RedisCoordinator.new(**options)
    replacement = Acp::RedisCoordinator.new(**options)
    competing_errors = []
    competing_thread = nil
    # rubocop:disable Metrics/BlockLength
    program = build_program(tenants: ["lease-race"], ingest: lambda do |_batch, _context|
      competing_thread = Thread.new do
        Kernel.sleep(3.2)
        replacement.heartbeat_worker(capacity: 1, active: 0)
        lease = replacement.acquire("lease-race")
        raise "replacement did not acquire expired ownership" unless lease

        state = replacement.read_state("lease-race")
        VersionedImporter.call(
          model: AcpPostgresVersionedRecord,
          records: [
            { external_id: "lease-race-event", source_updated_at: Time.utc(2025, 1, 3), value: "new" }
          ]
        )
        replacement.advance(
          "lease-race", "replacement-poll", Time.utc(2025, 1, 3),
          expected_revision: state.revision, lease: lease
        )
      rescue StandardError => e
        competing_errors << e
      end
      blocking_deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 4
      Thread.pass while Process.clock_gettime(Process::CLOCK_MONOTONIC) < blocking_deadline
      VersionedImporter.call(
        model: AcpPostgresVersionedRecord,
        records: [
          { external_id: "lease-race-event", source_updated_at: Time.utc(2025, 1, 2), value: "stale" }
        ]
      )
    end)
    # rubocop:enable Metrics/BlockLength
    runtime = Acp::Runtime.new(configuration: configuration(program), progress: coordinator, ownership: coordinator)

    run_until(runtime, timeout: 8) { competing_thread&.join(0) }

    assert_empty competing_errors
    assert_equal "new", AcpPostgresVersionedRecord.find_by!(external_id: "lease-race-event").value
    assert_equal Time.utc(2025, 1, 3), coordinator.read_state("lease-race").cursor
    assert_equal 1, coordinator.read_state("lease-race").revision
  ensure
    competing_thread&.join
    if coordinator
      prefix = coordinator.instance_variable_get(:@prefix)
      redis = Redis.new(url: url)
      keys = redis.scan_each(match: "#{prefix}:*").to_a
      redis.del(keys) unless keys.empty?
      redis.close
    end
  end

  def test_confirmed_postgres_commit_reconciles_a_lost_redis_advance_response
    url = ENV.fetch("ACP_TEST_REDIS_URL")
    application = "acp-postgres-redis-#{Process.pid}-#{SecureRandom.hex(6)}"
    fault = { pending: true, occurred: false }
    coordinator = Acp::RedisCoordinator.new(
      application: application,
      environment: "test",
      program: "commit-boundary",
      redis_url: url,
      connection_factory: -> { LostAdvanceReplyClient.new(Redis.new(url: url), fault) },
      worker_id: "postgres-test-worker",
      lease_ttl: 6
    )
    program = build_program(
      tenants: ["redis-commit"],
      ingest: ->(_batch, context) { AcpPostgresEvent.create!(event_key: context.tenant_id) }
    )
    runtime = Acp::Runtime.new(configuration: configuration(program), progress: coordinator, ownership: coordinator)

    run_until(runtime, timeout: 5) do
      fault[:occurred] && coordinator.read_state("redis-commit")&.revision == 1
    end

    assert_equal 1, AcpPostgresEvent.where(event_key: "redis-commit").count
    assert_equal 1, coordinator.read_state("redis-commit").revision
    assert fault[:occurred]
  ensure
    if coordinator
      prefix = coordinator.instance_variable_get(:@prefix)
      redis = Redis.new(url: url)
      keys = redis.scan_each(match: "#{prefix}:*").to_a
      redis.del(keys) unless keys.empty?
      redis.close
    end
  end

  def test_commit_before_progress_failure_replays_safely_on_runtime_restart
    url = ENV.fetch("ACP_TEST_REDIS_URL")
    application = "acp-postgres-replay-#{Process.pid}-#{SecureRandom.hex(6)}"
    fault = { before: true, occurred: false }
    coordinator = Acp::RedisCoordinator.new(
      application: application,
      environment: "test",
      program: "commit-replay",
      redis_url: url,
      connection_factory: -> { LostAdvanceReplyClient.new(Redis.new(url: url), fault) },
      worker_id: "postgres-replay-worker",
      lease_ttl: 6
    )
    ingestions = 0
    program = build_program(
      tenants: ["redis-replay"],
      ingest: lambda do |_batch, context|
        ingestions += 1
        AcpPostgresEvent.find_or_create_by!(event_key: context.tenant_id)
      end
    )
    first_runtime = Acp::Runtime.new(
      configuration: configuration(program),
      progress: coordinator,
      ownership: coordinator
    )

    run_until(first_runtime, timeout: 5) { fault[:occurred] }
    assert_equal 1, AcpPostgresEvent.where(event_key: "redis-replay").count
    assert_equal 0, coordinator.read_state("redis-replay").revision

    fault[:before] = false
    restarted_runtime = Acp::Runtime.new(
      configuration: configuration(program),
      progress: coordinator,
      ownership: coordinator
    )
    run_until(restarted_runtime, timeout: 5) do
      coordinator.read_state("redis-replay")&.revision == 1
    end

    assert_equal 2, ingestions
    assert_equal 1, AcpPostgresEvent.where(event_key: "redis-replay").count
    assert_equal Time.utc(2025, 1, 1), coordinator.read_state("redis-replay").cursor
  ensure
    if coordinator
      prefix = coordinator.instance_variable_get(:@prefix)
      redis = Redis.new(url: url)
      keys = redis.scan_each(match: "#{prefix}:*").to_a
      redis.del(keys) unless keys.empty?
      redis.close
    end
  end
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
