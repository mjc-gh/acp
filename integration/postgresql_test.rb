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

# Independent exact-bound pool; the test driver never leases from this pool.
class AcpPostgresBudgetRecord < ActiveRecord::Base
  self.abstract_class = true
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
      execute(script == Acp::RedisCoordinator::ADVANCE_SCRIPT) { @client.eval(script, **options) }
    end

    def evalsha(sha, **options)
      advancement = sha == Acp::RedisCoordinator::SCRIPT_SHAS.fetch(Acp::RedisCoordinator::ADVANCE_SCRIPT)
      execute(advancement) { @client.evalsha(sha, **options) }
    end

    def execute(advancement)
      if advancement && @fault[:outage]
        @fault[:attempts] = @fault.fetch(:attempts, 0) + 1
        raise IOError, "simulated prolonged Redis acknowledgement outage"
      end
      if advancement && @fault[:before]
        @fault[:before] = false
        @fault[:occurred] = true
        raise IOError, "simulated Redis outage before progress advancement"
      end
      result = yield
      if advancement && @fault[:pending]
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
    ingestions = 0
    program = build_program(
      tenants: ["redis-commit"],
      ingest: lambda do |_batch, context|
        ingestions += 1
        AcpPostgresEvent.create!(event_key: context.tenant_id)
      end
    )
    program.retry_capacity 2
    program.acknowledge_retry on: IOError, max_attempts: 3, backoff: 0.01, max_backoff: 0.01
    runtime = Acp::Runtime.new(configuration: configuration(program), progress: coordinator, ownership: coordinator)

    run_until(runtime, timeout: 5) do
      fault[:occurred] && runtime.metrics[:completed_cycles] == 1
    end

    assert_equal 1, AcpPostgresEvent.where(event_key: "redis-commit").count
    assert_equal 1, coordinator.read_state("redis-commit").revision
    assert fault[:occurred]
    assert_equal 1, ingestions
    assert_equal 1, runtime.metrics[:completed_cycles]
  ensure
    if coordinator
      prefix = coordinator.instance_variable_get(:@prefix)
      redis = Redis.new(url: url)
      keys = redis.scan_each(match: "#{prefix}:*").to_a
      redis.del(keys) unless keys.empty?
      redis.close
    end
  end

  def test_prolonged_acknowledgement_outage_bounds_committed_batches_and_recovers_without_reingestion
    url = ENV.fetch("ACP_TEST_REDIS_URL")
    fault = { outage: true }
    coordinator = Acp::RedisCoordinator.new(
      application: "acp-bounded-ack-#{Process.pid}-#{SecureRandom.hex(6)}",
      environment: "test", program: "bounded-ack", redis_url: url,
      connection_factory: -> { LostAdvanceReplyClient.new(Redis.new(url: url), fault) }
    )
    ingestions = []
    program = build_program(tenants: (1..8).to_a, interval: 60, ingest: lambda do |_batch, context|
      ingestions << [context.tenant_id, context.poll_id]
      AcpPostgresEvent.create!(event_key: context.tenant_id.to_s)
    end)
    program.retry_capacity 2
    program.coordination_timeout 0.2
    program.acknowledge_retry on: IOError, max_attempts: 100, backoff: 0.005, max_backoff: 0.005
    runtime = Acp::Runtime.new(configuration: configuration(program), progress: coordinator, ownership: coordinator)
    runtime.define_singleton_method(:initial_offset) { |_| 0 }
    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(2) { Kernel.sleep(0.005) until fault.fetch(:attempts, 0) >= 6 }
      assert_equal 2, AcpPostgresEvent.count
      assert_equal 2, ingestions.length
      assert_equal 2, runtime.metrics[:pending_acknowledgements]
      assert_equal 2, runtime.metrics[:retained_batches]
      assert_equal 0, runtime.metrics[:completed_cycles]
      fault[:outage] = false
      root.with_timeout(2) { Kernel.sleep(0.005) until runtime.metrics[:completed_cycles] == 8 }
      runtime.request_shutdown(timeout: 1)
      runner.wait
    end.wait
    assert_equal 8, ingestions.length
    assert_equal 8, AcpPostgresEvent.count
    assert_equal 2, runtime.metrics[:max_pending_acknowledgements]
    assert_equal 0, runtime.metrics[:retained_batches]
    assert_equal 0, runtime.metrics[:active_cycles]
  ensure
    if coordinator
      coordinator.close
      redis = Redis.new(url: url)
      keys = redis.scan_each(match: "#{coordinator.instance_variable_get(:@prefix)}:*").to_a
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

  def test_lazy_discovery_pages_release_selected_pool_before_redis_registration
    url = ENV.fetch("ACP_TEST_REDIS_URL")
    coordinator = Acp::RedisCoordinator.new(
      application: "acp-page-#{Process.pid}-#{SecureRandom.hex(6)}", environment: "test",
      program: "page-checkout", redis_url: url
    )
    pool = ActiveRecord::Base.connection_pool
    page_checkouts = []
    program = build_program(tenants: [], ingest: ->(*) {})
    program.discovery_batch_size 1
    program.tenants_page do |cursor, limit|
      ids = Enumerator.new do |output|
        page_checkouts << !pool.active_connection?.nil?
        output << AcpPostgresLockRow.where(lock_key: cursor || "a").pick(:lock_key)
      end
      assert_equal 1, limit
      [ids, cursor.nil? ? "b" : nil]
    end
    integrated = configuration(program)
    runtime = Acp::Runtime.new(configuration: integrated, ownership: coordinator, progress: coordinator)
    gate = Async::Queue.new
    started = Async::Queue.new
    original = coordinator.method(:register_discovered_tenants)
    checkouts_at_registration = []
    coordinator.define_singleton_method(:register_discovered_tenants) do |*arguments|
      checkouts_at_registration << !pool.active_connection?.nil?
      started.push(true)
      gate.pop
      original.call(*arguments)
    end
    Async do |root|
      discovery = root.async { runtime.send(:perform_discovery) }
      2.times do
        root.with_timeout(2) { started.pop }
        refute checkouts_at_registration.last
        # The discovery fiber must hold no selected-pool connection while gated.
        pool.with_connection(prevent_permanent_checkout: true) { |connection| assert connection.active? }
        gate.push(true)
      end
      assert_equal %w[a b], discovery.wait.map(&:tenant_id)
    end.wait
    assert_equal [true, true], page_checkouts
    assert_equal [false, false], checkouts_at_registration
  ensure
    if coordinator
      coordinator.close
      redis = Redis.new(url: url)
      keys = redis.scan_each(match: "#{coordinator.instance_variable_get(:@prefix)}:*").to_a
      redis.del(keys) unless keys.empty?
      redis.close
    end
  end

  def with_budget_pool(size)
    # setup uses Base.connection; explicitly return that unrelated checkout too.
    ActiveRecord::Base.connection_pool.release_connection
    AcpPostgresBudgetRecord.establish_connection(
      ActiveRecord::Base.connection_db_config.configuration_hash.merge(pool: size, checkout_timeout: 0.2)
    )
    pool = AcpPostgresBudgetRecord.connection_pool
    yield pool
    assert_equal 0, pool.stat[:busy]
    assert_equal 0, pool.stat[:waiting]
  ensure
    AcpPostgresBudgetRecord.remove_connection
  end

  # Keep gates and observations together so the simultaneous-checkout proof is visible.
  # rubocop:disable Metrics/BlockLength, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def test_exact_bound_pool_overlaps_resolution_ingestion_and_periodic_discovery
    [[8, 8, 8, 4, 4], [100, 5, 7, 5, 7]].each do |pipeline, resolve, ingest, resolving, ingesting|
      required = [pipeline, resolve + ingest].min + 1
      with_budget_pool(required) do |pool|
        coordinator = Acp::RedisCoordinator.new(
          application: "acp-budget-#{Process.pid}-#{SecureRandom.hex(6)}", environment: "test",
          program: "overlap", redis_url: ENV.fetch("ACP_TEST_REDIS_URL")
        )
        state = { resolve: 0, ingest: 0, discoveries: 0, maximum: 0, released: false, overlap: false }
        observe = lambda do
          AcpPostgresBudgetRecord.connection.select_value("SELECT 1")
          state[:maximum] = [state[:maximum], pool.stat[:busy]].max
        end
        ids = Array.new(ingesting) { |i| "ingest-#{i}" } + Array.new(resolving) { |i| "resolve-#{i}" }
        program = build_program(tenants: [], interval: 60, ingest: lambda do |_batch, _context|
          state[:ingest] += 1
          observe.call
          Kernel.sleep(0.001) until state[:released]
        end)
        program.pipeline_capacity pipeline
        program.resolve_concurrency resolve
        program.ingest_concurrency ingest
        program.discovery_interval 0.01
        program.tenants_page do |*|
          state[:discoveries] += 1
          if state[:discoveries] > 1 && !state[:released]
            Kernel.sleep(0.001) until state[:ingest] == ingesting
            if state[:discoveries] > 2
              Kernel.sleep(0.001) until state[:resolve] == resolving
              observe.call
              state[:overlap] = true
              Kernel.sleep(0.001) until state[:released]
            end
          end
          [state[:discoveries] == 1 ? ids.first(ingesting) : ids, nil]
        end
        program.initial_cursor do |_id|
          observe.call
          Time.utc(2025, 1, 1)
        end
        program.resolve do |id|
          if id.start_with?("resolve")
            state[:resolve] += 1
            observe.call
            Kernel.sleep(0.001) until state[:released]
          end
          id
        end
        errors = []
        integrated = Acp::Rails.configuration_for(program, transaction_owner: pool)
        runtime = Acp::Runtime.new(configuration: integrated, progress: coordinator, ownership: coordinator,
                                   on_error: ->(_id, error, _stage) { errors << error })
        runtime.define_singleton_method(:initial_offset) { |_| 0 }
        Async do |root|
          runner = root.async { runtime.run }
          begin
            begin
              root.with_timeout(3) { Kernel.sleep(0.001) until state[:overlap] }
            rescue Async::TimeoutError
              flunk "overlap stalled: #{state.inspect}; errors=#{errors.inspect}"
            end
            assert_equal required, state[:maximum]
            assert_equal required, pool.stat[:busy]
            assert_nil pool.active_connection?
            state[:released] = true
            root.with_timeout(3) { Kernel.sleep(0.001) until runtime.metrics[:completed_cycles] == ids.length }
            runtime.request_shutdown(timeout: 1)
            runner.wait
          ensure
            runner.stop
          end
        end.wait
        assert_empty errors
        assert_operator runtime.metrics[:max_active_resolves], :<=, resolve
      ensure
        if coordinator
          coordinator.close
          redis = Redis.new(url: ENV.fetch("ACP_TEST_REDIS_URL"))
          keys = redis.scan_each(match: "#{coordinator.instance_variable_get(:@prefix)}:*").to_a
          redis.del(keys) unless keys.empty?
          redis.close
        end
      end
    end
  end

  def test_exact_bound_pool_releases_connections_during_network_and_progress_waits
    with_budget_pool(2) do |pool|
      gates = { fetch: Async::Queue.new, acknowledge: Async::Queue.new }
      started = Async::Queue.new
      progress = MemoryProgress.new
      original_acknowledge = progress.method(:acknowledge)
      progress.define_singleton_method(:acknowledge) do |*arguments|
        started.push(:acknowledge)
        gates[:acknowledge].pop
        original_acknowledge.call(*arguments)
      end
      program = build_program(tenants: ["wait"], interval: 60, ingest: ->(*) {})
      program.pipeline_capacity 1
      initializations = 0
      program.initial_cursor do |_id|
        initializations += 1
        AcpPostgresBudgetRecord.lease_connection.select_value("SELECT 1")
        Time.utc(2025, 1, 1)
      end
      program.resolve do |id|
        AcpPostgresBudgetRecord.connection.select_value("SELECT 1")
        id
      end
      program.fetch do |_tenant, context|
        assert_nil pool.active_connection?
        started.push(:fetch)
        gates[:fetch].pop
        Acp::Batch.new(data: [], next_cursor: context.cursor)
      end
      integrated = Acp::Rails.configuration_for(program, transaction_owner: pool)
      runtime = Acp::Runtime.new(configuration: integrated, progress: progress)
      runtime.define_singleton_method(:initial_offset) { |_| 0 }
      Async do |root|
        runner = root.async { runtime.run }
        begin
          %i[fetch acknowledge].each do |stage|
            assert_equal stage, root.with_timeout(2) { started.pop }
            Kernel.sleep(0.02) # The callback/acknowledgement remains suspended throughout.
            assert_equal 0, pool.stat[:busy]
            gates[stage].push(true)
          end
          root.with_timeout(2) { Kernel.sleep(0.001) until runtime.metrics[:completed_cycles] == 1 }
        ensure
          runner.stop
        end
      end.wait
      assert_equal 1, initializations
      assert_equal 1, runtime.metrics[:max_active_resolves]
      assert_equal 1, progress.acknowledgements.length
    end
  end

  def test_initial_cursor_callbacks_share_the_resolution_semaphore
    with_budget_pool(6) do |pool|
      active = 0
      maximum = 0
      released = false
      entered = Async::Queue.new
      program = build_program(tenants: (1..8).to_a, interval: 60, ingest: ->(*) {})
      program.pipeline_capacity 8
      program.resolve_concurrency 2
      program.ingest_concurrency 3
      program.initial_cursor do |_id|
        active += 1
        maximum = [maximum, active].max
        AcpPostgresBudgetRecord.connection.select_value("SELECT 1")
        entered.push(true)
        Kernel.sleep(0.001) until released
        Time.utc(2025, 1, 1)
      ensure
        active -= 1
      end
      runtime = Acp::Runtime.new(configuration: Acp::Rails.configuration_for(program, transaction_owner: pool),
                                 progress: MemoryProgress.new)
      runtime.define_singleton_method(:initial_offset) { |_| 0 }
      Async do |root|
        runner = root.async { runtime.run }
        begin
          root.with_timeout(2) { 2.times { entered.pop } }
          Kernel.sleep(0.02)
          assert_equal 2, active
          assert_equal 2, pool.stat[:busy]
          released = true
          root.with_timeout(2) { Kernel.sleep(0.001) until runtime.metrics[:completed_cycles] == 8 }
        ensure
          runner.stop
        end
      end.wait
      assert_equal 2, maximum
      assert_equal 2, runtime.metrics[:max_active_resolves]
      assert_equal 8, runtime.metrics[:completed_cycles]
    end
  end

  def test_real_executor_and_permanent_checkout_cleanup_for_each_database_callback
    with_budget_pool(2) do |pool|
      %i[tenants initial_cursor resolve ingest tenants_page].each do |name|
        %i[complete error cancel].each do |outcome|
          program = build_program(tenants: [], ingest: ->(*) {})
          program.pipeline_capacity 1
          started = Async::Queue.new
          program.public_send(name) do |*|
            AcpPostgresCurrent.tenant_id = "callback"
            AcpPostgresBudgetRecord.lease_connection.select_value("SELECT 1")
            assert_equal 1, pool.stat[:busy]
            started.push(true)
            raise IOError, "callback failed" if outcome == :error

            Kernel.sleep(60) if outcome == :cancel
            name == :tenants_page ? [[], nil] : Time.utc(2025, 1, 1)
          end
          integrated = Acp::Rails.configuration_for(program, transaction_owner: pool)
          arguments = { tenants: [->(*) {}], initial_cursor: ["tenant"], resolve: ["tenant"], ingest: [nil, nil],
                        tenants_page: [nil, 1] }.fetch(name)
          Async do |root|
            callback = root.async do
              if outcome == :error
                assert_raises(IOError) { integrated.callback(name).call(*arguments) }
              else
                integrated.callback(name).call(*arguments)
              end
            ensure
              assert_nil pool.active_connection?
              assert_nil AcpPostgresCurrent.tenant_id
            end
            root.with_timeout(2) { started.pop }
            callback.stop if outcome == :cancel
            callback.wait
          end.wait
          assert_equal 0, pool.stat[:busy]
        end
      end
    end
  end

  def test_lease_loss_unwinds_initial_cursor_resolution_and_ingestion_checkouts
    %i[initial_cursor resolve ingest].each do |name|
      with_budget_pool(2) do |pool|
        started = false
        ownership = Acp::LocalOwnership.new
        ownership.define_singleton_method(:renewal_interval) { |_| 0.01 }
        ownership.define_singleton_method(:renew) { |*| !started }
        program = build_program(tenants: ["lost"], interval: 60, ingest: ->(*) {})
        program.pipeline_capacity 1
        program.public_send(name) do |*|
          AcpPostgresBudgetRecord.lease_connection.select_value("SELECT 1")
          AcpPostgresCurrent.tenant_id = "lost"
          started = true
          Kernel.sleep(60)
        end
        progress = MemoryProgress.new
        runtime = Acp::Runtime.new(configuration: Acp::Rails.configuration_for(program, transaction_owner: pool),
                                   progress: progress, ownership: ownership)
        runtime.define_singleton_method(:initial_offset) { |_| 0 }
        run_until(runtime, timeout: 2) { started && runtime.metrics[:active_cycles].zero? }
        assert started
        assert_equal 1, runtime.metrics[:renewal_deadline_misses]
        assert_empty progress.acknowledgements
        assert_nil AcpPostgresCurrent.tenant_id
      end
    end
  end

  def test_parked_retries_share_the_exact_bound_pool_after_readmission
    with_budget_pool(2) do |pool|
      maximum = 0
      program = build_program(tenants: %w[a b c d], interval: 60, ingest: lambda do |_batch, context|
        AcpPostgresBudgetRecord.lease_connection.select_value("SELECT 1")
        maximum = [maximum, pool.stat[:busy]].max
        raise IOError, "retry without retaining checkout" if context.attempt == 1
      end)
      program.pipeline_capacity 1
      program.resolve_concurrency 5
      program.ingest_concurrency 7
      program.retry_capacity 4
      program.ingest_retry on: IOError, max_attempts: 2, backoff: 0.1, max_backoff: 0.1
      progress = MemoryProgress.new
      runtime = Acp::Runtime.new(configuration: Acp::Rails.configuration_for(program, transaction_owner: pool),
                                 progress: progress)
      runtime.define_singleton_method(:initial_offset) { |_| 0 }

      run_until(runtime, timeout: 3) { runtime.metrics[:completed_cycles] == 4 }

      assert_equal 4, runtime.metrics[:completed_cycles]
      assert_operator runtime.metrics[:max_active_cycles], :>, 1
      assert_operator runtime.metrics[:max_parked_cycles], :>, 0
      assert_equal 1, runtime.metrics[:max_pipeline_reservations]
      assert_equal 1, maximum
      assert_equal 4, progress.acknowledgements.length
    end
  end
  # rubocop:enable Metrics/BlockLength, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
