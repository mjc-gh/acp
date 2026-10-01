# frozen_string_literal: true

require "test_helper"
ENV["RAILS_ENV"] ||= "test"
require "rails"
require "active_record"
require_relative "../test/rails_app/config/application"

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

# Fiber-local application state that Rails' executor must reset after callbacks.
class AcpPostgresCurrent < ActiveSupport::CurrentAttributes
  attribute :tenant_id
end

# Run against the PostgreSQL service configured by test:postgres. These tables
# belong to the test consumer; Acp itself creates no schema.
# rubocop:disable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
class TestAcpPostgresIngestion < Minitest::Test
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
    connection.add_index(:acp_postgres_lock_rows, :lock_key, unique: true)
    %w[a b].each { |key| AcpPostgresLockRow.create!(lock_key: key) }
  end

  def teardown
    ActiveRecord::Base.connection.drop_table(:acp_postgres_events, if_exists: true)
    ActiveRecord::Base.connection.drop_table(:acp_postgres_lock_rows, if_exists: true)
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
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
