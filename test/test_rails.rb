# frozen_string_literal: true

require "test_helper"
require "rails"
require "acp/rails"

# rubocop:disable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
class TestRailsIntegration < Minitest::Test
  class FakeConnection
    attr_reader :commits

    def initialize
      @open = false
      @commits = 0
    end

    def transaction_open?
      @open
    end

    def lose_next_commit_response!
      @lose_next_commit_response = true
    end

    def transaction
      @open = true
      result = yield
      @commits += 1
      if @lose_next_commit_response
        @lose_next_commit_response = false
        raise PG::ConnectionBad, "simulated lost COMMIT response"
      end
      result
    rescue ActiveRecord::Rollback
      nil
    ensure
      @open = false
    end
  end

  class FakePool
    attr_reader :connection, :active, :size, :checkout_options

    def initialize(size: 5)
      @size = size
      @connection = FakeConnection.new
      @active = 0
    end

    def db_config
      Struct.new(:adapter).new("postgresql")
    end

    def with_connection(**options)
      @checkout_options = options
      @active += 1
      yield connection
    ensure
      @active -= 1
    end
  end

  class FakeExecutor
    attr_reader :calls, :active

    def initialize
      @calls = 0
      @active = 0
    end

    def wrap
      @calls += 1
      @active += 1
      yield
    ensure
      @active -= 1
    end
  end

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
    @prior_isolation_level = ActiveSupport::IsolatedExecutionState.isolation_level
    ActiveSupport::IsolatedExecutionState.isolation_level = :fiber
  end

  def teardown
    ActiveSupport::IsolatedExecutionState.isolation_level = @prior_isolation_level
  end

  def build_program(interval: 0.01, ingest: ->(_batch, _context) {})
    Class.new(Acp::Program) do
      define_singleton_method(:name) { "RailsIntegrationTestProgram" }
      self.interval interval
      fetch_concurrency 1
      ingest_concurrency 1
      pipeline_capacity 1
      tenants { |emit| emit.call("tenant") }
      initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
      resolve { |tenant_id| tenant_id }
      fetch { |_tenant, context| Acp::Batch.new(data: [], next_cursor: context.cursor) }
      self.ingest(&ingest)
    end
  end

  def build_application(isolation_level: :fiber)
    active_support_config = Struct.new(:isolation_level).new(isolation_level)
    config = Struct.new(:active_support).new(active_support_config)
    Struct.new(:config, :executor).new(config, FakeExecutor.new)
  end

  def test_database_callbacks_checkout_connections_and_all_callbacks_use_executor
    pool = FakePool.new
    application = build_application
    program = build_program
    program.resolve do |tenant_id|
      assert_equal 1, pool.active
      tenant_id
    end
    program.fetch do |_tenant, _context|
      assert_equal 0, pool.active
    end
    config = Acp::Rails.configuration_for(program, transaction_owner: pool, application: application)

    config.callback(:tenants).call(->(_tenant_id) { assert_equal 1, pool.active })
    config.callback(:resolve).call("tenant")
    config.callback(:fetch).call("tenant", nil)

    assert_equal 0, pool.active
    assert_equal 0, pool.connection.commits
    assert_equal 3, application.executor.calls
  end

  def test_ingestion_commits_before_callback_returns
    pool = FakePool.new
    config = Acp::Rails.configuration_for(
      build_program(ingest: ->(_batch, _context) { assert pool.connection.transaction_open? }),
      transaction_owner: pool,
      application: build_application
    )

    config.callback(:ingest).call(:batch, :context)

    assert_equal 1, pool.connection.commits
    assert_equal 0, pool.active
  end

  def test_rollback_and_ambient_transactions_are_rejected
    pool = FakePool.new
    config = Acp::Rails.configuration_for(
      build_program(ingest: ->(_batch, _context) { raise ActiveRecord::Rollback }),
      transaction_owner: pool,
      application: build_application
    )

    assert_raises(Acp::Rails::TransactionRolledBack) do
      config.callback(:ingest).call(:batch, :context)
    end
    assert_equal 0, pool.connection.commits

    pool.connection.instance_variable_set(:@open, true)
    assert_raises(Acp::Rails::UnsupportedTransaction) do
      config.callback(:ingest).call(:batch, :context)
    end
  end

  def test_replays_uncertain_commit_with_the_same_batch_before_acknowledging
    ingestions = []
    destination = {}
    fetches = 0
    pool = FakePool.new
    pool.connection.lose_next_commit_response!
    program = build_program(
      interval: 2,
      ingest: lambda do |batch, context|
        ingestions << [batch.object_id, context.poll_id, context.attempt]
        destination[context.poll_id] ||= batch.object_id
      end
    )
    program.fetch do |_tenant, context|
      fetches += 1
      Acp::Batch.new(data: [], next_cursor: context.cursor)
    end
    config = Acp::Rails.configuration_for(program, transaction_owner: pool, application: build_application)
    progress = MemoryProgress.new
    runtime = Acp::Runtime.new(configuration: config, progress: progress)

    Async do |root|
      task = root.async { runtime.run }
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 3
      while progress.acknowledgements.empty? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        Kernel.sleep(0.005)
      end
      task.stop
    end

    assert_equal 1, fetches
    assert_equal [1, 2], ingestions.map(&:last)
    assert_equal ingestions.first.first(2), ingestions.last.first(2)
    assert_equal 1, destination.length
    assert_equal 2, pool.connection.commits
    assert_equal 1, progress.acknowledgements.length
  end

  def test_validates_fiber_isolation_and_connection_pool_budget
    error = assert_raises(Acp::ConfigurationError) do
      Acp::Rails.configuration_for(
        build_program,
        transaction_owner: FakePool.new(size: 1),
        application: build_application(isolation_level: :thread)
      )
    end
    assert_match(/isolation_level = :fiber/, error.message)

    error = assert_raises(Acp::ConfigurationError) do
      Acp::Rails.configuration_for(
        build_program,
        transaction_owner: FakePool.new(size: 1),
        application: build_application
      )
    end
    assert_match(/pool size 1 is below the required 2/, error.message)
  end

  def test_shared_cycle_pool_budget_boundaries
    [[8, 8, 8, 9], [100, 5, 7, 13], [1, 5, 7, 2],
     [6, 2, 4, 7], [4, 1, 8, 5], [20, 9, 2, 12]].each do |pipeline, resolve, ingest, required|
      program = build_program
      program.pipeline_capacity pipeline
      program.resolve_concurrency resolve
      program.ingest_concurrency ingest
      config = Acp::Rails.configuration_for(
        program, transaction_owner: FakePool.new(size: required), application: build_application
      )
      assert_equal pipeline, config.pipeline_capacity

      error = assert_raises(Acp::ConfigurationError) do
        Acp::Rails.configuration_for(
          program, transaction_owner: FakePool.new(size: required - 1), application: build_application
        )
      end
      assert_includes error.message, "pool size #{required - 1} is below the required #{required}"
      assert_includes error.message, "pipeline_capacity=#{pipeline}"
      assert_includes error.message, "resolve_concurrency=#{resolve}"
      assert_includes error.message, "ingest_concurrency=#{ingest}"
      assert_includes error.message, "+ 1 discovery"
      assert_includes error.message, "application headroom"
    end
  end

  # rubocop:disable Metrics/BlockLength
  def test_database_callback_scopes_unwind_on_errors_and_cancellation
    %i[tenants initial_cursor resolve ingest tenants_page].each do |name|
      %i[error cancellation].each do |outcome|
        pool = FakePool.new(size: 2)
        application = build_application
        program = build_program
        entered = false
        program.public_send(name) do |*|
          entered = true
          assert_equal 1, pool.active
          assert_equal 1, application.executor.active
          raise IOError, "callback failed" if outcome == :error

          Kernel.sleep(60)
        end
        config = Acp::Rails.configuration_for(program, transaction_owner: pool, application: application)
        arguments = { tenants: [->(*) {}], tenants_page: [nil, 1], initial_cursor: ["tenant"],
                      resolve: ["tenant"], ingest: [nil, nil] }.fetch(name)
        if outcome == :error
          assert_raises(IOError) { config.callback(name).call(*arguments) }
        else
          Async do |root|
            callback = root.async { config.callback(name).call(*arguments) }
            root.with_timeout(1) { Kernel.sleep(0.001) until entered }
            callback.stop
            callback.wait
          end.wait
        end
        assert entered
        assert_equal({ prevent_permanent_checkout: true }, pool.checkout_options)
        assert_equal 0, pool.active
        assert_equal 0, application.executor.active
        refute pool.connection.transaction_open?
      end
    end
  end
  # rubocop:enable Metrics/BlockLength

  def test_large_http_pipeline_can_use_a_small_database_pool
    program = build_program
    program.fetch_concurrency 2_500
    program.pipeline_capacity 2_500
    program.resolve_concurrency 2
    program.ingest_concurrency 4

    config = Acp::Rails.configuration_for(
      program, transaction_owner: FakePool.new(size: 7), application: build_application
    )

    assert_equal 2_500, config.effective_fetch_concurrency
    assert_equal 2, config.effective_resolve_concurrency
  end
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
