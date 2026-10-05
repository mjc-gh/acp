# frozen_string_literal: true

require "test_helper"
require "open3"
require "rbconfig"

class TestAcp < Minitest::Test
  class ValidProgram < Acp::Program
    interval 60
    fetch_concurrency 10
    ingest_concurrency 2
    pipeline_capacity 4

    tenants { |_emit| }
    initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
    resolve { |tenant_id| { id: tenant_id } }
    fetch { |_tenant, _context| Acp::Batch.new(data: [], next_cursor: Time.utc(2025, 1, 1)) }
    ingest { |_batch, _context| }
  end

  class ChildProgram < ValidProgram
    interval 30
    fetch do |tenant, context|
      [tenant, context]
    end
  end

  class InvalidProgram < ValidProgram
    interval 0
  end

  class IncompleteProgram < Acp::Program
    interval 60
    fetch_concurrency 2
    ingest_concurrency 1
    pipeline_capacity 2
  end

  def test_it_exposes_a_version
    refute_nil Acp::VERSION
  end

  def test_program_configuration_is_validated_and_immutable
    config = ValidProgram.configuration

    assert_equal "TestAcp::ValidProgram", config.name
    assert_equal 4, config.effective_fetch_concurrency
    assert config.frozen?
    assert config.callbacks.frozen?
    assert_raises(FrozenError) { config.callbacks[:fetch] = nil }
  end

  def test_subclass_changes_do_not_mutate_the_parent_definition
    assert_equal 60, ValidProgram.configuration.interval
    assert_equal 30, ChildProgram.configuration.interval
    refute_same ValidProgram.configuration.callbacks, ChildProgram.configuration.callbacks
  end

  def test_explicit_program_names_work_for_anonymous_definitions
    program = Class.new(ValidProgram)
    program.program_name "ExplicitSync"

    assert_equal "ExplicitSync", program.program_name
    assert_equal "ExplicitSync", program.configuration.name
    assert_equal "ExplicitSync", Class.new(program).configuration.name
  end

  def test_configuration_rejects_missing_callbacks
    error = assert_raises(Acp::ConfigurationError) { IncompleteProgram.configuration }
    assert_match(/missing callbacks/, error.message)
  end

  def test_configuration_rejects_invalid_limits
    assert_raises(Acp::ConfigurationError) { InvalidProgram.configuration }
  end

  def test_callbacks_receive_consumer_values_and_context
    fetched_tenant = ValidProgram.configuration.callback(:resolve).call(7)
    context = Acp::FetchContext.new(
      tenant_id: 7,
      cursor: Time.utc(2025, 1, 1),
      poll_id: "poll-1",
      attempt: 2
    )
    result = ChildProgram.configuration.callback(:fetch).call(fetched_tenant, context)

    assert_equal [fetched_tenant, context], result
    assert_equal [7, "poll-1", 2], [context.tenant_id, context.poll_id, context.attempt]
  end

  def test_batch_accepts_empty_and_unchanged_results_but_rejects_regression
    cursor = Time.utc(2025, 1, 1, 0, 0, 0, 123_456)
    empty = Acp::Batch.new(data: [], next_cursor: cursor)

    assert_equal [], empty.data
    assert_same empty, empty.validate_after!(cursor)
    assert_raises(Acp::CursorRegressionError) do
      Acp::Batch.new(data: [1], next_cursor: cursor - 1).validate_after!(cursor)
    end
  end

  def test_batch_requires_an_explicit_cursor
    assert_raises(Acp::InvalidBatchError) { Acp::Batch.new(data: [], next_cursor: nil) }
  end

  def test_timestamp_round_trip_preserves_microseconds_and_normalizes_utc
    timestamp = Time.new(2025, 2, 3, 4, 5, 6, "+02:00") + Rational(123_456, 1_000_000)
    serialized = Acp::Timestamp.dump(timestamp)
    restored = Acp::Timestamp.load(serialized)

    assert_equal "2025-02-03T02:05:06.123456Z", serialized
    assert_equal timestamp.to_r, restored.to_r
    assert_equal 0, restored.utc_offset
    assert_equal 7_200, timestamp.utc_offset
  end

  def test_timestamp_rejects_timezone_free_or_overprecise_values
    assert_raises(ArgumentError) { Acp::Timestamp.load("2025-01-01T00:00:00") }
    assert_raises(ArgumentError) { Acp::Timestamp.load("2025-01-01T00:00:00.1234567Z") }
    assert_raises(ArgumentError) { Acp::Timestamp.dump(Time.at(1, 1, :nanosecond)) }
  end

  def test_core_load_does_not_load_rails
    library = File.expand_path("../lib", __dir__)
    script = <<~'RUBY'
      require "acp"
      rails_loaded = $LOADED_FEATURES.any? do |path|
        path.match?(%r{/(?:active_record|rails)(?:/|\.rb)})
      end
      abort if rails_loaded
    RUBY
    _stdout, _stderr, status = Open3.capture3(RbConfig.ruby, "-I#{library}", "-e", script)

    assert status.success?, "requiring acp loaded Rails or Active Record"
  end
end

class TestRetryPolicy < Minitest::Test
  def test_policy_classifies_errors_and_excludes_cancellation
    require "async"

    policy = Acp::RetryPolicy.new(on: [IOError, ->(error) { error.message == "busy" }], max_attempts: 3)

    assert policy.retryable?(IOError.new, attempt: 1, elapsed: 0)
    assert policy.retryable?(RuntimeError.new("busy"), attempt: 1, elapsed: 0)
    refute policy.retryable?(Acp::CancellationError.new, attempt: 1, elapsed: 0)
    refute policy.retryable?(Async::Stop.new, attempt: 1, elapsed: 0)
  end

  def test_attempt_and_elapsed_budgets_stop_retries
    policy = Acp::RetryPolicy.new(on: IOError, max_attempts: 3, max_elapsed: 10)

    refute policy.retryable?(IOError.new, attempt: 3, elapsed: 2)
    refute policy.retryable?(IOError.new, attempt: 1, elapsed: 10)
  end

  def test_exponential_backoff_is_capped
    policy = Acp::RetryPolicy.new(backoff: 1, max_backoff: 3)

    assert_equal 2, policy.delay(attempt: 2)
    assert_equal 3, policy.delay(attempt: 5)
  end

  def test_invalid_budgets_are_rejected
    assert_raises(Acp::ConfigurationError) { Acp::RetryPolicy.new(max_attempts: 0) }
    assert_raises(Acp::ConfigurationError) { Acp::RetryPolicy.new(backoff: 2, max_backoff: 1) }
  end
end
