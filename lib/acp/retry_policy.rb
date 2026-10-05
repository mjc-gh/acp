# frozen_string_literal: true

module Acp
  # Consumer-selected retry classification and bounded backoff parameters.
  class RetryPolicy
    attr_reader :on, :max_attempts, :max_elapsed, :backoff, :max_backoff, :jitter, :timeout

    def initialize(on: [], **budgets)
      @on = normalize_matchers(on)
      validate_budget_names!(budgets)
      assign_budget_values(budgets)
      validate_budgets!
      freeze
    end

    def retryable?(error, attempt:, elapsed:)
      return false if cancellation?(error)
      return false unless attempt_allowed?(attempt) && elapsed_allowed?(elapsed)

      on.any? { |matcher| matches?(matcher, error) }
    end

    # Attempt is one-based; the first retry uses the base backoff.
    def delay(attempt:, random: Random)
      attempt = positive_integer(attempt, "attempt")
      exponential = exponential_delay(attempt)
      return exponential if jitter.zero?

      apply_jitter(exponential, random)
    end

    private

    def validate_budget_names!(budgets)
      unknown = budgets.keys - %i[max_attempts max_elapsed backoff max_backoff jitter timeout]
      raise ConfigurationError, "unknown retry budgets: #{unknown.join(", ")}" unless unknown.empty?
    end

    def assign_budget_values(budgets)
      @max_attempts = positive_integer(budgets.fetch(:max_attempts, 1), "max_attempts")
      @max_elapsed = optional_duration(budgets[:max_elapsed], "max_elapsed")
      @timeout = optional_duration(budgets[:timeout], "timeout")
      @backoff = nonnegative_number(budgets.fetch(:backoff, 0), "backoff")
      @max_backoff = nonnegative_number(budgets.fetch(:max_backoff, 0), "max_backoff")
      @jitter = nonnegative_number(budgets.fetch(:jitter, 0), "jitter")
    end

    def cancellation?(error)
      return true if process_cancellation?(error)

      error.class.ancestors.any? { |ancestor| async_stop_class?(ancestor) }
    end

    def process_cancellation?(error)
      [CancellationError, Interrupt, SystemExit, SignalException, NoMemoryError].any? do |klass|
        error.is_a?(klass)
      end
    end

    def async_stop_class?(klass)
      klass.name&.match?(/\AAsync::(?:.*::)?(?:Stop|Cancel)\z/)
    end

    def attempt_allowed?(attempt)
      attempt.is_a?(Integer) && attempt.positive? && attempt < max_attempts
    end

    def elapsed_allowed?(elapsed)
      elapsed.is_a?(Numeric) && elapsed.finite? && elapsed >= 0 && (max_elapsed.nil? || elapsed < max_elapsed)
    end

    def matches?(matcher, error)
      matcher.is_a?(Class) ? error.is_a?(matcher) : matcher.call(error)
    end

    def normalize_matchers(matchers)
      result = Array(matchers).dup
      valid = result.all? { |matcher| exception_class?(matcher) || matcher.respond_to?(:call) }
      raise ConfigurationError, "retry matchers must be exception classes or callable predicates" unless valid

      result.freeze
    end

    def exception_class?(matcher)
      matcher.is_a?(Class) && matcher <= Exception
    end

    def validate_budgets!
      raise ConfigurationError, "max_backoff must be at least backoff" if max_backoff < backoff
      raise ConfigurationError, "jitter must be between 0 and 1" if jitter > 1
    end

    def exponential_delay(attempt)
      return 0 if backoff.zero? || max_backoff.zero?
      return max_backoff if attempt > Math.log2(max_backoff / backoff) + 1

      [backoff * (2**(attempt - 1)), max_backoff].min
    end

    def apply_jitter(delay, random)
      [delay * (1 + ((random.rand * 2) - 1) * jitter), 0].max
    end

    def positive_integer(value, name)
      return value if value.is_a?(Integer) && value.positive?

      raise ConfigurationError, "#{name} must be a positive integer"
    end

    def nonnegative_number(value, name)
      valid = value.is_a?(Numeric) && value.finite? && value >= 0
      return value if valid

      raise ConfigurationError, "#{name} must be a finite nonnegative number"
    end

    def optional_duration(value, name)
      value.nil? ? nil : nonnegative_number(value, name)
    end
  end
end
