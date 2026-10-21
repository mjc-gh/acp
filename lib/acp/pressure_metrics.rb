# frozen_string_literal: true

require "English"

module Acp
  # Fixed-cardinality, lifetime histograms. No individual observations are retained.
  class PressureMetrics
    BOUNDARIES = [0.0, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300].freeze
    OUTCOMES = %i[success error timeout cancelled].freeze
    TIMEOUT_CLASSES = %w[Acp::StageTimeoutError Async::TimeoutError Timeout::Error
                         Redis::TimeoutError RedisClient::TimeoutError].freeze

    def initialize(names, clock:)
      @clock = clock
      @histograms = names.to_h do |name|
        [name, { count: 0, sum: 0.0, min: nil, max: nil, last: 0.0,
                 buckets: Array.new(BOUNDARIES.length + 1, 0),
                 outcomes: OUTCOMES.to_h { |outcome| [outcome, 0] } }]
      end
    end

    # Update one fixed histogram without allocating a sample object.
    # rubocop:disable Metrics/AbcSize
    def observe(name, value, outcome: :success)
      histogram = @histograms.fetch(name)
      histogram[:count] += 1
      histogram[:sum] += value
      histogram[:last] = value
      histogram[:min] = histogram[:min] ? [histogram[:min], value].min : value
      histogram[:max] = histogram[:max] ? [histogram[:max], value].max : value
      index = BOUNDARIES.bsearch_index { |boundary| value <= boundary } || BOUNDARIES.length
      histogram[:buckets][index] += 1
      histogram[:outcomes][outcome] += 1
    end
    # rubocop:enable Metrics/AbcSize

    def measure(name)
      started = @clock.now
      result = yield
      completed = true
      result
    ensure
      outcome = completed ? :success : self.class.outcome($ERROR_INFO)
      observe(name, [@clock.now - started, 0].max, outcome: outcome)
    end

    def self.outcome(error)
      return :success unless error
      return :timeout if error.class.ancestors.any? { |type| TIMEOUT_CLASSES.include?(type.name) }
      return :cancelled if error.is_a?(CancellationError) || !error.is_a?(StandardError)

      :error
    end

    def snapshot
      @histograms.to_h do |name, histogram|
        values = histogram.merge(buckets: histogram[:buckets].dup.freeze,
                                 outcomes: histogram[:outcomes].dup.freeze)
        [50, 95, 99].each { |percentile| values["p#{percentile}".to_sym] = percentile(histogram, percentile) }
        [name, values.freeze]
      end.freeze
    end

    private

    def percentile(histogram, percentile)
      return 0.0 if histogram[:count].zero?

      target = (histogram[:count] * percentile / 100.0).ceil
      cumulative = 0
      index = histogram[:buckets].index { |count| (cumulative += count) >= target }
      BOUNDARIES[index] # nil denotes the open-ended (>300s) bucket, including in JSON.
    end
  end
end
