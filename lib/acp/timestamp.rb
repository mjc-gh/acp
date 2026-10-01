# frozen_string_literal: true

require "date"
require "time"

module Acp
  # Durable cursors use UTC timestamps with microsecond precision.
  module Timestamp
    PRECISION = 6
    ISO8601_WITH_ZONE = /(?:Z|[+-]\d{2}:?\d{2})\z/i

    module_function

    def normalize(value)
      timestamp = parse(value)
      validate_precision(timestamp)
      timestamp.getutc.freeze
    end

    def dump(value)
      normalize(value).iso8601(PRECISION)
    end

    def load(value)
      normalize(value)
    end

    def parse(value)
      case value
      when Time then value
      when DateTime
        validate_fraction(value.sec_fraction)
        value.to_time
      when String then parse_string(value)
      else raise ArgumentError, "expected a Time, DateTime, or ISO 8601 timestamp"
      end
    end

    def parse_string(value)
      raise ArgumentError, "timestamp strings must include a UTC offset" unless value.match?(ISO8601_WITH_ZONE)

      fraction = value.match(/\.(\d+)(?=(?:Z|[+-]\d{2}:?\d{2})\z)/i)&.[](1)
      validate_fraction(Rational("0.#{fraction}")) if fraction
      Time.iso8601(value)
    rescue Date::Error => e
      raise ArgumentError, "invalid timestamp: #{e.message}"
    end

    def validate_precision(timestamp)
      return if (timestamp.nsec % 1_000).zero?

      raise ArgumentError, "timestamp precision finer than microseconds is unsupported"
    end

    def validate_fraction(fraction)
      return if (fraction * 1_000_000).denominator == 1

      raise ArgumentError, "timestamp precision finer than microseconds is unsupported"
    end
  end
end
