# frozen_string_literal: true

module Acp
  # Immutable, validated settings and callbacks for one program class.
  class Configuration
    REQUIRED_CALLBACKS = %i[tenants initial_cursor resolve fetch ingest].freeze
    REQUIRED_SETTINGS = %i[interval fetch_concurrency ingest_concurrency pipeline_capacity].freeze

    attr_reader :name, :interval, :fetch_concurrency, :ingest_concurrency,
                :pipeline_capacity, :callbacks, :fetch_retry, :ingest_retry

    def initialize(name:, settings:, callbacks:, fetch_retry:, ingest_retry:)
      @name = validate_name(name)
      validate_definition!(settings, callbacks)
      assign_settings(settings)
      @callbacks = callbacks.dup.freeze
      @fetch_retry = fetch_retry
      @ingest_retry = ingest_retry
      freeze
    end

    def effective_fetch_concurrency
      [fetch_concurrency, pipeline_capacity].min
    end

    def callback(name)
      callbacks.fetch(name.to_sym)
    end

    private

    def assign_settings(settings)
      @interval = positive_number(settings.fetch(:interval), "interval")
      @fetch_concurrency = positive_integer(settings.fetch(:fetch_concurrency), "fetch_concurrency")
      @ingest_concurrency = positive_integer(settings.fetch(:ingest_concurrency), "ingest_concurrency")
      @pipeline_capacity = positive_integer(settings.fetch(:pipeline_capacity), "pipeline_capacity")
    end

    def validate_definition!(settings, callbacks)
      missing_settings = REQUIRED_SETTINGS.reject { |setting| settings.key?(setting) }
      missing_callbacks = REQUIRED_CALLBACKS.reject { |callback| callbacks[callback].respond_to?(:call) }
      raise ConfigurationError, "missing settings: #{missing_settings.join(", ")}" unless missing_settings.empty?
      raise ConfigurationError, "missing callbacks: #{missing_callbacks.join(", ")}" unless missing_callbacks.empty?
    end

    def validate_name(value)
      valid = value.is_a?(String) && !value.strip.empty?
      raise ConfigurationError, "program name must be a nonempty string" unless valid

      value.dup.freeze
    end

    def positive_integer(value, name)
      return value if value.is_a?(Integer) && value.positive?

      raise ConfigurationError, "#{name} must be a positive integer"
    end

    def positive_number(value, name)
      valid = value.is_a?(Numeric) && value.finite? && value.positive?
      return value if valid

      raise ConfigurationError, "#{name} must be a finite positive number"
    end
  end
end
