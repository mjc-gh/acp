# frozen_string_literal: true

module Acp
  # Immutable, validated settings and callbacks for one program class.
  class Configuration
    REQUIRED_CALLBACKS = %i[initial_cursor resolve fetch ingest].freeze
    REQUIRED_SETTINGS = %i[interval fetch_concurrency ingest_concurrency pipeline_capacity].freeze

    attr_reader :name, :interval, :fetch_concurrency, :ingest_concurrency,
                :pipeline_capacity, :callbacks, :fetch_retry, :ingest_retry,
                :discovery_interval, :retry_cooldown, :resolve_concurrency,
                :retry_capacity, :upstream_capacity, :resolve_timeout, :initial_cursor_timeout,
                :coordination_timeout, :cleanup_timeout, :acknowledge_retry, :discovery_batch_size,
                :retained_byte_capacity, :max_batch_bytes, :max_response_bytes

    # The policies are independent parts of a program definition.
    # rubocop:disable Metrics/ParameterLists
    def initialize(name:, settings:, callbacks:, fetch_retry:, ingest_retry:, acknowledge_retry: nil)
      @name = validate_name(name)
      validate_definition!(settings, callbacks)
      assign_settings(settings)
      @callbacks = callbacks.dup.freeze
      @fetch_retry = fetch_retry
      @ingest_retry = ingest_retry
      @acknowledge_retry = acknowledge_retry || ingest_retry
      @discovery_interval = positive_number(settings.fetch(:discovery_interval, 60), "discovery_interval")
      @retry_cooldown = positive_number(settings.fetch(:retry_cooldown, settings.fetch(:interval)), "retry_cooldown")
      freeze
    end
    # rubocop:enable Metrics/ParameterLists

    def effective_fetch_concurrency
      [fetch_concurrency, pipeline_capacity].min
    end

    def effective_resolve_concurrency
      [resolve_concurrency, pipeline_capacity].min
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
      resolve_limit = settings.fetch(:resolve_concurrency, ingest_concurrency)
      @resolve_concurrency = positive_integer(resolve_limit, "resolve_concurrency")
      assign_isolation_settings(settings)
      assign_discovery_settings(settings)
      assign_memory_settings(settings)
    end

    def assign_memory_settings(settings)
      %i[retained_byte_capacity max_batch_bytes max_response_bytes].each do |name|
        value = settings[name]
        instance_variable_set(:"@#{name}", value.nil? ? nil : positive_integer(value, name.to_s))
      end
      return unless retained_byte_capacity && !max_batch_bytes

      raise ConfigurationError, "retained_byte_capacity requires max_batch_bytes"
    end

    def assign_isolation_settings(settings)
      @retry_capacity = settings.fetch(:retry_capacity, 0)
      unless @retry_capacity.is_a?(Integer) && @retry_capacity >= 0
        raise ConfigurationError, "retry_capacity must be a nonnegative integer"
      end

      @upstream_capacity = positive_integer(settings.fetch(:upstream_capacity, pipeline_capacity), "upstream_capacity")
      %i[resolve_timeout initial_cursor_timeout coordination_timeout].each do |name|
        value = settings[name]
        instance_variable_set(:"@#{name}", value.nil? ? nil : positive_number(value, name.to_s))
      end
      @cleanup_timeout = positive_number(settings.fetch(:cleanup_timeout, 1), "cleanup_timeout")
    end

    def assign_discovery_settings(settings)
      @discovery_batch_size = positive_integer(settings.fetch(:discovery_batch_size, 256), "discovery_batch_size")
      raise ConfigurationError, "discovery_batch_size must be at most 1024" if @discovery_batch_size > 1024
    end

    def validate_definition!(settings, callbacks)
      missing_settings = REQUIRED_SETTINGS.reject { |setting| settings.key?(setting) }
      missing_callbacks = REQUIRED_CALLBACKS.reject { |callback| callbacks[callback].respond_to?(:call) }
      raise ConfigurationError, "missing settings: #{missing_settings.join(", ")}" unless missing_settings.empty?
      raise ConfigurationError, "missing callbacks: #{missing_callbacks.join(", ")}" unless missing_callbacks.empty?

      validate_discovery!(callbacks)
    end

    def validate_discovery!(callbacks)
      discovery = %i[tenants tenants_page].count { |name| callbacks[name].respond_to?(:call) }
      raise ConfigurationError, "provide exactly one of tenants or tenants_page" unless discovery == 1
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
