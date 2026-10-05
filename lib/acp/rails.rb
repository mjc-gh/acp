# frozen_string_literal: true

require "active_record"
require "pg"

module Acp
  # Explicit Rails 8 integration. Requiring acp alone never loads Rails.
  # rubocop:disable Metrics/ModuleLength, Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
  module Rails
    COMMITTED = Object.new.freeze
    private_constant :COMMITTED

    class TransactionRolledBack < Acp::Error; end
    class UnsupportedTransaction < Acp::Error; end

    class << self
      # Build a runtime configuration whose callbacks use the Rails executor and
      # whose database callbacks hold a connection only for their database work.
      # transaction_owner may be an Active Record model or a connection pool.
      def configuration_for(program, transaction_owner:, application: default_application,
                            ingest_retry: nil)
        configuration = program.respond_to?(:configuration) ? program.configuration : program
        validate_rails!(application)
        pool = connection_pool(transaction_owner)
        validate_postgres!(pool)
        validate_pool_budget!(configuration, pool)
        selected_retry = ingest_retry || rails_ingest_retry(configuration.ingest_retry)
        raise ConfigurationError, "ingest_retry must be an Acp::RetryPolicy" unless selected_retry.is_a?(RetryPolicy)

        callbacks = configuration.callbacks.to_h do |name, callback|
          [name, wrap_callback(name, callback, application, pool)]
        end

        Configuration.new(
          name: configuration.name,
          settings: settings_for(configuration),
          callbacks: callbacks,
          fetch_retry: configuration.fetch_retry,
          ingest_retry: selected_retry
        )
      end

      private

      def default_application
        return ::Rails.application if defined?(::Rails) && ::Rails.respond_to?(:application)

        raise ConfigurationError, "load a Rails application before configuring Acp::Rails"
      end

      def validate_rails!(application)
        unless defined?(::Rails) && ::Rails.respond_to?(:gem_version) && ::Rails.gem_version.segments.first == 8
          raise ConfigurationError, "Acp::Rails supports Rails 8.x"
        end
        if Gem::Version.new(RUBY_VERSION) < Gem::Version.new("3.2")
          raise ConfigurationError, "Acp::Rails requires Ruby 3.2 or newer"
        end

        configured_level = application.config.active_support.isolation_level
        runtime_level = ActiveSupport::IsolatedExecutionState.isolation_level
        return if configured_level == :fiber && runtime_level == :fiber

        raise ConfigurationError,
              "configure config.active_support.isolation_level = :fiber before Rails initializes"
      end

      def connection_pool(owner)
        pool = owner.respond_to?(:connection_pool) ? owner.connection_pool : owner
        return pool if pool.respond_to?(:with_connection) && pool.respond_to?(:size)

        raise ConfigurationError, "transaction_owner must be an Active Record model or connection pool"
      end

      def validate_pool_budget!(configuration, pool)
        concurrent_ingests = [configuration.ingest_concurrency, configuration.pipeline_capacity].min
        required = configuration.effective_resolve_concurrency + concurrent_ingests + 1
        return if pool.size >= required

        raise ConfigurationError,
              "transaction pool size #{pool.size} is below the required #{required} " \
              "(effective resolve_concurrency + effective ingest_concurrency + discovery)"
      end

      def validate_postgres!(pool)
        adapter = pool.db_config.adapter if pool.respond_to?(:db_config)
        if adapter && adapter != "postgresql"
          raise ConfigurationError, "Acp::Rails requires a PostgreSQL connection pool"
        end

        version = Gem.loaded_specs.fetch("pg").version
        return if version >= Gem::Version.new("1.5") && version < Gem::Version.new("2.0")

        raise ConfigurationError, "Acp::Rails supports pg >= 1.5, < 2.0"
      end

      def settings_for(configuration)
        {
          interval: configuration.interval,
          fetch_concurrency: configuration.fetch_concurrency,
          ingest_concurrency: configuration.ingest_concurrency,
          pipeline_capacity: configuration.pipeline_capacity,
          resolve_concurrency: configuration.resolve_concurrency,
          discovery_interval: configuration.discovery_interval,
          retry_cooldown: configuration.retry_cooldown
        }
      end

      def wrap_callback(name, callback, application, pool)
        lambda do |*arguments|
          application.executor.wrap do
            if database_callback?(name)
              pool.with_connection(prevent_permanent_checkout: true) do |connection|
                if name == :ingest
                  transactional_ingest(connection, callback, arguments)
                else
                  callback.call(*arguments)
                end
              end
            else
              callback.call(*arguments)
            end
          end
        end
      end

      def database_callback?(name)
        %i[tenants initial_cursor resolve ingest].include?(name)
      end

      def transactional_ingest(connection, callback, arguments)
        if connection.transaction_open?
          raise UnsupportedTransaction, "Acp ingestion cannot run inside an ambient transaction"
        end

        result = connection.transaction do
          callback.call(*arguments)
          COMMITTED
        end
        raise TransactionRolledBack, "Acp ingestion transaction rolled back" unless result.equal?(COMMITTED)

        nil
      end

      def rails_ingest_retry(policy)
        if policy.on.empty? && policy.max_attempts == 1 && policy.max_elapsed.nil? && policy.timeout.nil?
          return RetryPolicy.new(on: database_failure_matcher, max_attempts: 3, max_elapsed: 30,
                                 backoff: 0.1, max_backoff: 1)
        end

        RetryPolicy.new(on: policy.on + [database_failure_matcher], max_attempts: policy.max_attempts,
                        max_elapsed: policy.max_elapsed, backoff: policy.backoff,
                        max_backoff: policy.max_backoff, jitter: policy.jitter, timeout: policy.timeout)
      end

      def database_failure_matcher
        lambda do |error|
          transient_database_error?(error)
        end
      end

      def transient_database_error?(error)
        selected = %i[Deadlocked LockWaitTimeout SerializationFailure ConnectionNotEstablished ConnectionTimeoutError]
        return true if active_record_transient?(error, selected)

        pg_error = %i[ConnectionBad UnableToSend]
        postgres_transient?(error, pg_error)
      end

      def active_record_transient?(error, selected)
        error.class.ancestors.any? do |ancestor|
          selected.include?(ancestor.name&.split("::")&.last&.to_sym)
        end
      end

      def postgres_transient?(error, pg_error)
        cause = error
        while cause
          return true if cause.class.ancestors.any? do |ancestor|
            ancestor.name&.start_with?("PG::") && pg_error.include?(ancestor.name.split("::").last.to_sym)
          end

          cause = cause.cause
        end
        false
      end
    end
  end
  # rubocop:enable Metrics/ModuleLength, Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
end
