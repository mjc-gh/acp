# frozen_string_literal: true

module Acp
  # Class-level DSL for declaring one tenant polling program.
  class Program
    SETTING_NAMES = %i[interval fetch_concurrency ingest_concurrency pipeline_capacity].freeze
    CALLBACK_NAMES = %i[tenants initial_cursor resolve fetch ingest].freeze

    class << self
      def inherited(subclass)
        super
        subclass.instance_variable_set(:@settings, settings.dup)
        subclass.instance_variable_set(:@callbacks, callbacks.dup)
        subclass.instance_variable_set(:@program_name, @program_name)
        subclass.instance_variable_set(:@fetch_retry, @fetch_retry)
        subclass.instance_variable_set(:@ingest_retry, @ingest_retry)
      end

      SETTING_NAMES.each do |setting|
        define_method(setting) do |value = :__acp_missing__|
          return settings.fetch(setting) if value == :__acp_missing__

          settings[setting] = value
        end
      end

      CALLBACK_NAMES.each do |callback_name|
        define_method(callback_name) do |&block|
          raise DefinitionError, "#{callback_name} requires a block" unless block

          callbacks[callback_name] = block
        end
      end

      def program_name(value = :__acp_missing__)
        return @program_name || name unless value == :__acp_missing__

        @program_name = value
      end

      def fetch_retry(policy = nil, **options)
        @fetch_retry = retry_policy(policy, options)
      end

      def ingest_retry(policy = nil, **options)
        @ingest_retry = retry_policy(policy, options)
      end

      def configuration
        effective_name = @program_name || name
        Configuration.new(
          name: effective_name,
          settings: settings,
          callbacks: callbacks,
          fetch_retry: @fetch_retry || RetryPolicy.new,
          ingest_retry: @ingest_retry || RetryPolicy.new
        )
      end

      alias validate! configuration

      private

      def settings
        @settings ||= {}
      end

      def callbacks
        @callbacks ||= {}
      end

      def retry_policy(policy, options)
        raise DefinitionError, "provide a RetryPolicy or retry options, not both" if policy && !options.empty?
        raise DefinitionError, "retry policy must be an Acp::RetryPolicy" if policy && !policy.is_a?(RetryPolicy)

        policy || RetryPolicy.new(**options)
      end
    end
  end
end
