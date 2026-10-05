# frozen_string_literal: true

require "optparse"
require "async"
require_relative "../acp"

module Acp
  # Rails process entrypoint. It leaves process supervision and replica
  # management to the deployment environment.
  # rubocop:disable Metrics/ClassLength, Metrics/AbcSize, Metrics/MethodLength, Metrics/CyclomaticComplexity
  class Worker
    DEFAULTS = {
      "rails" => "config/environment",
      "redis-url" => ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0"),
      "lease-ttl" => "60",
      "discovery-interval" => ENV.fetch("ACP_DISCOVERY_INTERVAL", "60"),
      "drain-timeout" => ENV.fetch("ACP_DRAIN_TIMEOUT", "30")
    }.freeze

    def initialize(arguments, environment: ENV, output: $stdout, error: $stderr)
      @arguments = arguments.dup
      @environment = environment
      @output = output
      @error = error
    end

    def run
      @startup_complete = false
      options = parse_options
      return 0 if @help_requested

      boot_rails(options.fetch("rails"))
      require "acp/rails"
      require "acp/redis"
      configuration = configuration_for(options)
      coordinator = coordinator_for(options, configuration)
      runtime = Runtime.new(
        configuration: configuration,
        progress: coordinator,
        ownership: coordinator,
        drain_timeout: Float(options.fetch("drain-timeout")),
        on_error: method(:log_runtime_error)
      )
      @startup_complete = true
      begin
        run_runtime(runtime)
      ensure
        begin
          call_consumer_shutdown(options)
        ensure
          coordinator.close if coordinator.respond_to?(:close)
        end
      end
      0
    rescue StandardError => e
      phase = @startup_complete ? "runtime" : "startup"
      @error.puts("acp-worker #{phase} error: #{e.class}: #{e.message}")
      @startup_complete ? 1 : 78
    end

    private

    def parse_options
      options = DEFAULTS.merge(
        "program" => nil,
        "transaction-owner" => nil,
        "application" => nil,
        "environment" => nil,
        "namespace" => nil,
        "worker-id" => nil,
        "pipeline-capacity" => nil,
        "fetch-concurrency" => nil,
        "ingest-concurrency" => nil,
        "resolve-concurrency" => nil
      )
      options.each_key do |name|
        env_name = "ACP_#{name.tr("-", "_").upcase}"
        options[name] = @environment[env_name] if @environment.key?(env_name)
      end
      options["redis-url"] = @environment.fetch("REDIS_URL", options.fetch("redis-url"))
      parser = OptionParser.new do |flags|
        flags.banner = "Usage: acp-worker [options]"
        options.each_key do |name|
          flags.on("--#{name} VALUE", "#{name} (or ACP_#{name.tr("-", "_").upcase})") do |value|
            options[name] = value
          end
        end
        flags.on("-h", "--help", "Show this help") do
          @output.puts(flags)
          @help_requested = true
        end
      end
      parser.parse!(@arguments)
      raise OptionParser::ParseError, "unexpected arguments: #{@arguments.join(" ")}" unless @arguments.empty?
      return options if @help_requested

      %w[program transaction-owner].each do |required|
        if options[required].to_s.empty?
          raise ConfigurationError,
                "set --#{required} or ACP_#{required.tr("-", "_").upcase}"
        end
      end
      options
    end

    def boot_rails(path)
      require File.expand_path(path, Dir.pwd)
      return if defined?(::Rails) && ::Rails.respond_to?(:application) && ::Rails.application

      raise ConfigurationError, "Rails boot file did not initialize Rails.application"
    end

    def configuration_for(options)
      program = constantize(options.fetch("program"))
      base = program.configuration
      settings = {
        interval: base.interval,
        fetch_concurrency: integer_option(options, "fetch-concurrency", base.fetch_concurrency),
        ingest_concurrency: integer_option(options, "ingest-concurrency", base.ingest_concurrency),
        resolve_concurrency: integer_option(options, "resolve-concurrency", base.resolve_concurrency),
        pipeline_capacity: integer_option(options, "pipeline-capacity", base.pipeline_capacity),
        discovery_interval: Float(options.fetch("discovery-interval")),
        retry_cooldown: base.retry_cooldown
      }
      configured = Configuration.new(
        name: base.name,
        settings: settings,
        callbacks: base.callbacks,
        fetch_retry: base.fetch_retry,
        ingest_retry: base.ingest_retry
      )
      ::Acp::Rails.configuration_for(
        configured,
        transaction_owner: constantize(options.fetch("transaction-owner"))
      )
    end

    def coordinator_for(options, configuration)
      program_name = configuration.name
      application = options["application"] || options["namespace"] || rails_application_name
      environment = options["environment"] || @environment.fetch("RAILS_ENV", "development")
      worker_id = options["worker-id"] || "#{@environment.fetch("HOSTNAME", "worker")}-#{Process.pid}"
      ::Acp::RedisCoordinator.new(
        application: application,
        environment: environment,
        program: program_name,
        redis_url: options.fetch("redis-url"),
        worker_id: worker_id,
        lease_ttl: Float(options.fetch("lease-ttl"))
      )
    end

    def run_runtime(runtime)
      signal = nil
      %w[TERM INT].each { |name| Signal.trap(name) { signal = name } }
      Async do |root|
        runner = root.async { runtime.run }
        signal_watcher = root.async do
          loop do
            if signal
              runtime.request_shutdown
              break
            end
            Async::Task.current.sleep(0.1)
          end
        end
        runner.wait
        signal_watcher.stop
      end
    ensure
      %w[TERM INT].each { |name| Signal.trap(name, "DEFAULT") }
    end

    def call_consumer_shutdown(options)
      program = constantize(options.fetch("program"))
      return unless program.respond_to?(:acp_shutdown)

      ::Rails.application.executor.wrap { program.acp_shutdown }
    end

    def log_runtime_error(tenant_id, exception, stage)
      details = { stage: stage, tenant_id: tenant_id, error_class: exception.class.name }
      @error.puts("acp-worker event: #{details.inspect}")
    end

    def rails_application_name
      name = ::Rails.application.class.name
      name&.sub(/::Application\z/, "") || "rails-app"
    end

    def integer_option(options, name, fallback)
      options[name].nil? ? fallback : Integer(options.fetch(name))
    end

    def constantize(name)
      name.split("::").reject(&:empty?).inject(Object) { |scope, part| scope.const_get(part, false) }
    end
  end
  # rubocop:enable Metrics/ClassLength, Metrics/AbcSize, Metrics/MethodLength, Metrics/CyclomaticComplexity
end
