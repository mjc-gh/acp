# frozen_string_literal: true

require "json"
require "active_support"
require "active_support/notifications"
require_relative "../lib/acp/redis"

# Small seeded comparison with real Redis renewal and synthetic cooperative
# upstream latency. Every scenario uses the same tenants, payloads, and budgets.
# rubocop:disable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
class RetryIsolationBenchmark
  SEED = 20_261_021
  PIPELINE = 2
  RETRIES = 2
  INGEST = 1
  PAYLOAD_BYTES = 4096
  DURATION = 1.2

  def initialize(scenario, retry_capacity)
    @scenario = scenario
    @retry_capacity = retry_capacity
    @clock = Acp::MonotonicClock.new
    @observations = Acp::PressureMetrics.new(%i[healthy_admission healthy_completion], clock: @clock)
    @retry_attempts = 0
    @retry_successes = 0
    @peak_tasks = 0
    random = Random.new(SEED)
    @latencies = (1..20).to_h { |id| [id, 0.001 + random.rand * 0.003] }
    @coordinator = Acp::RedisCoordinator.new(
      application: "acp-retry-benchmark-#{Process.pid}-#{SecureRandom.hex(6)}",
      environment: "benchmark", program: "retry-isolation",
      redis_url: ENV.fetch("ACP_TEST_REDIS_URL"), lease_ttl: 3, operation_timeout: 0.2
    )
  end

  def healthy?(id)
    @scenario == :healthy || (@scenario == :mixed && id > 2)
  end

  def build_program
    benchmark = self
    Class.new(Acp::Program) do
      program_name "RetryIsolationBenchmark"
      interval 0.1
      pipeline_capacity PIPELINE
      retry_capacity benchmark.instance_variable_get(:@retry_capacity)
      fetch_concurrency PIPELINE
      ingest_concurrency INGEST
      coordination_timeout 0.2
      resolve_timeout 0.2
      initial_cursor_timeout 0.2
      retry_cooldown 0.1
      tenants { |emit| (1..20).each { |id| emit.call(id) } }
      initial_cursor { |_| Time.utc(2025, 1, 1) }
      resolve { |id| id }
      fetch { |id, context| benchmark.fetch(id, context) }
      fetch_retry on: IOError, max_attempts: 4, max_elapsed: 1.5, timeout: 0.2, backoff: 0.4, max_backoff: 0.4
      ingest { |*| Kernel.sleep(0.001) }
    end
  end

  def fetch(id, context)
    @retry_attempts += 1 if context.attempt > 1
    Kernel.sleep(@latencies.fetch(id))
    raise IOError if !healthy?(id) && (@scenario == :all_failed || context.attempt == 1)

    @retry_successes += 1 if context.attempt > 1
    Acp::Batch.new(data: ["x" * PAYLOAD_BYTES], next_cursor: context.cursor)
  end

  def task_count(node)
    count = 1
    node.children&.each { |child| count += task_count(child) }
    count
  end

  def run
    subscription = ActiveSupport::Notifications.subscribe(/\Apoll_(?:started|completed)\.acp\z/) do |name, *args|
      payload = args.last
      next unless healthy?(payload[:tenant_id])

      if name == "poll_started.acp"
        @observations.observe(:healthy_admission, payload[:attributes].fetch(:admission_lag))
      else
        @observations.observe(:healthy_completion, payload.fetch(:duration))
      end
    end
    runtime = Acp::Runtime.new(configuration: build_program.configuration,
                               progress: @coordinator, ownership: @coordinator)
    runtime.define_singleton_method(:initial_offset) { |_| 0 }
    drain_duration = nil
    Async do |root|
      runner = root.async { runtime.run }
      sampler = root.async do
        loop do
          @peak_tasks = [@peak_tasks, task_count(runner)].max
          root.sleep(0.001)
        end
      end
      root.sleep(DURATION)
      started = @clock.now
      runtime.request_shutdown(timeout: 0.05)
      runner.wait
      drain_duration = @clock.now - started
      sampler.stop
      sampler.wait
    end.wait
    metrics = runtime.metrics
    {
      scenario: @scenario, retry_capacity: @retry_capacity, seed: SEED,
      pipeline_capacity: PIPELINE, ingest_workers: INGEST, workload_seconds: DURATION,
      healthy: @observations.snapshot, retry_attempts: @retry_attempts, retry_successes: @retry_successes,
      completed_cycles: metrics[:completed_cycles], retry_overflows: metrics[:retry_overflows],
      peak_cycle_tasks: metrics[:max_active_cycles], peak_runtime_tasks_sampled: @peak_tasks,
      runtime_task_bound: 2 * (PIPELINE + @retry_capacity) + 2 * INGEST + 6,
      peak_retained_payload_bytes: metrics[:max_retained_batches] * PAYLOAD_BYTES,
      retained_payload_bound_bytes: (PIPELINE + @retry_capacity) * PAYLOAD_BYTES,
      peak_pipeline: metrics[:max_pipeline_reservations], peak_retries: metrics[:max_retry_reservations],
      renewal_slack: @coordinator.metrics[:pressure][:lease_renewal_slack],
      drain_seconds: drain_duration, ending_cycles: metrics[:active_cycles],
      ending_parked: metrics[:parked_cycles], ending_batches: metrics[:retained_batches]
    }
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription) if subscription
    @coordinator.close
    redis = Redis.new(url: ENV.fetch("ACP_TEST_REDIS_URL"))
    keys = redis.scan_each(match: "#{@coordinator.instance_variable_get(:@prefix)}:*").to_a
    redis.del(keys) unless keys.empty?
    redis.close
  end
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize

reports = %i[healthy mixed all_failed].flat_map do |scenario|
  [0, RetryIsolationBenchmark::RETRIES].map { |capacity| RetryIsolationBenchmark.new(scenario, capacity).run }
end
puts JSON.pretty_generate(ruby: RUBY_DESCRIPTION, reports: reports)
