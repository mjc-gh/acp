# frozen_string_literal: true

ENV["RAILS_ENV"] = "test"
require "rails"
require "active_record"
require_relative "../test/rails_app/config/application"
require_relative "../examples/versioned_importer"
require_relative "../test/fixtures/reliability_upstream"
require_relative "../lib/acp"
require_relative "../lib/acp/rails"
require_relative "../lib/acp/redis"
require "json"
require_relative "../benchmark/workload"
require_relative "../benchmark/http_workload"
require_relative "../benchmark/postgres_pool_metrics"

# Consumer-owned table model used for the multi-process service benchmark.
class AcpReliabilityBenchmarkRecord < ActiveRecord::Base
  self.table_name = "acp_reliability_benchmark_records"
end

AcpRailsTestApp::Application.initialize!
ActiveRecord::Base.connection_pool.singleton_class.prepend(PostgresPoolMetrics)

worker_id = ENV.fetch("ACP_BENCH_WORKER_ID")
tenant_count = Integer(ENV.fetch("ACP_BENCH_TENANTS", "30000"))
start_file = ENV.fetch("ACP_BENCH_START_FILE")
upstream = ReliabilityUpstream.new(
  seed: Integer(ENV.fetch("ACP_BENCH_SEED", "20261001")),
  latency: [Float(ENV.fetch("ACP_BENCH_LATENCY_MIN", "1")),
            Float(ENV.fetch("ACP_BENCH_LATENCY_MAX", "19"))],
  payload_size: Integer(ENV.fetch("ACP_BENCH_PAYLOAD_BYTES", "1024")),
  **CapacityWorkload.upstream_options
)
if ENV["ACP_BENCH_HTTP_URL"]
  upstream = HttpWorkload.new(url: ENV.fetch("ACP_BENCH_HTTP_URL"),
                              capacity: CapacityWorkload.integer("FETCH_CONCURRENCY", 3500),
                              ca_file: ENV["ACP_BENCH_HTTP_CA_FILE"],
                              blocking_parse: CapacityWorkload.number("BLOCKING_PARSE", 0))
end
program = Class.new(Acp::Program) do
  define_singleton_method(:name) { "AcpReliabilityServiceBenchmark" }
  interval(Float(ENV.fetch("ACP_BENCH_INTERVAL", "60")))
  fetch_concurrency(Integer(ENV.fetch("ACP_BENCH_FETCH_CONCURRENCY", "3500")))
  ingest_concurrency(Integer(ENV.fetch("ACP_BENCH_INGEST_CONCURRENCY", "16")))
  pipeline_capacity(Integer(ENV.fetch("ACP_BENCH_PIPELINE_CAPACITY", "4500")))
  resolve_concurrency(Integer(ENV.fetch("ACP_BENCH_RESOLVE_CONCURRENCY", "16")))
  tenants { |emit| (1..tenant_count).each { |tenant_id| emit.call(tenant_id) } }
  initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
  resolve { |tenant_id| tenant_id }
  fetch do |tenant_id, context|
    options = { tenant_id: tenant_id, cursor: context.cursor }
    options[:context] = context if upstream.is_a?(HttpWorkload)
    upstream.fetch(**options)
  end
  ingest do |batch, _context|
    Kernel.sleep(CapacityWorkload.number("INGEST_DELAY", 0))
    records = batch.data.map do |record|
      {
        external_id: record.fetch(:tenant_id).to_s,
        source_updated_at: record.fetch(:updated_at),
        value: record.fetch(:payload)
      }
    end
    VersionedImporter.call(model: AcpReliabilityBenchmarkRecord, records: records)
  end
end
CapacityWorkload.configure(program)
configuration = Acp::Rails.configuration_for(program, transaction_owner: ActiveRecord::Base)
coordinator = Acp::RedisCoordinator.new(
  application: ENV.fetch("ACP_BENCH_APPLICATION"),
  environment: "benchmark",
  program: configuration.name,
  redis_url: ENV.fetch("ACP_TEST_REDIS_URL"),
  worker_id: worker_id,
  lease_ttl: CapacityWorkload.number("LEASE_TTL", 9)
)
exporter = if ENV["ACP_BENCH_EXPORT_DELAY"]
             Acp::BoundedExporter.new(capacity: 16) { |_| sleep(CapacityWorkload.number("EXPORT_DELAY", 0)) }
           end
if ENV["ACP_BENCH_BLOCKING_NOTIFICATION"]
  ActiveSupport::Notifications.subscribe(/\.acp\z/) do
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + CapacityWorkload.number("BLOCKING_NOTIFICATION", 0)
    nil while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  end
end
runtime = Acp::Runtime.new(configuration: configuration, progress: coordinator, ownership: coordinator,
                           exporter: exporter)
successful_tenants = Hash.new(0)
coordinator.define_singleton_method(:advance) do |tenant_id, *args, **options|
  Kernel.sleep(CapacityWorkload.number("ACKNOWLEDGE_DELAY", 0))
  result = super(tenant_id, *args, **options)
  successful_tenants[tenant_id] += 1
  result
end
shutdown_signal = nil
%w[TERM INT].each { |signal| Signal.trap(signal) { shutdown_signal = signal } }
$stdout.sync = true
process_started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
sample_file = File.open(ENV.fetch("ACP_BENCH_SAMPLE_PATH"), "a")
puts JSON.generate(booted: true, worker_id: worker_id)
started_at = nil
baseline_completed = nil
baseline_fetches = nil
baseline_payload_bytes = nil
discovery_seconds = nil
measurement_completed = nil
measurement_tenants = nil
measurement_process = nil
baseline_process = nil

# Startup, sampling, and bounded shutdown are kept together for this subprocess.
# rubocop:disable Metrics/BlockLength
Async do |root|
  runner = root.async { runtime.run }
  root.sleep(0.05) while runtime.metrics.fetch(:discovered_tenants) < tenant_count
  discovery_seconds = Process.clock_gettime(Process::CLOCK_MONOTONIC) - process_started_at
  puts JSON.generate(discovered: true, worker_id: worker_id, discovery_seconds: discovery_seconds)
  root.sleep(0.05) until File.exist?(start_file)
  started_at = Float(File.read(start_file))
  until (delay = started_at - Process.clock_gettime(Process::CLOCK_MONOTONIC)) <= 0
    root.sleep(delay)
  end
  baseline_completed = runtime.metrics.fetch(:completed_cycles)
  baseline_fetches = upstream.calls
  baseline_payload_bytes = upstream.payload_bytes
  baseline_process = CapacityWorkload.process_metrics
  successful_tenants.clear
  sampler = root.async do
    duration = CapacityWorkload.number("DURATION", 160)
    sample_times = ([0] + (1..(duration / 5).floor).map { |i| i * 5.0 } +
                    CapacityWorkload.boundaries(duration)).uniq.sort
    sample_times.each do |at|
      root.sleep([started_at + at - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max)
      rss = File.read("/proc/self/status").match(/^VmRSS:\s+(\d+)\s+kB$/)[1].to_i / 1024.0
      snapshot = runtime.metrics
      completed = snapshot.fetch(:completed_cycles) - baseline_completed
      sample_file.puts(
        JSON.generate(
          {
            at_seconds: at,
            actual_at_seconds: Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at,
            completed_cycles: completed,
            active_cycles: snapshot.fetch(:active_cycles),
            queued_batches: snapshot.fetch(:queued_batches),
            overdue_tenants: snapshot.fetch(:overdue_tenants),
            oldest_overdue_age: snapshot.fetch(:oldest_overdue_age),
            pressure: snapshot.fetch(:pressure),
            redis: snapshot[:redis],
            retained_bytes: snapshot[:retained_bytes],
            reserved_bytes: snapshot[:reserved_bytes],
            postgres_pool: ActiveRecord::Base.connection_pool.stat,
            postgres_checkouts: ActiveRecord::Base.connection_pool.checkout_metrics.dup,
            rss_megabytes: rss,
            process: CapacityWorkload.process_metrics, http: (upstream.metrics if upstream.is_a?(HttpWorkload))
          }
        )
      )
      sample_file.flush
    end
    measurement_completed = runtime.metrics[:completed_cycles] - baseline_completed
    measurement_tenants = successful_tenants.dup
    measurement_process = CapacityWorkload.process_metrics
    runtime.request_shutdown(timeout: 10)
  end
  watcher = root.async do
    loop do
      if shutdown_signal
        runtime.request_shutdown(timeout: 10)
        break
      end
      root.sleep(0.05)
    end
  end
  puts JSON.generate(ready: true, worker_id: worker_id, start_at: started_at)
  runner.wait
  sampler.stop
  watcher.stop
end
# rubocop:enable Metrics/BlockLength
metrics = runtime.metrics
exporter&.close(timeout: 0.1)
upstream.close if upstream.respond_to?(:close)
puts JSON.generate(worker_id: worker_id, metrics: metrics,
                   postgres_pool: ActiveRecord::Base.connection_pool.stat,
                   postgres_checkouts: ActiveRecord::Base.connection_pool.checkout_metrics,
                   stage_residence_seconds: CapacityWorkload.stage_times(metrics),
                   process: CapacityWorkload.process_metrics,
                   baseline_process: baseline_process, measurement_process: measurement_process,
                   http: (upstream.metrics if upstream.is_a?(HttpWorkload)),
                   drain_seconds: Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at -
                     CapacityWorkload.number("DURATION", 160),
                   benchmark_completed_cycles: metrics.fetch(:completed_cycles) - baseline_completed,
                   measurement_completed_cycles: measurement_completed,
                   successful_tenant_cycles: measurement_tenants,
                   baseline_completed_cycles: baseline_completed,
                   upstream_fetches: upstream.calls - baseline_fetches,
                   payload_bytes: upstream.payload_bytes - baseline_payload_bytes,
                   startup_discovery_seconds: discovery_seconds,
                   elapsed_seconds: Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at,
                   latency_seconds: {
                     p50_bucket_upper_bound: upstream.latency_percentile(0.50),
                     p95_bucket_upper_bound: upstream.latency_percentile(0.95),
                     p99_bucket_upper_bound: upstream.latency_percentile(0.99)
                   })
sample_file.close
