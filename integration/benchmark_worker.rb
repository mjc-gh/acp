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

# Consumer-owned table model used for the multi-process service benchmark.
class AcpReliabilityBenchmarkRecord < ActiveRecord::Base
  self.table_name = "acp_reliability_benchmark_records"
end

AcpRailsTestApp::Application.initialize!

worker_id = ENV.fetch("ACP_BENCH_WORKER_ID")
tenant_count = Integer(ENV.fetch("ACP_BENCH_TENANTS", "30000"))
start_file = ENV.fetch("ACP_BENCH_START_FILE")
upstream = ReliabilityUpstream.new(
  seed: Integer(ENV.fetch("ACP_BENCH_SEED", "20261001")),
  latency: [Float(ENV.fetch("ACP_BENCH_LATENCY_MIN", "1")),
            Float(ENV.fetch("ACP_BENCH_LATENCY_MAX", "19"))],
  payload_size: Integer(ENV.fetch("ACP_BENCH_PAYLOAD_BYTES", "1024"))
)
program = Class.new(Acp::Program) do
  define_singleton_method(:name) { "AcpReliabilityServiceBenchmark" }
  interval(Float(ENV.fetch("ACP_BENCH_INTERVAL", "60")))
  fetch_concurrency(Integer(ENV.fetch("ACP_BENCH_FETCH_CONCURRENCY", "2500")))
  ingest_concurrency(Integer(ENV.fetch("ACP_BENCH_INGEST_CONCURRENCY", "16")))
  pipeline_capacity(Integer(ENV.fetch("ACP_BENCH_PIPELINE_CAPACITY", "2500")))
  resolve_concurrency(Integer(ENV.fetch("ACP_BENCH_RESOLVE_CONCURRENCY", "16")))
  tenants { |emit| (1..tenant_count).each { |tenant_id| emit.call(tenant_id) } }
  initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
  resolve { |tenant_id| tenant_id }
  fetch { |tenant_id, context| upstream.fetch(tenant_id: tenant_id, cursor: context.cursor) }
  ingest do |batch, _context|
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
configuration = Acp::Rails.configuration_for(program, transaction_owner: ActiveRecord::Base)
coordinator = Acp::RedisCoordinator.new(
  application: ENV.fetch("ACP_BENCH_APPLICATION"),
  environment: "benchmark",
  program: configuration.name,
  redis_url: ENV.fetch("ACP_TEST_REDIS_URL"),
  worker_id: worker_id,
  lease_ttl: 9
)
runtime = Acp::Runtime.new(configuration: configuration, progress: coordinator, ownership: coordinator)
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
  sampler = root.async do
    loop do
      root.sleep(5)
      rss = File.read("/proc/self/status").match(/^VmRSS:\s+(\d+)\s+kB$/)[1].to_i / 1024.0
      completed = runtime.metrics.fetch(:completed_cycles) - baseline_completed
      sample_file.puts(
        JSON.generate(
          {
            at_seconds: Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at,
            completed_cycles: completed,
            active_cycles: runtime.metrics.fetch(:active_cycles),
            queued_batches: runtime.metrics.fetch(:queued_batches),
            rss_megabytes: rss
          }
        )
      )
      sample_file.flush
    end
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
puts JSON.generate(worker_id: worker_id, metrics: metrics,
                   benchmark_completed_cycles: metrics.fetch(:completed_cycles) - baseline_completed,
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
