# frozen_string_literal: true

require "json"
require "rbconfig"
require_relative "../test/fixtures/reliability_upstream"
require_relative "../lib/acp"

# In-memory progress adapter used only by the local scheduler benchmark.
class BenchmarkProgress
  State = Struct.new(:cursor, :revision, :poll_id, keyword_init: true)

  def initialize
    @states = {}
  end

  def read_state(tenant_id)
    @states[tenant_id]
  end

  def initialize_state(tenant_id, cursor)
    @states[tenant_id] ||= State.new(cursor: cursor, revision: 0).freeze
  end

  def advance(tenant_id, poll_id, cursor, expected_revision:, **_options)
    state = @states.fetch(tenant_id)
    return true if state.poll_id == poll_id
    raise "progress revision conflict" unless state.revision == expected_revision

    @states[tenant_id] = State.new(cursor: cursor, revision: state.revision + 1, poll_id: poll_id).freeze
    true
  end
end

tenant_count = Integer(ENV.fetch("ACP_BENCH_TENANTS", "30000"))
duration = Float(ENV.fetch("ACP_BENCH_DURATION", "70"))
interval = Float(ENV.fetch("ACP_BENCH_INTERVAL", "60"))
fetch_capacity = Integer(ENV.fetch("ACP_BENCH_FETCH_CONCURRENCY", "100"))
ingest_capacity = Integer(ENV.fetch("ACP_BENCH_INGEST_CONCURRENCY", "16"))
pipeline_capacity = Integer(ENV.fetch("ACP_BENCH_PIPELINE_CAPACITY", "256"))
latency = [Float(ENV.fetch("ACP_BENCH_LATENCY_MIN", "0.001")),
           Float(ENV.fetch("ACP_BENCH_LATENCY_MAX", "0.01"))]
payload_size = Integer(ENV.fetch("ACP_BENCH_PAYLOAD_BYTES", "1024"))
seed = Integer(ENV.fetch("ACP_BENCH_SEED", "20261001"))
raise ArgumentError, "tenant count must be positive" unless tenant_count.positive?
raise ArgumentError, "duration must be positive" unless duration.positive?

tenants = (1..tenant_count).to_a.freeze
upstream = ReliabilityUpstream.new(seed: seed, latency: latency, payload_size: payload_size)
program = Class.new(Acp::Program) do
  define_singleton_method(:name) { "AcpReliabilityBenchmark" }
  interval(interval)
  fetch_concurrency(fetch_capacity)
  ingest_concurrency(ingest_capacity)
  pipeline_capacity(pipeline_capacity)
  tenants { |emit| tenants.each { |tenant_id| emit.call(tenant_id) } }
  initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
  resolve { |tenant_id| tenant_id }
  fetch { |tenant_id, context| upstream.fetch(tenant_id: tenant_id, cursor: context.cursor) }
  ingest { |_batch, _context| }
end
progress = BenchmarkProgress.new
runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress)
initial_rss = File.read("/proc/self/status").match(/^VmRSS:\s+(\d+)\s+kB$/)[1].to_i / 1024.0
peak_rss = initial_rss
started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

Async do |root|
  runner = root.async { runtime.run }
  sampler = root.async do
    loop do
      rss = File.read("/proc/self/status").match(/^VmRSS:\s+(\d+)\s+kB$/)[1].to_i / 1024.0
      peak_rss = [peak_rss, rss].max
      Async::Task.current.sleep(0.1)
    end
  end
  root.sleep(duration)
  runtime.request_shutdown(timeout: 10)
  runner.wait
  sampler.stop
end

elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
metrics = runtime.metrics
completed = metrics.fetch(:completed_cycles)
ending_rss = File.read("/proc/self/status").match(/^VmRSS:\s+(\d+)\s+kB$/)[1].to_i / 1024.0
os_release = File.foreach("/etc/os-release").find { |line| line.start_with?("PRETTY_NAME=") }&.split("=", 2)&.last
report = {
  benchmark: "local-runtime",
  seed: seed,
  ruby: RUBY_DESCRIPTION,
  dependencies: %w[async redis rails pg].to_h do |name|
    spec = Gem.loaded_specs[name]
    [name, spec&.version&.to_s]
  end,
  platform: RbConfig::CONFIG.fetch("host"),
  os_release: os_release&.delete_prefix("\"")&.delete_suffix("\""),
  tenant_count: tenant_count,
  capacities: {
    fetch: fetch_capacity,
    ingest: ingest_capacity,
    pipeline: pipeline_capacity
  },
  duration_seconds: elapsed.round(3),
  configured_latency_seconds: latency,
  measured_latency_seconds: {
    p50_bucket_upper_bound: upstream.latency_percentile(0.50),
    p95_bucket_upper_bound: upstream.latency_percentile(0.95),
    p99_bucket_upper_bound: upstream.latency_percentile(0.99)
  },
  payload_bytes_per_record: payload_size,
  payload_bytes_total: upstream.payload_bytes,
  fetches: upstream.calls,
  completed_cycles: completed,
  throughput_cycles_per_second: (completed / elapsed).round(2),
  polling_lag_seconds_bucket_upper_bounds: metrics.slice(
    :polling_lag_p50, :polling_lag_p95, :polling_lag_p99
  ),
  max_active_cycles: metrics.fetch(:max_active_cycles),
  max_active_fetches: metrics.fetch(:max_active_fetches),
  max_queued_batches: metrics.fetch(:max_queued_batches),
  max_active_ingests: metrics.fetch(:max_active_ingests),
  initial_rss_megabytes: initial_rss.round(2),
  peak_rss_megabytes: peak_rss.round(2),
  ending_rss_megabytes: ending_rss.round(2)
}
puts JSON.pretty_generate(report)
