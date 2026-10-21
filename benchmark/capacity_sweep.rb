# frozen_string_literal: true

require "json"
require "open3"
require_relative "workload"

# Independent one-variable comparisons, including both overlap and saturation.
base = {
  "ACP_BENCH_TENANTS" => "1000", "ACP_BENCH_DURATION" => "4", "ACP_BENCH_INTERVAL" => "0.1",
  "ACP_BENCH_SEED" => "20261001", "ACP_BENCH_LATENCY_MIN" => "0.01", "ACP_BENCH_LATENCY_MAX" => "0.03",
  "ACP_BENCH_PAYLOAD_BYTES" => "1024", "ACP_BENCH_FETCH_CONCURRENCY" => "16",
  "ACP_BENCH_RESOLVE_CONCURRENCY" => "4", "ACP_BENCH_INGEST_CONCURRENCY" => "4",
  "ACP_BENCH_PIPELINE_CAPACITY" => "16", "ACP_BENCH_RESOLVE_DELAY" => "0.002",
  "ACP_BENCH_INGEST_DELAY" => "0.01", "ACP_BENCH_ACKNOWLEDGE_DELAY" => "0.005",
  "ACP_BENCH_REPORT" => nil
}.freeze
cases = {
  baseline: {}, pipeline32: { "ACP_BENCH_PIPELINE_CAPACITY" => "32" },
  pipeline128: { "ACP_BENCH_PIPELINE_CAPACITY" => "128" },
  fetch8: { "ACP_BENCH_FETCH_CONCURRENCY" => "8" }, fetch32: { "ACP_BENCH_FETCH_CONCURRENCY" => "32" },
  resolve1: { "ACP_BENCH_RESOLVE_CONCURRENCY" => "1" },
  resolve8: { "ACP_BENCH_RESOLVE_CONCURRENCY" => "8" },
  ingest1: { "ACP_BENCH_INGEST_CONCURRENCY" => "1" },
  ingest8: { "ACP_BENCH_INGEST_CONCURRENCY" => "8" },
  overlap32: { "ACP_BENCH_INGEST_CONCURRENCY" => "8", "ACP_BENCH_PIPELINE_CAPACITY" => "32" },
  overlap128: { "ACP_BENCH_INGEST_CONCURRENCY" => "8", "ACP_BENCH_PIPELINE_CAPACITY" => "128" },
  heavy_tail: { "ACP_BENCH_TAIL_PROBABILITY" => "0.1", "ACP_BENCH_TAIL_MULTIPLIER" => "10" },
  byte_constrained: { "ACP_BENCH_RETAINED_BYTE_CAPACITY" => "262144" }
}
results = cases.map do |name, overrides|
  environment = base.merge(overrides)
  stdout, stderr, status = Open3.capture3(environment, "bundle", "exec", "rake", "benchmark:runtime")
  raise "#{name} failed: #{stderr}" unless status.success?

  report = JSON.parse(stdout)
  metrics = report.fetch("runtime_pressure")
  { name: name, environment: environment.compact, workload: report.fetch("workload"),
    windows: report.fetch("post_warmup_windows"), capacities: report.fetch("capacities"),
    peaks: metrics.select { |key, _| key.start_with?("max_") },
    byte_budget_waits: metrics.fetch("byte_budget_waits"),
    stage_seconds: metrics.fetch("pressure").to_h do |stage, histogram|
      [stage, { mean: histogram["sum"] / [histogram["count"], 1].max, p95: histogram["p95"] }]
    end,
    environment_details: report.slice("ruby", "dependencies", "platform", "os_release"),
    released: metrics.values_at("retained_bytes", "reserved_bytes", "active_cycles").all?(&:zero?) }
end
CapacityWorkload.write_report(benchmark: "capacity-independent-local-sweep", smoke_only: true, results: results)
