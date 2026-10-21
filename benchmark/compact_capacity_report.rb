# frozen_string_literal: true

require "json"
require_relative "workload"

# Retain stage distributions, per-worker peaks, and equal-window evidence without
# committing every tenant ID or every histogram bucket from the raw service run.
report = JSON.parse(File.read(ARGV.fetch(0)))
report.fetch("results").each do |result|
  result.fetch("workers").each do |worker|
    coverage = worker.delete("successful_tenant_cycles").values
    worker["tenant_coverage"] = { count: coverage.length, minimum_cycles: coverage.min, maximum_cycles: coverage.max }
    duration = result.fetch("measurement_duration_seconds")
    windows = result.fetch("post_warmup_windows")
    boundaries = [0, duration - windows.sum { |window| window.fetch("duration_seconds") },
                  duration - windows.last.fetch("duration_seconds"), duration]
    worker["sample_count"] = worker.fetch("samples").length
    worker["sampled_postgres_peak_busy"] = worker.fetch("samples").filter_map do |sample|
      sample.dig("postgres_pool", "busy")
    end.max
    worker["samples"].select! { |sample| boundaries.include?(sample.fetch("at_seconds")) }
    worker["post_warmup_stage_residence_seconds"] = worker["samples"].last(3).each_cons(2).map do |left, right|
      right.fetch("pressure").to_h do |stage, histogram|
        previous = left.fetch("pressure").fetch(stage)
        count = histogram.fetch("count") - previous.fetch("count")
        [stage, { count: count, mean: (histogram.fetch("sum") - previous.fetch("sum")) / [count, 1].max }]
      end
    end
    worker["samples"].each do |sample|
      sample.delete("pressure")
      sample.delete("redis") # Final coordinator metrics below retain connection and renewal peaks.
    end
    worker.fetch("metrics").fetch("pressure").each_value { |histogram| histogram.delete("buckets") }
  end
end
CapacityWorkload.write_report(report.transform_keys(&:to_sym))
