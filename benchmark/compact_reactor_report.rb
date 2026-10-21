# frozen_string_literal: true

require "json"
require_relative "workload"

# Retain measured extrema/quantiles and process counters, omit histogram buckets.
module CompactReactorReport
  module_function

  def compact(value)
    case value
    when Hash
      value.reject { |key, _| key == "buckets" }.transform_values { |entry| compact(entry) }
    when Array then value.map { |entry| compact(entry) }
    else value
    end
  end

  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
  # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def print_table(report)
    puts "| Workload | Processes | Cycles/s | CPU s | GC s | Lag p99/max ms | " \
         "Slack min s | Renewals/misses | Drain max s |"
    puts "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    report.fetch("cases").each do |entry|
      entry.fetch("results").each do |result|
        workers = result.fetch("workers")
        lag = workers.map { |worker| worker.fetch("pressure").fetch("reactor_lag") }
        redis = workers.map { |worker| worker.fetch("redis") }
        slack = redis.filter_map { |metrics| metrics.fetch("pressure").fetch("lease_renewal_slack")["min"] }.min
        values = [entry["scenario"], result["worker_count"], result["throughput_cycles_per_second"],
                  workers.sum { |worker| worker["cpu_seconds"] }.round(3),
                  workers.sum { |worker| worker["gc_seconds"] }.round(3),
                  "#{(lag.map { |h| h["p99"] }.max * 1000).round(1)}/" \
                  "#{(lag.map { |h| h["max"] }.max * 1000).round(1)}",
                  slack&.round(3),
                  "#{redis.sum { |metrics| metrics["renewal_successes"] }}/" \
                  "#{redis.sum { |metrics| metrics["renewal_deadline_misses"] }}",
                  workers.map { |worker| worker["drain_seconds"] }.max.round(3)]
        puts "| #{values.join(" | ")} |"
      end
    end
    print_resources(report)
  end

  def print_resources(report)
    puts "\n| Workload | Processes | Allocations | Peak RSS MiB | Sampled sockets | " \
         "Export drops | Parse/transform total s |"
    puts "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    report.fetch("cases").each do |entry|
      entry.fetch("results").each do |result|
        workers = result.fetch("workers")
        values = [entry["scenario"], result["worker_count"], workers.sum { |w| w["allocated_objects"] },
                  result["peak_worker_rss_megabytes"], workers.sum { |w| w["max_sampled_sockets"] },
                  workers.sum { |w| w.dig("telemetry", "dropped") || 0 },
                  %w[parse transform].map do |stage|
                    workers.sum { |w| w.dig("http", "pressure", stage, "sum") || 0 }.round(3)
                  end.join("/")]
        puts "| #{values.join(" | ")} |"
      end
      next unless entry["upstream"]

      stats = entry["upstream"]
      summary = stats.slice("requests", "completed", "connections", "max_active", "max_reactor_lag", "cpu_seconds")
      puts "\n#{entry["scenario"]} upstream: #{summary}\n"
    end
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength
  # rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
end

report = JSON.parse(File.read(ARGV.fetch(0)))
if ENV["ACP_BENCH_REPORT"]
  CapacityWorkload.write_report(benchmark: "reactor-responsiveness", **CompactReactorReport.compact(report)
    .reject { |key, _| key == "benchmark" }.transform_keys(&:to_sym))
else
  CompactReactorReport.print_table(report)
end
