# frozen_string_literal: true

require "open3"
require "tempfile"
require "timeout"
require "json"
require "net/http"
require_relative "workload"

# Small measured baselines by default; inherit ACP_BENCH_* to run the full load.
# rubocop:disable Metrics/ModuleLength
module ReactorResponsiveness
  CASES = {
    "idle" => { "INTERVAL" => "3600" },
    "synthetic" => {},
    "membership" => { "TENANTS" => "30000", "INTERVAL" => "3600" },
    "http_tls" => { "HTTP" => "1" },
    "large_payload" => { "HTTP" => "1", "HTTP_PAYLOAD" => "262144" },
    "blocking_parse" => { "HTTP" => "1", "HTTP_LATENCY" => "0", "BLOCKING_PARSE" => "0.05" },
    "blocking_telemetry" => { "BLOCKING_NOTIFICATION" => "0.02" },
    "bounded_telemetry" => { "EXPORT_DELAY" => "0.02" }
  }.freeze

  module_function

  def fixture_metrics(url, ca_file)
    uri = URI.join(url, "metrics")
    http = Net::HTTP.new(uri.host, uri.port, nil)
    http.use_ssl = uri.scheme == "https"
    http.ca_file = ca_file
    http.read_timeout = 5
    JSON.parse(http.start { |client| client.get(uri.request_uri).body })
  end

  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
  # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def summarize(result)
    result.slice("worker_count", "throughput_cycles_per_second", "capacities_per_worker",
                 "fleet_connection_budgets", "peak_worker_rss_megabytes").merge(
                   responsiveness_gates: gates(result.fetch("workers")),
                   workers: result.fetch("workers").map do |worker|
                     baseline = worker.fetch("baseline_process")
                     measured = worker.fetch("measurement_process")
                     metrics = worker.fetch("metrics")
                     { cpu_seconds: %w[cpu_user_seconds cpu_system_seconds].sum { |key| measured[key] - baseline[key] },
                       allocated_objects: measured["gc"]["total_allocated_objects"] -
                         baseline["gc"]["total_allocated_objects"],
                       gc_seconds: measured["gc_total_seconds"] - baseline["gc_total_seconds"],
                       max_sampled_sockets: worker.fetch("samples").map { |s| s["process"]["sockets"] }.max,
                       nofile_limit: measured["nofile_limit"], drain_seconds: worker["drain_seconds"],
                       completed_cycles: worker["measurement_completed_cycles"],
                       pressure: metrics.fetch("pressure").slice("reactor_lag", "renewal_wakeup_lag",
                                                                 "notification_service", "admission_lag"),
                       redis: metrics["redis"], http: worker["http"], telemetry: metrics["telemetry"],
                       released: %w[active_cycles pipeline_reservations reserved_bytes retained_bytes
                                    active_fetches active_ingests queued_batches].all? { |key| metrics[key].zero? } }
                   end
                 )
  end

  def gates(workers)
    metrics = workers.map { |worker| worker.fetch("metrics") }
    {
      reactor_p99: metrics.all? { |m| (m.dig("pressure", "reactor_lag", "p99") || Float::INFINITY) <= 0.025 },
      reactor_max: metrics.all? { |m| (m.dig("pressure", "reactor_lag", "max") || 0) <= 0.15 },
      renewal_slack: metrics.all? { |m| (m.dig("redis", "pressure", "lease_renewal_slack", "min") || 3) >= 1.5 },
      no_misses: metrics.all? do |m|
        m["renewal_deadline_misses"].zero? && m.dig("redis", "renewal_deadline_misses").zero?
      end,
      drain: workers.all? { |worker| worker["drain_seconds"] <= 10.3 }
    }
  end

  def run_case(name, options)
    process = output = error = nil
    environment = {
      "ACP_BENCH_TENANTS" => "100", "ACP_BENCH_DURATION" => "10", "ACP_BENCH_INTERVAL" => "0.01",
      "ACP_BENCH_LATENCY_MIN" => "1.1", "ACP_BENCH_LATENCY_MAX" => "1.1",
      "ACP_BENCH_FETCH_CONCURRENCY" => "32", "ACP_BENCH_PIPELINE_CAPACITY" => "48",
      "ACP_BENCH_INGEST_CONCURRENCY" => "4", "ACP_BENCH_RESOLVE_CONCURRENCY" => "4",
      "ACP_BENCH_MAX_BATCH_BYTES" => "1048576", "ACP_BENCH_MAX_RESPONSE_BYTES" => "1048576",
      "ACP_BENCH_LEASE_TTL" => "3", "ACP_BENCH_REQUIRE_RELEASE" => "0",
      "ACP_BENCH_SUMMARY" => "1"
    }.merge(ENV.to_h).merge(options.to_h { |key, value| ["ACP_BENCH_#{key}", value] })
    Tempfile.create(["acp-reactor-ca", ".pem"]) do |certificate|
      if options["HTTP"]
        fixture_env = { "ACP_HTTP_TLS" => "1", "ACP_HTTP_LATENCY" => options.fetch("HTTP_LATENCY", "1.1"),
                        "ACP_HTTP_PAYLOAD_BYTES" => options.fetch("HTTP_PAYLOAD", "1024") }
        script = File.expand_path("http_upstream.rb", __dir__)
        input, output, error, process = Open3.popen3(fixture_env, RbConfig.ruby, script)
        input.close
        fixture = Timeout.timeout(10) { JSON.parse(output.gets) }
        certificate.write(fixture.fetch("certificate"))
        certificate.flush
        environment["ACP_BENCH_HTTP_URL"] = fixture.fetch("url")
        environment["ACP_BENCH_HTTP_CA_FILE"] = certificate.path
      end
      Tempfile.create(["acp-reactor-report", ".json"]) do |report|
        environment["ACP_BENCH_REPORT"] = report.path
        _, stderr, status = Open3.capture3(environment, RbConfig.ruby,
                                           File.expand_path("../integration/benchmark_services.rb", __dir__))
        raise "#{name} failed: #{stderr}" unless status.success?

        full = JSON.parse(File.read(report.path))
        { scenario: name, duration: environment["ACP_BENCH_DURATION"], workload: full["workload"],
          host: full.slice("ruby", "available_processors", "cgroup_cpu_limit", "cgroup_memory_limit_bytes",
                           "dependencies", "redis_server", "postgres_server"),
          upstream: (fixture_metrics(fixture.fetch("url"), certificate.path) if fixture),
          results: full.fetch("results").map { |result| summarize(result) } }
      end
    end
  ensure
    if process
      Process.kill("TERM", process.pid) if process.alive?
      unless process.join(5)
        Process.kill("KILL", process.pid)
        process.join
      end
    end
    output&.close
    error&.close
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength
  # rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

  def run
    names = ENV.fetch("ACP_REACTOR_CASES", CASES.keys.join(",")).split(",")
    reports = names.map do |name|
      warn "Measuring #{name}"
      run_case(name, CASES.fetch(name))
    end
    CapacityWorkload.write_report(benchmark: "reactor-responsiveness", cases: reports)
  end
end
# rubocop:enable Metrics/ModuleLength

ReactorResponsiveness.run if $PROGRAM_NAME == __FILE__
