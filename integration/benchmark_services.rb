# frozen_string_literal: true

require "open3"
require "rbconfig"
require "json"
require "timeout"
require "base64"
require "securerandom"
require "tmpdir"
require "fileutils"
require "etc"
require_relative "../benchmark/workload"
require_relative "../lib/acp"
ENV["RAILS_ENV"] = "test"
require "rails"
require "redis"
require "active_record"
require_relative "../test/rails_app/config/application"

AcpRailsTestApp::Application.initialize!

# Model for the benchmark's consumer-owned destination table.
class AcpReliabilityBenchmarkRecord < ActiveRecord::Base
  self.table_name = "acp_reliability_benchmark_records"
end

TENANT_COUNT = Integer(ENV.fetch("ACP_BENCH_TENANTS", "30000"))
DURATION = Float(ENV.fetch("ACP_BENCH_DURATION", "160"))
WORKER_COUNTS = ENV.fetch("ACP_BENCH_WORKERS", "1,2").split(",").map { |value| Integer(value) }.freeze
APPLICATION = "acp-benchmark-#{Process.pid}-#{SecureRandom.hex(5)}".freeze
WORKER_SCRIPT = File.expand_path("benchmark_worker.rb", __dir__)

def read_worker_message(child, timeout: 300)
  Timeout.timeout(timeout) { JSON.parse(child.fetch(:stdout).gets || "{}") }
end

def cgroup_limit(path)
  return unless File.file?(path)

  value = File.read(path).strip
  return if value == "max"

  value
end

def os_release
  entry = File.foreach("/etc/os-release").find { |line| line.start_with?("PRETTY_NAME=") }
  entry&.split("=", 2)&.last&.delete("\"")&.strip
rescue Errno::ENOENT
  nil
end

raise ArgumentError, "tenant count must be positive" unless TENANT_COUNT.positive?
raise ArgumentError, "duration must be at least 10 seconds for sampled steady-state reporting" if DURATION < 10
raise ArgumentError, "worker counts must be positive" unless WORKER_COUNTS.all?(&:positive?)
raise "ACP_TEST_REDIS_URL is required" unless ENV.key?("ACP_TEST_REDIS_URL")
raise "ACP_TEST_DATABASE_URL is required" unless ENV.key?("ACP_TEST_DATABASE_URL")

connection = ActiveRecord::Base.connection
connection.drop_table(:acp_reliability_benchmark_records, if_exists: true)
connection.create_table(:acp_reliability_benchmark_records) do |table|
  table.string :external_id, null: false
  table.datetime :source_updated_at, null: false, precision: 6
  table.text :value, null: false
end
connection.add_index(
  :acp_reliability_benchmark_records,
  :external_id,
  unique: true,
  name: "index_acp_benchmark_records_on_external_id"
)
redis = Redis.new(url: ENV.fetch("ACP_TEST_REDIS_URL"))
summary = []
sample_directory = Dir.mktmpdir("acp-benchmark-samples")

# This executable deliberately coordinates several services and worker
# lifecycles in one place; the inner accounting mirrors the emitted JSON report.
# rubocop:disable Metrics/BlockLength
begin
  WORKER_COUNTS.each do |worker_count|
    run_application = "#{APPLICATION}-#{worker_count}"
    start_file = File.join(sample_directory, "start-at-#{worker_count}")
    children = []
    rss_by_pid = Hash.new(0.0)
    initial_rss_by_pid = {}
    ending_rss_by_pid = {}
    begin
      pipeline = CapacityWorkload.integer("PIPELINE_CAPACITY", 4500)
      resolve = CapacityWorkload.integer("RESOLVE_CONCURRENCY", 16)
      ingest = CapacityWorkload.integer("INGEST_CONCURRENCY", 16)
      acp_connections = [pipeline, resolve + ingest].min + 1
      pool_headroom = CapacityWorkload.integer("POOL_HEADROOM", 4)
      raise ArgumentError, "pool headroom must be nonnegative" if pool_headroom.negative?

      pool_size = Integer(ENV.fetch("ACP_TEST_POOL", (acp_connections + pool_headroom).to_s))
      worker_count.times do |index|
        environment = ENV.to_h.merge(
          "ACP_BENCH_APPLICATION" => run_application,
          "ACP_BENCH_WORKER_ID" => "bench-#{worker_count}-#{index + 1}",
          "ACP_BENCH_START_FILE" => start_file,
          "ACP_BENCH_SAMPLE_PATH" => File.join(sample_directory, "#{worker_count}-#{index + 1}.jsonl"),
          "ACP_TEST_POOL" => pool_size.to_s
        )
        stdin, stdout, stderr, wait_thread = Open3.popen3(
          environment, RbConfig.ruby, "-Ilib", WORKER_SCRIPT
        )
        stdin.close
        child = { pid: wait_thread.pid, stdout: stdout, stderr: stderr, process: wait_thread }
        booted = read_worker_message(child)
        raise "benchmark worker failed to boot: #{booted.inspect}" unless booted["booted"]

        children << child
      end

      children.each do |child|
        discovered = read_worker_message(child)
        raise "benchmark worker failed discovery: #{discovered.inspect}" unless discovered["discovered"]
      end
      started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
      File.write(start_file, started_at.to_s)
      children.each do |child|
        ready = read_worker_message(child)
        raise "benchmark worker failed to start: #{ready.inspect}" unless ready["ready"]
      end

      deadline = started_at + DURATION
      while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        children.each do |child|
          status_path = "/proc/#{child.fetch(:pid)}/status"
          next unless File.exist?(status_path)

          resident = File.read(status_path)[/^VmRSS:\s+(\d+)\s+kB$/, 1]
          next unless resident

          current_rss = resident.to_i / 1024.0
          initial_rss_by_pid[child.fetch(:pid)] ||= current_rss
          ending_rss_by_pid[child.fetch(:pid)] = current_rss
          rss_by_pid[child.fetch(:pid)] = [rss_by_pid[child.fetch(:pid)], current_rss].max
        end
        sleep 0.25
      end
      worker_reports = children.map do |child|
        output = Timeout.timeout(20) { child.fetch(:stdout).read.lines }
        report = JSON.parse(output.last || "{}")
        status = child.fetch(:process).value
        raise "benchmark worker exited unsuccessfully: #{child.fetch(:stderr).read}" unless status.success?

        report
      end
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
      rows = AcpReliabilityBenchmarkRecord.count
      completed = worker_reports.sum { |report| report.fetch("measurement_completed_cycles") }
      worker_reports.each_with_index do |report, index|
        sample_path = File.join(sample_directory, "#{worker_count}-#{index + 1}.jsonl")
        samples = File.readlines(sample_path, chomp: true).map { |line| JSON.parse(line) }
        report["samples"] = samples
      end
      windows = CapacityWorkload.boundaries(DURATION).each_cons(2).map do |left, right|
        count = worker_reports.sum do |report|
          start_sample = report.fetch("samples").find { |sample| sample.fetch("at_seconds") == left }
          end_sample = report.fetch("samples").find { |sample| sample.fetch("at_seconds") == right }
          end_sample.fetch("completed_cycles") - start_sample.fetch("completed_cycles")
        end
        rss = worker_reports.sum do |report|
          samples = report.fetch("samples").select do |sample|
            sample.fetch("at_seconds") > left && sample.fetch("at_seconds") <= right
          end
          samples.sum { |sample| sample.fetch("rss_megabytes") } / samples.length
        end
        { duration_seconds: right - left, completed_cycles: count,
          throughput_cycles_per_second: count / (right - left), mean_rss_megabytes: rss }
      end
      coverage = Hash.new(0)
      worker_reports.each do |report|
        report.fetch("successful_tenant_cycles").each { |id, count| coverage[id] += count }
      end
      rss_growth = windows.last[:mean_rss_megabytes] / windows.first[:mean_rss_megabytes] - 1
      limits = {
        max_pipeline_reservations: CapacityWorkload.integer("PIPELINE_CAPACITY", 4500),
        max_active_cycles: CapacityWorkload.integer("PIPELINE_CAPACITY", 4500),
        max_queued_batches: CapacityWorkload.integer("PIPELINE_CAPACITY", 4500),
        max_active_fetches: CapacityWorkload.integer("FETCH_CONCURRENCY", 3500),
        max_active_resolves: CapacityWorkload.integer("RESOLVE_CONCURRENCY", 16),
        max_active_ingests: CapacityWorkload.integer("INGEST_CONCURRENCY", 16),
        max_reserved_bytes: CapacityWorkload.integer("RETAINED_BYTE_CAPACITY", 268_435_456),
        max_retained_bytes: CapacityWorkload.integer("RETAINED_BYTE_CAPACITY", 268_435_456)
      }
      gates = {
        full_workload: TENANT_COUNT == 30_000 && windows.all? { |window| window[:duration_seconds] >= 60 },
        throughput: windows.all? { |window| window[:throughput_cycles_per_second] >= 500 },
        healthy_tenant_coverage: coverage.length == TENANT_COUNT && coverage.values.all? { |count| count >= 2 },
        rss_growth: rss_growth < 0.10,
        reservation_limits: worker_reports.all? do |report|
          limits.all? { |name, limit| report.fetch("metrics").fetch(name.to_s) <= limit }
        end,
        released: worker_reports.all? do |report|
          %w[reserved_bytes retained_bytes pipeline_reservations retry_reservations active_cycles
             byte_budget_waiters active_fetches queued_batches active_ingests pending_acknowledgements].all? do |name|
            report.fetch("metrics").fetch(name).zero?
          end
        end
      }
      summary << {
        worker_count: worker_count,
        tenant_count: TENANT_COUNT,
        duration_seconds: elapsed.round(3),
        completed_cycles: completed,
        throughput_cycles_per_second: (completed / DURATION).round(2),
        measurement_duration_seconds: DURATION,
        drain_completed_cycles: worker_reports.sum { |report| report.fetch("benchmark_completed_cycles") } - completed,
        post_warmup_windows: windows,
        rss_growth_fraction: rss_growth,
        healthy_tenants_progressed: coverage.length,
        release_gates: gates,
        release_passed: gates.values.all?,
        destination_rows: rows,
        initial_worker_rss_megabytes: initial_rss_by_pid.values.sum.round(2),
        ending_worker_rss_megabytes: ending_rss_by_pid.values.sum.round(2),
        peak_worker_rss_megabytes: rss_by_pid.values.sum.round(2),
        capacities_per_worker: {
          fetch: Integer(ENV.fetch("ACP_BENCH_FETCH_CONCURRENCY", "3500")),
          resolve: Integer(ENV.fetch("ACP_BENCH_RESOLVE_CONCURRENCY", "16")),
          ingest: Integer(ENV.fetch("ACP_BENCH_INGEST_CONCURRENCY", "16")),
          pipeline: Integer(ENV.fetch("ACP_BENCH_PIPELINE_CAPACITY", "4500")),
          retained_bytes: CapacityWorkload.integer("RETAINED_BYTE_CAPACITY", 268_435_456)
        },
        postgres_pool_per_worker: {
          acp_requirement: acp_connections,
          configured: pool_size,
          application_headroom: pool_size - acp_connections,
          benchmark_headroom_allowance: pool_headroom
        },
        fleet_connection_budgets: {
          postgres: worker_count * pool_size,
          postgres_benchmark_driver: 1,
          redis: worker_count * 24,
          http_equivalent_requests: worker_count * CapacityWorkload.integer("FETCH_CONCURRENCY", 3500),
          http_actual: if ENV["ACP_BENCH_HTTP_URL"]
                         worker_count * CapacityWorkload.integer("FETCH_CONCURRENCY", 3500)
                       else
                         0
                       end
        },
        workers: worker_reports
      }
      connection.execute("TRUNCATE TABLE acp_reliability_benchmark_records")
    ensure
      children.each do |child|
        Process.kill("TERM", child.fetch(:pid)) if child.fetch(:process).alive?
        child.fetch(:stdout).close unless child.fetch(:stdout).closed?
        child.fetch(:stderr).close unless child.fetch(:stderr).closed?
      end
      prefix_data = [run_application, "benchmark", "AcpReliabilityServiceBenchmark"]
      namespace = Base64.urlsafe_encode64(JSON.generate(prefix_data), padding: false)
      keys = redis.scan_each(match: "acp:v1:#{namespace}:*").to_a
      redis.del(keys) unless keys.empty?
    end
  end
  CapacityWorkload.write_report(
    benchmark: "redis-postgresql-multi-process",
    workload: CapacityWorkload.describe,
    ruby: RUBY_DESCRIPTION,
    platform: RbConfig::CONFIG.fetch("host"),
    os_release: os_release,
    available_processors: Etc.nprocessors,
    cgroup_cpu_limit: cgroup_limit("/sys/fs/cgroup/cpu.max"),
    cgroup_memory_limit_bytes: cgroup_limit("/sys/fs/cgroup/memory.max"),
    redis_server: redis.info("server").fetch("redis_version"),
    postgres_server: connection.select_value("SHOW server_version"),
    dependencies: %w[async redis rails pg].to_h { |name| [name, Gem.loaded_specs[name]&.version&.to_s] },
    configured_latency_seconds: [Float(ENV.fetch("ACP_BENCH_LATENCY_MIN", "1")),
                                 Float(ENV.fetch("ACP_BENCH_LATENCY_MAX", "19"))],
    payload_bytes_per_record: Integer(ENV.fetch("ACP_BENCH_PAYLOAD_BYTES", "1024")),
    latency_distribution_method: "seeded per tenant/attempt, histogram bucket upper bounds",
    pressure_histogram_boundaries_seconds: Acp::PressureMetrics::BOUNDARIES,
    pressure_metrics_lifetime: "worker lifetime, including startup and drain; final and sampled worker snapshots",
    results: summary
  )
  require_release = ENV.fetch("ACP_BENCH_REQUIRE_RELEASE", TENANT_COUNT == 30_000 && DURATION >= 160 ? "1" : "0")
  exit(1) if require_release == "1" && summary.none? { |result| result[:release_passed] }
ensure
  redis&.close
  connection.drop_table(:acp_reliability_benchmark_records, if_exists: true)
  FileUtils.remove_entry(sample_directory) if sample_directory && File.directory?(sample_directory)
end
# rubocop:enable Metrics/BlockLength
