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
  name: "index_acp_versioned_records_on_external_id"
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
      worker_count.times do |index|
        pool_size = Integer(ENV.fetch("ACP_BENCH_RESOLVE_CONCURRENCY", "16")) +
                    Integer(ENV.fetch("ACP_BENCH_INGEST_CONCURRENCY", "16")) + 5
        environment = ENV.to_h.merge(
          "ACP_BENCH_APPLICATION" => run_application,
          "ACP_BENCH_WORKER_ID" => "bench-#{worker_count}-#{index + 1}",
          "ACP_BENCH_START_FILE" => start_file,
          "ACP_BENCH_SAMPLE_PATH" => File.join(sample_directory, "#{worker_count}-#{index + 1}.jsonl"),
          "ACP_TEST_POOL" => ENV.fetch("ACP_TEST_POOL", pool_size.to_s)
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
      children.each { |child| Process.kill("TERM", child.fetch(:pid)) if child.fetch(:process).alive? }
      worker_reports = children.map do |child|
        output = Timeout.timeout(20) { child.fetch(:stdout).read.lines }
        report = JSON.parse(output.last || "{}")
        status = child.fetch(:process).value
        raise "benchmark worker exited unsuccessfully: #{child.fetch(:stderr).read}" unless status.success?

        report
      end
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
      rows = AcpReliabilityBenchmarkRecord.count
      completed = worker_reports.sum { |report| report.fetch("benchmark_completed_cycles") }
      steady_window = [Float(ENV.fetch("ACP_BENCH_STEADY_WINDOW", "30")), DURATION / 2].min
      worker_reports.each_with_index do |report, index|
        sample_path = File.join(sample_directory, "#{worker_count}-#{index + 1}.jsonl")
        samples = File.readlines(sample_path, chomp: true).map { |line| JSON.parse(line) }
        baseline = samples.select { |sample| sample.fetch("at_seconds") <= DURATION - steady_window }
                          .max_by { |sample| sample.fetch("at_seconds") }
        report["samples"] = samples
        report["steady_state_start"] = baseline
      end
      unless worker_reports.all? { |report| report["steady_state_start"] }
        raise "benchmark duration did not include a steady-state sample for every worker"
      end

      steady_start = worker_reports.sum { |report| report.dig("steady_state_start", "completed_cycles") || 0 }
      steady_at = worker_reports.sum { |report| report.dig("steady_state_start", "at_seconds") || 0 } /
                  [worker_reports.count { |report| report["steady_state_start"] }, 1].max
      steady_end = worker_reports.sum { |report| report.fetch("elapsed_seconds") } / worker_reports.length
      steady_duration = steady_end - steady_at
      raise "benchmark duration did not include a steady-state sample" unless steady_duration.positive?

      summary << {
        worker_count: worker_count,
        tenant_count: TENANT_COUNT,
        duration_seconds: elapsed.round(3),
        completed_cycles: completed,
        throughput_cycles_per_second: (completed / elapsed).round(2),
        steady_state_window_seconds: steady_duration.round(2),
        steady_state_throughput_cycles_per_second: ((completed - steady_start) / steady_duration).round(2),
        destination_rows: rows,
        initial_worker_rss_megabytes: initial_rss_by_pid.values.sum.round(2),
        ending_worker_rss_megabytes: ending_rss_by_pid.values.sum.round(2),
        peak_worker_rss_megabytes: rss_by_pid.values.sum.round(2),
        capacities_per_worker: {
          fetch: Integer(ENV.fetch("ACP_BENCH_FETCH_CONCURRENCY", "2500")),
          ingest: Integer(ENV.fetch("ACP_BENCH_INGEST_CONCURRENCY", "16")),
          pipeline: Integer(ENV.fetch("ACP_BENCH_PIPELINE_CAPACITY", "2500"))
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
  puts JSON.pretty_generate(
    benchmark: "redis-postgresql-multi-process",
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
    results: summary
  )
ensure
  redis&.close
  connection.drop_table(:acp_reliability_benchmark_records, if_exists: true)
  FileUtils.remove_entry(sample_directory) if sample_directory && File.directory?(sample_directory)
end
# rubocop:enable Metrics/BlockLength
