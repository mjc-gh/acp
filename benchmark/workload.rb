# frozen_string_literal: true

# Shared, fully reported workload controls for local and service comparisons.
module CapacityWorkload
  module_function

  def integer(name, default)
    Integer(ENV.fetch("ACP_BENCH_#{name}", default.to_s))
  end

  def number(name, default)
    Float(ENV.fetch("ACP_BENCH_#{name}", default.to_s))
  end

  def upstream_options
    { tail_probability: number("TAIL_PROBABILITY", 0), tail_multiplier: number("TAIL_MULTIPLIER", 10) }
  end

  # All capacities remain independently overrideable for one-variable sweeps.
  def configure(program)
    program.resolve_concurrency(integer("RESOLVE_CONCURRENCY", 16))
    program.max_batch_bytes(integer("MAX_BATCH_BYTES", 65_536))
    program.retained_byte_capacity(integer("RETAINED_BYTE_CAPACITY", 268_435_456))
    program.max_response_bytes(integer("MAX_RESPONSE_BYTES", 65_536))
    program.resolve do |tenant_id|
      Kernel.sleep(CapacityWorkload.number("RESOLVE_DELAY", 0))
      tenant_id
    end
  end

  def describe
    { seed: integer("SEED", 20_261_001), tail: upstream_options,
      resolve_delay: number("RESOLVE_DELAY", 0), ingest_delay: number("INGEST_DELAY", 0),
      acknowledge_delay: number("ACKNOWLEDGE_DELAY", 0),
      max_batch_bytes: integer("MAX_BATCH_BYTES", 65_536),
      max_response_bytes: integer("MAX_RESPONSE_BYTES", 65_536),
      retained_byte_capacity: integer("RETAINED_BYTE_CAPACITY", 268_435_456),
      http_url: ENV["ACP_BENCH_HTTP_URL"], http_ca_file: ENV["ACP_BENCH_HTTP_CA_FILE"],
      blocking_parse_seconds: number("BLOCKING_PARSE", 0),
      target_utilization: 0.75 }
  end

  def process_metrics
    { cpu_user_seconds: Process.times.utime, cpu_system_seconds: Process.times.stime,
      gc: GC.stat, gc_total_seconds: GC.total_time / 1_000_000_000.0,
      file_descriptors: Dir.children("/proc/self/fd").length,
      sockets: Dir.glob("/proc/self/fd/*").count do |path|
        File.readlink(path).start_with?("socket:")
      rescue Errno::ENOENT
        false
      end,
      nofile_limit: Process.getrlimit(:NOFILE) }
  end

  def boundaries(duration)
    warmup = number("WARMUP", duration / 4)
    raise ArgumentError, "warmup must be nonnegative and less than duration" unless warmup >= 0 && warmup < duration

    [warmup, (warmup + duration) / 2, duration]
  end

  def rss
    File.read("/proc/self/status")[/^VmRSS:\s+(\d+)\s+kB$/, 1].to_i / 1024.0
  end

  def stage_times(metrics)
    metrics.fetch(:pressure).to_h do |name, histogram|
      [name, { mean: histogram[:sum] / [histogram[:count], 1].max,
               count: histogram[:count], p95_bucket_upper_bound: histogram[:p95] }]
    end
  end

  def write_report(report)
    json = JSON.pretty_generate(report)
    if ENV.key?("ACP_BENCH_REPORT")
      File.write(ENV.fetch("ACP_BENCH_REPORT"), "#{json}\n")
      if ENV["ACP_BENCH_SUMMARY"] == "1"
        puts JSON.generate(report_path: ENV.fetch("ACP_BENCH_REPORT"), benchmark: report[:benchmark])
        return
      end
    end
    puts json
  end
end
