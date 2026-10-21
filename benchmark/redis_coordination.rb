# frozen_string_literal: true

# Run on otherwise idle disposable Redis: bundle exec ruby benchmark/redis_coordination.rb
require "json"
require_relative "../lib/acp/redis"

# Server counters include Lua's internal commands; EVAL/EVALSHA counts measure wire operations.
# rubocop:disable Metrics/ModuleLength, Metrics/MethodLength, Metrics/AbcSize, Metrics/BlockLength
# rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
module RedisCoordinationBenchmark
  module_function

  def now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def counters(redis)
    info = redis.info
    stats = redis.info("commandstats")
    wire = %w[eval evalsha].sum { |name| stats.fetch(name, {}).fetch("calls", 0).to_i }
    { commands: info.fetch("total_commands_processed").to_i, script_calls: wire,
      input_bytes: info.fetch("total_net_input_bytes").to_i,
      output_bytes: info.fetch("total_net_output_bytes").to_i,
      cpu_seconds: info.fetch("used_cpu_user").to_f + info.fetch("used_cpu_sys").to_f }
  end

  def pool_counters(stores)
    %i[commands_checkout commands_service renewal_checkout renewal_service].to_h do |name|
      values = Array(stores).map { |store| store.metrics[:pressure].fetch(name) }
      [name, { count: values.sum { |value| value[:count] }, seconds: values.sum { |value| value[:sum] } }]
    end
  end

  def measure(redis, stores: [])
    before = counters(redis)
    pools_before = pool_counters(stores)
    start = now
    yield
    elapsed = now - start
    after = counters(redis)
    pools = pool_counters(stores).to_h do |name, values|
      [name, values.to_h { |field, value| [field, value - pools_before[name][field]] }]
    end
    after.transform_values.with_index { |value, index| value - before.values[index] }
         .merge(seconds: elapsed, pools: pools)
  end

  def program(count)
    Class.new(Acp::Program) do
      program_name "CoordinationBenchmark"
      interval 60
      discovery_interval 60
      pipeline_capacity 8
      fetch_concurrency 8
      ingest_concurrency 2
      tenants { |emit| count.times { |id| emit.call(id) } }
      initial_cursor { |_| Time.utc(2025, 1, 1) }
      resolve { |id| id }
      fetch { |_, context| Acp::Batch.new(data: [], next_cursor: context.cursor) }
      ingest { |*| nil }
    end
  end

  def run
    url = ENV.fetch("ACP_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")
    redis = Redis.new(url: url)
    count = Integer(ENV.fetch("TENANTS", "30000"))
    results = []
    [1, 2, 16, 64].each do |workers|
      stores = workers.times.map do |index|
        Acp::RedisCoordinator.new(application: "benchmark-#{Process.pid}", environment: "test",
                                  program: workers.to_s, redis_url: url, worker_id: "worker-#{index}",
                                  lease_ttl: 600, worker_scan_limit: 256)
      end
      store = stores.first
      configuration = program(count).configuration
      runtime = Acp::Runtime.new(configuration: configuration, ownership: store, progress: store)
      stores.each { |worker| worker.heartbeat_worker(capacity: 8, active: 0) }
      # Warm script caches without adding a tenant.
      token = store.acquire_discovery
      store.begin_discovery(token)
      store.release_discovery(token)
      discovery = measure(redis, stores: store) { Async { runtime.send(:perform_discovery) }.wait }
      assignments = nil
      snapshot = measure(redis, stores: store) { Async { assignments = store.scheduling_tenants }.wait }
      records = store.send(:live_worker_records)
      by_id = stores.to_h { |worker| [worker.send(:encoded_worker_id), worker] }
      targets = (0...1000).map do |id|
        owner = store.send(:assigned_worker, store.canonical_tenant_id(id), records).first
        [id, by_id.fetch(owner)]
      end
      latencies = []
      claims = measure(redis, stores: stores) do
        targets.each do |id, worker|
          start = now
          lease = worker.acquire(id)
          raise "claim failed" unless lease

          latencies << now - start
          worker.release(id, lease)
        end
      end
      cycle_ids = assignments.first(100)
      cycle_ids.each { |id| store.initialize_state(id, Time.utc(2025, 1, 1)) }
      cycles = measure(redis, stores: store) do
        Async do |root|
          queue = Acp::Runtime::IngestQueue.new(8)
          runtime.instance_variable_set(:@ingest_queue, queue)
          ingester = root.async { runtime.send(:ingestion_worker, queue) }
          cycle_ids.each { |id| runtime.send(:run_cycle, id) }
          ingester.stop
        end.wait
      end
      row = { workers: workers, tenants: count, discovery: discovery, snapshot: snapshot,
              claims: claims.merge(count: targets.length, p50: latencies.sort[500], p99: latencies.sort[990]),
              cycles: cycles.merge(count: runtime.metrics[:completed_cycles]),
              checkout_wait: store.metrics[:pressure][:commands_checkout].slice(:count, :sum, :max),
              command_latency: store.metrics[:pressure][:commands_service].slice(:count, :sum, :max) }
      results << row
      puts JSON.generate(row)
    ensure
      stores&.each(&:close)
      if store
        keys = redis.scan_each(match: "#{store.instance_variable_get(:@prefix)}:*").to_a
        redis.unlink(keys) unless keys.empty?
      end
    end
    results
  ensure
    redis&.close
  end
end
# rubocop:enable Metrics/ModuleLength, Metrics/MethodLength, Metrics/AbcSize, Metrics/BlockLength
# rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

RedisCoordinationBenchmark.run if $PROGRAM_NAME == __FILE__
