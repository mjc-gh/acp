# frozen_string_literal: true

# Rails discovery and full-runtime measurements, with real PostgreSQL page reads.
require_relative "redis_coordination"
ENV["RAILS_ENV"] ||= "test"
require "rails"
require "active_record"
require_relative "../test/rails_app/config/application"
AcpRailsTestApp::Application.initialize!
require_relative "../lib/acp/rails"

# Instrument actual selected-pool checkout and hold boundaries, including lazy reads.
module BenchmarkPoolTiming
  # Keep the measured entry/exit boundaries in one scope.
  # rubocop:disable Metrics/MethodLength
  def with_connection(**options)
    return super if active_connection?

    start = RedisCoordinationBenchmark.now
    super do |connection|
      acquired = RedisCoordinationBenchmark.now
      @benchmark_waits << acquired - start
      begin
        yield connection
      ensure
        @benchmark_holds << RedisCoordinationBenchmark.now - acquired
      end
    end
  end
  # rubocop:enable Metrics/MethodLength

  def reset_benchmark
    @benchmark_waits = []
    @benchmark_holds = []
  end

  def benchmark_timings
    { checkouts: @benchmark_holds.length, wait_seconds: @benchmark_waits.sum,
      hold_seconds: @benchmark_holds.sum, max_hold_seconds: @benchmark_holds.max }
  end
end

# rubocop:disable Metrics/BlockLength, Metrics/AbcSize, Metrics/MethodLength
# rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
# Exercise Rails discovery checkout scopes separately from the full scheduler.
module RedisCoordinationFullBenchmark
  module_function

  def configuration(count, mode, pool)
    program = RedisCoordinationBenchmark.program(count)
    if mode == :paged
      program.tenants_page do |cursor, limit|
        start = cursor || 0
        stop = [start + limit, count].min
        ids = Enumerator.new do |output|
          pool.with_connection do |connection|
            connection.select_values("SELECT generate_series(#{start}, #{stop - 1})").each { |id| output << id }
          end
        end
        [ids, stop == count ? nil : stop]
      end
    else
      program.tenants do |emit|
        count.times.each_slice(256) do |batch|
          pool.with_connection do |connection|
            connection.select_values("SELECT generate_series(#{batch.first}, #{batch.last})")
                      .each { |id| emit.call(id) }
          end
        end
      end
    end
    Acp::Rails.configuration_for(program, transaction_owner: pool)
  end

  def run
    pool = ActiveRecord::Base.connection_pool
    pool.singleton_class.prepend(BenchmarkPoolTiming)
    pool.reset_benchmark
    pool.with_connection { |connection| connection.select_value("SELECT 1") }
    url = ENV.fetch("ACP_TEST_REDIS_URL")
    redis = Redis.new(url: url)
    count = Integer(ENV.fetch("TENANTS", "30000"))
    [1, 2, 16, 64].each do |workers|
      stores = workers.times.map do |index|
        Acp::RedisCoordinator.new(application: "full-benchmark-#{Process.pid}", environment: "test",
                                  program: workers.to_s, redis_url: url, worker_id: "worker-#{index}", lease_ttl: 600)
      end
      stores.each { |worker| worker.heartbeat_worker(capacity: 8, active: 0) }
      store = stores.first
      %i[legacy paged].each do |mode|
        pool.reset_benchmark
        runtime = Acp::Runtime.new(configuration: configuration(count, mode, pool), ownership: store, progress: store)
        result = RedisCoordinationBenchmark.measure(redis, stores: store) do
          Async { runtime.send(:perform_discovery) }.wait
        end
        puts JSON.generate(type: "rails_discovery", workers: workers, mode: mode, result: result,
                           database: pool.benchmark_timings)
      end
      # A full scheduler/queue/heartbeat run in one process, with live registry
      # peers (not 64 Ruby processes). One fetch lasts past the initial lease TTL.
      selected = configuration(count, :paged, pool)
      callbacks = selected.callbacks.dup
      slow_fetch = true
      callbacks[:fetch] = lambda do |_, context|
        if slow_fetch
          slow_fetch = false
          Kernel.sleep(3.2)
        end
        Acp::Batch.new(data: [], next_cursor: context.cursor)
      end
      selected = Acp::Configuration.new(name: selected.name,
                                        settings: Acp::Program::SETTING_NAMES.to_h do |name|
                                          [name, selected.public_send(name)]
                                        end,
                                        callbacks: callbacks, fetch_retry: selected.fetch_retry,
                                        ingest_retry: selected.ingest_retry)
      # Keep the local worker identity/capacity; registry peers stay live for 600s.
      store.close
      local = Acp::RedisCoordinator.new(application: "full-benchmark-#{Process.pid}", environment: "test",
                                        program: workers.to_s, redis_url: url, worker_id: "worker-0", lease_ttl: 3)
      runtime = Acp::Runtime.new(configuration: selected, ownership: local, progress: local)
      runtime.define_singleton_method(:initial_offset) { |_| 0 }
      pool.reset_benchmark
      result = RedisCoordinationBenchmark.measure(redis, stores: local) do
        Async do |root|
          runner = root.async { runtime.run }
          root.with_timeout(120) do
            Kernel.sleep(0.01) until runtime.metrics[:completed_cycles] >= 100 && local.metrics[:renewal_successes] >= 3
          end
          runtime.request_shutdown(timeout: 10)
          runner.wait
        ensure
          runner&.stop
        end.wait
      end
      puts JSON.generate(type: "runtime", workers: workers, result: result,
                         completed: runtime.metrics[:completed_cycles], database: pool.benchmark_timings,
                         renewal_slack: local.metrics[:pressure][:lease_renewal_slack].slice(:count, :min, :max),
                         renewal_misses: local.metrics[:renewal_deadline_misses],
                         redis_checkout: local.metrics[:pressure][:commands_checkout].slice(:count, :sum, :max))
    ensure
      local&.close
      stores&.each(&:close)
      if store
        keys = redis.scan_each(match: "#{store.instance_variable_get(:@prefix)}:*").to_a
        redis.unlink(keys) unless keys.empty?
      end
    end
  ensure
    redis&.close
  end
end
# rubocop:enable Metrics/BlockLength, Metrics/AbcSize, Metrics/MethodLength
# rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

RedisCoordinationFullBenchmark.run
