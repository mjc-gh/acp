# frozen_string_literal: true

require "test_helper"
require "net/http"
require "socket"

# Exercise real HTTP socket waits, rather than substituting sleep for fetch.
# rubocop:disable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
# rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
class TestHttpRuntime < Minitest::Test
  def test_http_requests_overlap_and_lease_renewals_continue
    listener = TCPServer.new("127.0.0.1", 0)
    port = listener.addr[1]
    lock = Mutex.new
    active = 0
    maximum = 0
    handlers = []
    server = Thread.new do
      loop do
        socket = listener.accept
        handlers << Thread.new(socket) do |connection|
          connection.gets
          while (line = connection.gets)
            break if line == "\r\n"
          end
          lock.synchronize do
            active += 1
            maximum = [maximum, active].max
          end
          sleep 0.08
          connection.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
          lock.synchronize { active -= 1 }
        ensure
          connection.close
        end
      end
    rescue IOError, Errno::EBADF
      nil
    end
    renewals = 0
    ownership = Acp::LocalOwnership.new
    ownership.define_singleton_method(:renewal_interval) { |_token| 0.005 }
    ownership.define_singleton_method(:renew) do |*|
      renewals += 1
      true
    end
    progress = Object.new
    progress.define_singleton_method(:read) { |_id| Time.utc(2025, 1, 1) }
    progress.define_singleton_method(:acknowledge) { |*| nil }
    runtime = build_runtime(port: port, progress: progress, ownership: ownership)

    Async do
      runner = Async::Task.current.async { runtime.run }
      Async::Task.current.with_timeout(3) do
        Kernel.sleep(0.005) until runtime.metrics[:completed_cycles] == 3
      end
      runtime.request_shutdown(timeout: 1)
      runner.wait
    end

    assert_equal 3, maximum
    assert_operator renewals, :>=, 9
    assert_equal 3, runtime.metrics[:max_active_fetches]
    assert_equal 0, runtime.metrics[:active_cycles]
  ensure
    listener&.close
    server&.join
    handlers&.each(&:join)
  end

  def test_cancelling_a_request_closes_its_socket_and_releases_capacity
    listener = TCPServer.new("127.0.0.1", 0)
    started = Thread::Queue.new
    closed = Thread::Queue.new
    server = Thread.new do
      connection = listener.accept
      while (line = connection.gets)
        break if line == "\r\n"
      end
      started.push(true)
      closed.push(connection.read.empty?)
    ensure
      connection&.close
    end
    acknowledgements = 0
    progress = Object.new
    progress.define_singleton_method(:read) { |_id| Time.utc(2025, 1, 1) }
    progress.define_singleton_method(:acknowledge) { |*| acknowledgements += 1 }
    runtime = build_runtime(port: listener.addr[1], progress: progress,
                            ownership: Acp::LocalOwnership.new, tenant_ids: [1])

    Async do |root|
      runner = root.async { runtime.run }
      root.with_timeout(3) { Kernel.sleep(0.005) while started.empty? }
      runner.stop
      runner.wait
    ensure
      runner&.stop
    end

    assert server.join(1), "cancelled HTTP connection remained open"
    assert closed.pop
    assert_equal 0, acknowledgements
    assert_equal 0, runtime.metrics[:active_fetches]
    assert_equal 0, runtime.metrics[:active_cycles]
  ensure
    listener&.close
    server&.kill if server&.alive?
    server&.join
  end

  private

  def build_runtime(port:, progress:, ownership:, tenant_ids: [1, 2, 3])
    program = Class.new(Acp::Program) do
      program_name "HttpOverlapTest"
      interval 60
      fetch_concurrency 3
      ingest_concurrency 1
      pipeline_capacity 3
      tenants { |emit| tenant_ids.each { |id| emit.call(id) } }
      initial_cursor { |_id| Time.utc(2025, 1, 1) }
      resolve { |id| id }
      fetch do |_tenant, context|
        response = Net::HTTP.new("127.0.0.1", port, nil).start { |http| http.get("/") }
        raise "unexpected HTTP response" unless response.body == "ok"

        Acp::Batch.new(data: [], next_cursor: context.cursor)
      end
      ingest { |*| nil }
    end
    runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress, ownership: ownership)
    runtime.define_singleton_method(:initial_offset) { |_id| 0 }
    runtime
  end
end
# rubocop:enable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
# rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
