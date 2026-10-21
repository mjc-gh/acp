# frozen_string_literal: true

require "test_helper"
require "net/http"
require "socket"
require "open3"
require "tempfile"
require "timeout"
require_relative "../benchmark/http_workload"

# Exercise real HTTP socket waits, rather than substituting sleep for fetch.
# rubocop:disable Metrics/ClassLength, Metrics/MethodLength, Metrics/AbcSize
# rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
# rubocop:disable Metrics/BlockLength
class TestHttpRuntime < Minitest::Test
  def test_tls_keep_alive_parsing_and_renewal_progress
    with_http_fixture("ACP_HTTP_TLS" => "1", "ACP_HTTP_RECORDS" => "8",
                      "ACP_HTTP_PAYLOAD_BYTES" => "16384") do |fixture, certificate|
      upstream = HttpWorkload.new(url: fixture.fetch("url"), capacity: 3, ca_file: certificate.path)
      renewals = 0
      ownership = Acp::LocalOwnership.new
      ownership.define_singleton_method(:renewal_interval) { |_| 0.005 }
      ownership.define_singleton_method(:renew) do |*|
        renewals += 1
        true
      end
      progress = Object.new
      progress.define_singleton_method(:read) { |_| Time.utc(2025) }
      progress.define_singleton_method(:acknowledge) { |*| nil }
      program = Class.new(Acp::Program) do
        program_name "TlsReuseTest"
        interval 0.001
        fetch_concurrency 3
        ingest_concurrency 1
        pipeline_capacity 3
        tenants { |emit| [1, 2, 3].each { |id| emit.call(id) } }
        initial_cursor { |_| Time.utc(2025) }
        resolve { |id| id }
        fetch do |id, context|
          upstream.fetch(tenant_id: id, cursor: context.cursor, context: context)
        end
        ingest do |batch, _|
          raise "invalid parsed batch" unless batch.data.length == 8 && batch.data.first[:payload] == "X" * 16_384
        end
      end
      runtime = Acp::Runtime.new(configuration: program.configuration, progress: progress, ownership: ownership)
      Async do |root|
        runner = root.async { runtime.run }
        root.with_timeout(10) { Kernel.sleep(0.005) until runtime.metrics[:completed_cycles] >= 9 }
        runtime.request_shutdown(timeout: 1)
        runner.wait
        assert_operator upstream.metrics[:reused_requests], :>=, 6
        assert_equal 3, upstream.metrics[:connections_created]
        assert_operator upstream.metrics[:pressure][:parse][:count], :>=, 9
        assert_operator renewals, :>, 9
        upstream.close
      ensure
        runner&.stop
      end.wait
      assert_equal 0, runtime.metrics[:active_cycles]
      assert_equal 0, runtime.metrics[:pipeline_reservations]
    ensure
      upstream&.close
    end
  end

  def test_persistent_client_discards_failed_connections
    with_http_fixture("ACP_HTTP_FAIL_EVERY" => "2") do |fixture, _|
      upstream = HttpWorkload.new(url: fixture.fetch("url"), capacity: 1)
      Async do
        upstream.fetch(tenant_id: 1, cursor: Time.utc(2025))
        assert_raises(EOFError) { upstream.fetch(tenant_id: 1, cursor: Time.utc(2025)) }
        assert_equal 0, upstream.metrics[:idle]
        upstream.fetch(tenant_id: 1, cursor: Time.utc(2025))
        assert_equal 2, upstream.metrics[:connections_created]
        assert_equal 0, upstream.metrics[:active]
        upstream.close
      end.wait
    ensure
      upstream&.close
    end
  end

  def test_cancelling_a_tls_pooled_request_closes_and_replaces_the_client
    with_http_fixture("ACP_HTTP_TLS" => "1", "ACP_HTTP_LATENCY" => "0.2") do |fixture, certificate|
      upstream = HttpWorkload.new(url: fixture.fetch("url"), capacity: 1, ca_file: certificate.path)
      Async do |root|
        request = root.async { upstream.fetch(tenant_id: 1, cursor: Time.utc(2025)) }
        root.with_timeout(5) { Kernel.sleep(0.001) until upstream.metrics[:active] == 1 }
        request.stop
        request.wait
        assert_equal 0, upstream.metrics[:idle]
        assert_equal 0, upstream.metrics[:active]
        upstream.fetch(tenant_id: 1, cursor: Time.utc(2025))
        assert_equal 2, upstream.metrics[:connections_created]
        upstream.close
      end.wait
    ensure
      upstream&.close
    end
  end

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

  def with_http_fixture(environment)
    script = File.expand_path("../benchmark/http_upstream.rb", __dir__)
    input, output, error, process = Open3.popen3(environment, RbConfig.ruby, script)
    input.close
    fixture = Timeout.timeout(10) { JSON.parse(output.gets) }
    Tempfile.create(["acp-test-ca", ".pem"]) do |certificate|
      certificate.write(fixture["certificate"].to_s)
      certificate.flush
      yield fixture, certificate
    end
  ensure
    if process
      Process.kill("TERM", process.pid) if process.alive?
      unless process.join(3)
        Process.kill("KILL", process.pid)
        process.join
      end
    end
    output&.close
    error&.close
  end

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
# rubocop:enable Metrics/BlockLength
