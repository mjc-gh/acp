# frozen_string_literal: true

require "async"
require "socket"
require "openssl"
require "json"

# Independently provision this process/container; /metrics reports its own load.
# Never run this fixture on a worker reactor when collecting performance evidence.
module HttpUpstream
  module_function

  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
  def listener
    tcp = TCPServer.new(ENV.fetch("ACP_HTTP_BIND", "127.0.0.1"), Integer(ENV.fetch("ACP_HTTP_PORT", "0")))
    return [tcp, tcp.addr[1], nil] unless ENV["ACP_HTTP_TLS"] == "1"

    key = OpenSSL::PKey::RSA.new(2048)
    certificate = OpenSSL::X509::Certificate.new
    certificate.version = 2
    certificate.serial = 1
    certificate.subject = certificate.issuer = OpenSSL::X509::Name.parse("/CN=localhost")
    certificate.public_key = key.public_key
    certificate.not_before = Time.now - 60
    certificate.not_after = Time.now + 86_400
    factory = OpenSSL::X509::ExtensionFactory.new
    factory.subject_certificate = factory.issuer_certificate = certificate
    certificate.add_extension(factory.create_extension("subjectAltName", "DNS:localhost,IP:127.0.0.1"))
    certificate.add_extension(factory.create_extension("basicConstraints", "CA:TRUE", true))
    certificate.sign(key, OpenSSL::Digest.new("SHA256"))
    context = OpenSSL::SSL::SSLContext.new
    context.cert = certificate
    context.key = key
    [OpenSSL::SSL::SSLServer.new(tcp, context), tcp.addr[1], certificate.to_pem]
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Metrics/BlockLength
  # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def run
    server, port, certificate = listener
    payload = JSON.generate(Array.new(Integer(ENV.fetch("ACP_HTTP_RECORDS", "1"))) do
      { payload: "x" * Integer(ENV.fetch("ACP_HTTP_PAYLOAD_BYTES", "1024")) }
    end)
    latency = Float(ENV.fetch("ACP_HTTP_LATENCY", "0.05"))
    fail_every = Integer(ENV.fetch("ACP_HTTP_FAIL_EVERY", "0"))
    stats = { requests: 0, completed: 0, active: 0, max_active: 0, connections: 0, open: 0, max_reactor_lag: 0.0 }
    stop = false
    Signal.trap("TERM") { stop = true }
    $stdout.sync = true
    puts JSON.generate(url: "http#{certificate ? "s" : ""}://127.0.0.1:#{port}/", certificate: certificate)
    Async do |root|
      sampler = root.async do
        loop do
          expected = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 0.1
          root.sleep(0.1)
          lag = Process.clock_gettime(Process::CLOCK_MONOTONIC) - expected
          stats[:max_reactor_lag] = [stats[:max_reactor_lag], lag].max
        end
      end
      acceptor = root.async do |parent|
        loop do
          connection = begin
            server.accept
          rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
            next # A peer can cancel during the TLS handshake, before dispatch.
          end
          stats[:connections] += 1
          stats[:open] += 1
          parent.async(connection) do |_, socket|
            counted = false
            while (request = socket.gets)
              while (line = socket.gets)
                break if line == "\r\n"
              end
              if request.split[1] == "/metrics"
                body = JSON.generate(stats.merge(cpu_seconds: Process.times.utime + Process.times.stime,
                                                 gc: GC.stat, nofile_limit: Process.getrlimit(:NOFILE)))
              else
                stats[:requests] += 1
                break if fail_every.positive? && (stats[:requests] % fail_every).zero?

                stats[:active] += 1
                counted = true
                stats[:max_active] = [stats[:max_active], stats[:active]].max
                Kernel.sleep(latency)
                body = payload
              end
              socket.write("HTTP/1.1 200 OK\r\nContent-Length: #{body.bytesize}\r\n" \
                           "Connection: keep-alive\r\n\r\n#{body}")
              next unless counted

              stats[:completed] += 1
              stats[:active] -= 1
              counted = false
            end
          rescue IOError, SystemCallError, OpenSSL::SSL::SSLError
            nil
          ensure
            stats[:active] -= 1 if counted
            stats[:open] -= 1
            socket.close
          end
        end
      end
      root.sleep(0.01) until stop
      acceptor.stop
      sampler.stop
    end
  ensure
    server&.close
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength, Metrics/BlockLength
  # rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
end

HttpUpstream.run if $PROGRAM_NAME == __FILE__
