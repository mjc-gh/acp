# frozen_string_literal: true

require "net/http"
require "json"
require "openssl"

# Reactor-local persistent clients. Each checked-out client has one owner; an
# exceptional/cancelled request is closed rather than returned to the idle pool.
class HttpWorkload
  attr_reader :calls, :payload_bytes

  def initialize(url:, capacity:, ca_file: nil, blocking_parse: 0)
    @uri = URI(url)
    @capacity = capacity
    @ca_file = ca_file
    @blocking_parse = blocking_parse
    @semaphore = Async::Semaphore.new(capacity)
    @idle = []
    @calls = @payload_bytes = @active = @connections = @reused = 0
    @pressure = Acp::PressureMetrics.new(%i[socket parse transform], clock: Acp::MonotonicClock.new)
  end

  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
  # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  def fetch(tenant_id:, cursor:, context: nil)
    @semaphore.acquire
    acquired = true
    @active += 1
    client = @idle.pop
    if client
      @reused += 1
    else
      client = new_client
      @connections += 1
    end
    body = @pressure.measure(:socket) do
      client.start unless client.started?
      data = +""
      client.request(Net::HTTP::Get.new(@uri.request_uri)) do |response|
        raise IOError, "HTTP #{response.code}" unless response.code == "200"

        response.read_body do |chunk|
          context&.validate_response_bytes!(data.bytesize + chunk.bytesize)
          data << chunk
        end
      end
      data
    end
    records = @pressure.measure(:parse) do
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @blocking_parse
      nil while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      JSON.parse(body)
    end
    batch = @pressure.measure(:transform) do
      data = records.map do |record|
        { tenant_id: tenant_id, updated_at: cursor + 1, payload: record.fetch("payload").upcase }
      end
      Acp::Batch.new(data: data, next_cursor: cursor + 1,
                     size_bytes: data.sum { |record| record[:payload].bytesize + 512 })
    end
    @calls += 1
    @payload_bytes += body.bytesize
    reusable = true
    batch
  ensure
    begin
      if client
        if reusable
          @idle << client
        elsif client.started?
          client.finish
        end
      end
    ensure
      if acquired
        @active -= 1
        @semaphore.release
      end
    end
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength
  # rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity

  def metrics
    { capacity: @capacity, active: @active, idle: @idle.length, connections_created: @connections,
      reused_requests: @reused, pressure: @pressure.snapshot }
  end

  def latency_percentile(percentile)
    @pressure.snapshot[:socket].fetch(:"p#{(percentile * 100).to_i}")
  end

  def close
    @idle.pop.finish until @idle.empty?
  end

  private

  def new_client
    Net::HTTP.new(@uri.host, @uri.port, nil).tap do |client|
      client.use_ssl = @uri.scheme == "https"
      client.ca_file = @ca_file if @ca_file
      client.open_timeout = client.read_timeout = client.write_timeout = 30
      client.keep_alive_timeout = 30
      client.max_retries = 0
    end
  end
end
