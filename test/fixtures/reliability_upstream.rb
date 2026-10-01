# frozen_string_literal: true

require "digest"
require "timeout"

# Deterministic, scheduler-friendly upstream used by reliability tests and the
# local runtime benchmark. Faults are keyed by [tenant_id, one_based_attempt].
class ReliabilityUpstream
  # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength
  # rubocop:disable Metrics/ParameterLists, Metrics/PerceivedComplexity
  LATENCY_BUCKETS = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300].freeze

  attr_reader :calls, :payload_bytes, :latency_buckets

  def initialize(seed: 1, latency: [0.0, 0.0], payload_size: 128, records_per_batch: 1,
                 timestamp_window: 1, faults: {})
    @seed = Integer(seed)
    @latency_min, @latency_max = latency.map { |value| Float(value) }
    unless @latency_min >= 0 && @latency_max >= @latency_min
      raise ArgumentError, "latency must be nonnegative and ordered"
    end
    raise ArgumentError, "payload_size must be nonnegative" unless payload_size.is_a?(Integer) && payload_size >= 0
    unless records_per_batch.is_a?(Integer) && records_per_batch >= 0
      raise ArgumentError, "records_per_batch must be nonnegative"
    end
    unless timestamp_window.is_a?(Numeric) && timestamp_window.positive?
      raise ArgumentError, "timestamp_window must be positive"
    end

    @payload_size = payload_size
    @records_per_batch = records_per_batch
    timestamp_microseconds = Float(timestamp_window) * 1_000_000
    window_microseconds = timestamp_microseconds.round
    unless window_microseconds.positive? && (window_microseconds - timestamp_microseconds).abs < 0.000_001
      raise ArgumentError, "timestamp_window must have microsecond precision"
    end

    @timestamp_window = window_microseconds / 1_000_000.0
    @faults = faults.transform_keys { |key| [key[0], Integer(key[1])] }.freeze
    @attempts = Hash.new(0)
    @calls = 0
    @payload_bytes = 0
    @latency_buckets = Array.new(LATENCY_BUCKETS.length, 0)
  end

  def fetch(tenant_id:, cursor:)
    @attempts[tenant_id] += 1
    attempt = @attempts[tenant_id]
    fault = @faults[[tenant_id, attempt]]
    raise_fault(fault) if fault && fault != :slow

    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    delay = latency_for(tenant_id, attempt) + (fault == :slow ? @latency_max : 0)
    Kernel.sleep(delay) if delay.positive?
    records = records_for(tenant_id, cursor, attempt)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    @calls += 1
    @payload_bytes += records.sum { |record| record.fetch(:payload).bytesize }
    bucket = LATENCY_BUCKETS.index { |boundary| elapsed <= boundary } || LATENCY_BUCKETS.length - 1
    @latency_buckets[bucket] += 1
    Acp::Batch.new(data: records, next_cursor: Acp::Timestamp.normalize(cursor) + @timestamp_window)
  end

  def latency_percentile(percentile)
    return 0.0 if @calls.zero?

    target = (@calls * percentile).ceil
    cumulative = 0
    LATENCY_BUCKETS.each_with_index do |boundary, index|
      cumulative += @latency_buckets[index]
      return boundary if cumulative >= target
    end
  end

  private

  def latency_for(tenant_id, attempt)
    return @latency_min if @latency_min == @latency_max

    random = seeded_random(tenant_id, attempt, "latency")
    @latency_min + (random.rand * (@latency_max - @latency_min))
  end

  def records_for(tenant_id, cursor, attempt)
    base_time = Acp::Timestamp.normalize(cursor)
    random = seeded_random(tenant_id, attempt, "records")
    Array.new(@records_per_batch) do |index|
      offset = (random.rand * @timestamp_window * 1_000_000).round / 1_000_000.0
      timestamp = base_time + offset
      {
        id: Digest::SHA256.hexdigest("#{@seed}:#{tenant_id}:#{attempt}:#{index}")[0, 24],
        tenant_id: tenant_id,
        updated_at: timestamp,
        payload: "x" * @payload_size
      }
    end
  end

  def seeded_random(tenant_id, attempt, purpose)
    value = Digest::SHA256.hexdigest("#{@seed}:#{tenant_id.class}:#{tenant_id}:#{attempt}:#{purpose}")
    Random.new(Integer(value[0, 16], 16))
  end

  def raise_fault(fault)
    case fault
    when :timeout then raise Timeout::Error, "simulated upstream timeout"
    when :disconnect then raise IOError, "simulated upstream disconnect"
    else
      raise ArgumentError, "unknown simulated upstream fault: #{fault.inspect}"
    end
  end
  # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength
  # rubocop:enable Metrics/ParameterLists, Metrics/PerceivedComplexity
end
