# frozen_string_literal: true

require "test_helper"
require_relative "fixtures/reliability_upstream"

class TestReliabilityFixtures < Minitest::Test
  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
  def test_seeded_payloads_and_timestamp_windows_are_reproducible
    cursor = Time.utc(2025, 1, 1)
    first = ReliabilityUpstream.new(seed: 12, payload_size: 64, records_per_batch: 3, timestamp_window: 2)
    replay = ReliabilityUpstream.new(seed: 12, payload_size: 64, records_per_batch: 3, timestamp_window: 2)

    left = first.fetch(tenant_id: "tenant-a", cursor: cursor)
    right = replay.fetch(tenant_id: "tenant-a", cursor: cursor)

    assert_equal left.data, right.data
    assert_equal cursor + 2, left.next_cursor
    assert(left.data.all? { |record| record.fetch(:payload).bytesize == 64 })
    assert(left.data.all? { |record| record.fetch(:updated_at).between?(cursor, left.next_cursor) })
  end

  def test_seeded_latency_and_fault_modes_are_repeatable
    upstream = ReliabilityUpstream.new(
      seed: 5,
      latency: [0.0, 0.001],
      faults: { [7, 1] => :timeout, [7, 2] => :disconnect }
    )

    assert_raises(Timeout::Error) { upstream.fetch(tenant_id: 7, cursor: Time.utc(2025, 1, 1)) }
    assert_raises(IOError) { upstream.fetch(tenant_id: 7, cursor: Time.utc(2025, 1, 1)) }
    batch = upstream.fetch(tenant_id: 7, cursor: Time.utc(2025, 1, 1))

    assert_kind_of Acp::Batch, batch
    assert_equal 1, upstream.calls
    assert_equal 1, upstream.latency_buckets.sum
    assert_operator upstream.latency_percentile(0.95), :>=, 0
  end

  def test_invalid_fixture_options_fail_early
    assert_raises(ArgumentError) { ReliabilityUpstream.new(latency: [2, 1]) }
    assert_raises(ArgumentError) { ReliabilityUpstream.new(payload_size: -1) }
    assert_raises(ArgumentError) { ReliabilityUpstream.new(records_per_batch: -1) }
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength
end
