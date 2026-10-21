# frozen_string_literal: true

require "test_helper"
require "active_support/notifications"

# Interleavings are driven by elapsed-work clocks and gates, not latency assertions.
# rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/ClassLength
class TestResponsiveness < Minitest::Test
  def test_synchronous_observers_and_error_handlers_are_measured
    clock = Struct.new(:time) do
      def now
        time
      end
    end.new(0.0)
    configuration = Struct.new(:coordination_timeout, :retained_byte_capacity).new
    configuration.define_singleton_method(:name) { "ObserverTest" }
    configuration.define_singleton_method(:pipeline_capacity) { 1 }
    configuration.define_singleton_method(:retry_capacity) { 0 }
    runtime = Acp::Runtime.new(configuration: configuration, progress: Object.new, clock: clock,
                               on_error: ->(*) { clock.time += 0.3 })
    subscriber = ActiveSupport::Notifications.subscribe("failure.acp") { clock.time += 0.2 }
    runtime.send(:report_error, nil, IOError.new("offline"), :fetch)

    assert_in_delta 0.2, runtime.metrics[:pressure][:notification_service][:max]
    assert_in_delta 0.3, runtime.metrics[:pressure][:error_handler_service][:max]
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  def test_discovery_yields_before_finishing_and_keeps_unique_states
    emitted = 0
    observed = nil
    configuration = Class.new(Acp::Program) do
      program_name "YieldTest"
      interval 1
      fetch_concurrency 1
      ingest_concurrency 1
      pipeline_capacity 1
      tenants do |emit|
        500.times do |id|
          emit.call(id)
          emitted += 1
        end
      end
      initial_cursor { |_| Time.utc(2025) }
      resolve { |id| id }
      fetch { |_, context| Acp::Batch.new(data: [], next_cursor: context.cursor) }
      ingest { |*| nil }
    end.configuration
    runtime = Acp::Runtime.new(configuration: configuration, progress: Object.new)
    clock = Object.new
    ticks = 0
    clock.define_singleton_method(:now) { ticks += 0.006 }
    constructor = Acp::CooperativeWork.method(:new)
    states = nil
    Acp::CooperativeWork.stub(:new, -> { constructor.call(clock: clock) }) do
      Async do |root|
        scanner = root.async { states = runtime.send(:discover) }
        root.async { observed = emitted }
        scanner.wait
      end
    end

    assert_operator observed, :<, 500
    assert_equal 500, states.map(&:tenant_id).uniq.length
    assert states.all?(&:enabled)
  end

  def test_heap_arbitrary_removals_preserve_order_and_scheduled_flags
    heap = Acp::Runtime::DueHeap.new
    random = Random.new(42)
    states = Array.new(1000) do |index|
      Acp::Runtime::State.new(tenant_id: index, due: random.rand(100), sequence: index)
    end
    states.each { |state| heap.push(state) }
    removed = states.sample(400, random: random)
    removed.each { |state| heap.delete(state) }
    remaining = []
    remaining << heap.pop until heap.empty?

    assert_equal((states - removed).sort_by { |state| [state.due, state.sequence] }, remaining)
    assert states.none?(&:scheduled)
  end

  def test_exporter_drops_new_records_and_copies_ownership
    entered = Thread::Queue.new
    release = Thread::Queue.new
    received = Thread::Queue.new
    exporter = Acp::BoundedExporter.new(capacity: 1, max_event_bytes: 64) do |json|
      entered << true
      release.pop
      received << JSON.parse(json)
    end
    event = { value: +"original" }
    assert exporter.offer(event)
    entered.pop
    event[:value].replace("changed")
    assert exporter.offer(value: "pending")
    refute exporter.offer(value: "overflow")
    refute exporter.offer(value: "x" * 100)
    refute exporter.close(timeout: 0)
    refute exporter.offer(value: "closed")
    assert_equal 4, exporter.metrics[:dropped]
    assert_equal 0, exporter.metrics[:queued]
    release << true
    assert_equal "original", received.pop.fetch("value")
    assert exporter.close(timeout: 1)
  ensure
    release << true if release
    exporter&.close(timeout: 1)
  end

  def test_export_errors_do_not_stop_drain_or_block_reactor
    exporter = Acp::BoundedExporter.new { |_| raise IOError, "offline" }
    assert exporter.offer(event: "test")
    progressed = false
    Async do |root|
      root.async do
        Kernel.sleep(0)
        progressed = true
      end
      assert exporter.close(timeout: 1)
    end

    assert progressed
    assert_equal 1, exporter.metrics[:errors]
  ensure
    exporter&.close(timeout: 1)
  end
end
# rubocop:enable Metrics/MethodLength, Metrics/AbcSize, Metrics/ClassLength
