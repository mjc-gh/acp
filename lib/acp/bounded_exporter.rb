# frozen_string_literal: true

require "json"

module Acp
  # Lossy telemetry only. Owns copied JSON strings, never application objects or
  # reservations. One consumer thread, capacity pending records, drop-new overflow.
  class BoundedExporter
    def initialize(capacity: 256, max_event_bytes: 4096, &consumer)
      unless capacity.is_a?(Integer) && capacity.positive? &&
             max_event_bytes.is_a?(Integer) && max_event_bytes.positive? && consumer
        raise ArgumentError, "positive capacity/max_event_bytes and a consumer are required"
      end

      @queue = SizedQueue.new(capacity)
      @max_event_bytes = max_event_bytes
      @lock = Mutex.new
      @counts = { accepted: 0, dropped: 0, exported: 0, errors: 0 }
      @thread = Thread.new { consume(consumer) }
    end

    def offer(event)
      json = JSON.generate(event).freeze
      @lock.synchronize do
        return drop if @queue.closed? || json.bytesize > @max_event_bytes

        @queue.push(json, true)
        @counts[:accepted] += 1
        true
      rescue ThreadError
        drop
      end
    end

    def metrics
      @lock.synchronize { @counts.merge(queued: @queue.length, alive: @thread.alive?).freeze }
    end

    # Close admission, drain until deadline, then discard pending records. An
    # already executing consumer may outlive close; never wait for it indefinitely.
    def close(timeout: 1)
      unless timeout.is_a?(Numeric) && timeout.finite? && timeout >= 0
        raise ArgumentError, "timeout must be finite and nonnegative"
      end

      @lock.synchronize { @queue.close }
      wait_for_consumer(timeout)
      discard_pending
      !@thread.alive?
    end

    private

    def discard_pending
      @lock.synchronize do
        @counts[:dropped] += 1 while @queue.pop(true)
      rescue ThreadError
        nil
      end
    end

    def wait_for_consumer(timeout)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      while @thread.alive? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        Kernel.sleep(remaining.clamp(0, 0.001))
      end
    end

    def drop
      @counts[:dropped] += 1
      false
    end

    def consume(consumer)
      while (json = @queue.pop)
        begin
          consumer.call(json)
          @lock.synchronize { @counts[:exported] += 1 }
        rescue StandardError
          @lock.synchronize { @counts[:errors] += 1 }
        end
      end
    end
  end
end
