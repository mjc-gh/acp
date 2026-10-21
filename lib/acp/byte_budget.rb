# frozen_string_literal: true

require "async/notification"

module Acp
  # Reactor-local FIFO weighted admission. No growth while holding a reservation:
  # estimate overflow fails rather than deadlocking several partially held batches.
  class ByteBudget
    def initialize(capacity, metrics)
      @capacity = capacity
      @metrics = metrics
      @waiters = []
      @changed = Async::Notification.new
      %i[reserved_bytes max_reserved_bytes retained_bytes max_retained_bytes
         byte_budget_waits byte_budget_waiters max_byte_budget_waiters
         oversized_batches estimate_overflows].each { |name| metrics[name] = 0 }
    end

    def available?(bytes)
      !@capacity || @metrics[:reserved_bytes] + bytes <= @capacity
    end

    # FIFO prevents a stream of small batches from starving a large one.
    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    def acquire(bytes)
      raise PayloadLimitError, "estimate exceeds entire retained byte budget" if bytes > @capacity

      waiter = Object.new
      @waiters << waiter
      @metrics[:byte_budget_waits] += 1 if @waiters.first != waiter || @metrics[:reserved_bytes] + bytes > @capacity
      @metrics[:byte_budget_waiters] += 1
      @metrics[:max_byte_budget_waiters] = [@metrics[:max_byte_budget_waiters], @metrics[:byte_budget_waiters]].max
      @changed.wait until @waiters.first == waiter && @metrics[:reserved_bytes] + bytes <= @capacity
      @metrics[:reserved_bytes] += bytes
      @metrics[:max_reserved_bytes] = [@metrics[:max_reserved_bytes], @metrics[:reserved_bytes]].max
      bytes
    ensure
      if waiter
        @waiters.delete(waiter)
        @metrics[:byte_budget_waiters] -= 1
        @changed.signal
      end
    end
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

    def release(bytes)
      @metrics[:reserved_bytes] -= bytes
      @changed.signal
    end
  end
end
