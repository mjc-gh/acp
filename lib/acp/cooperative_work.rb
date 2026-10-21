# frozen_string_literal: true

module Acp
  # Check only at invariant-safe boundaries. A single operation/callback can
  # exceed the target: cooperative scheduling cannot preempt Ruby or native code.
  class CooperativeWork
    BUDGET = 0.005

    def initialize(clock: MonotonicClock.new, budget: BUDGET)
      @clock = clock
      @budget = budget
      @started = clock.now
    end

    def checkpoint
      return unless @clock.now - @started >= @budget

      Async::Task.current?&.yield
      @started = @clock.now
    end

    def self.each(items)
      return enum_for(__method__, items) unless block_given?

      budget = new
      items.each do |*values|
        yield(*values)
        budget.checkpoint
      end
    end
  end
end
