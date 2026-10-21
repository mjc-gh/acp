# frozen_string_literal: true

module Acp
  # Apply one cooperative operation budget across adapter checkout and I/O.
  class DeadlineAdapter
    def initialize(adapter, timeout)
      @adapter = adapter
      @timeout = timeout
    end

    def method_missing(name, ...)
      return super unless @adapter.respond_to?(name)
      return @adapter.public_send(name, ...) unless @timeout && Async::Task.current?

      Async::Task.current.with_timeout(@timeout, StageTimeoutError) { @adapter.public_send(name, ...) }
    end

    def respond_to_missing?(name, include_private = false)
      @adapter.respond_to?(name, include_private) || super
    end
  end
end
