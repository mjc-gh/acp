# frozen_string_literal: true

module Acp
  # Consumer data paired with the explicit cursor produced by one fetch.
  class Batch
    attr_reader :data, :next_cursor

    def initialize(data:, next_cursor:)
      raise InvalidBatchError, "batch data cannot be nil" if data.nil?
      raise InvalidBatchError, "batch next_cursor is required" if next_cursor.nil?

      @data = data
      @next_cursor = Timestamp.normalize(next_cursor)
      freeze
    rescue ArgumentError => e
      raise InvalidBatchError, e.message
    end

    # Equal timestamps are valid (for empty results or no new progress).
    def validate_after!(committed_cursor)
      cursor = Timestamp.normalize(committed_cursor)
      raise CursorRegressionError, "next_cursor cannot move backwards" if next_cursor < cursor

      self
    end
  end
end
