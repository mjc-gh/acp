# frozen_string_literal: true

require "securerandom"

module Acp
  # Immutable identity and progress metadata for one polling-stage invocation.
  class Context
    attr_reader :tenant_id, :cursor, :poll_id, :attempt

    def initialize(tenant_id:, cursor:, poll_id:, attempt: 1)
      raise ArgumentError, "tenant_id is required" if tenant_id.nil?
      raise ArgumentError, "poll_id must be a nonempty string" unless poll_id.is_a?(String) && !poll_id.empty?
      raise ArgumentError, "attempt must be a positive integer" unless attempt.is_a?(Integer) && attempt.positive?

      @tenant_id = tenant_id
      @cursor = Timestamp.normalize(cursor)
      @poll_id = poll_id.dup.freeze
      @attempt = attempt
      freeze
    end

    def self.new_poll_id
      SecureRandom.uuid
    end
  end

  class FetchContext < Context; end
  class IngestContext < Context; end
end
