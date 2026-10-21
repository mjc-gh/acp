# frozen_string_literal: true

module Acp
  # Materialize lazy IDs while still inside the application's page callback scope.
  module DiscoveryPage
    def self.materialize(result, limit)
      unless result.is_a?(Array) && result.length == 2 && result.first.respond_to?(:each)
        raise ArgumentError, "tenants_page must return [enumerable_ids, next_cursor]"
      end

      ids = []
      result.first.each do |id|
        raise ArgumentError, "tenants_page exceeded its requested limit" if ids.length >= limit

        ids << id
      end
      [ids, result.last]
    end
  end
end
