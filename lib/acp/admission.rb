# frozen_string_literal: true

require "async/notification"
require "digest"

module Acp
  # Reactor-local reservations. Waiting retries retain a bounded reservation;
  # fresh tenants never create tasks until a pipeline reservation is available.
  class Admission
    Reservation = Struct.new(:tenant_id, :group, :active, :retry, :ready, keyword_init: true)

    def initialize(configuration, metrics)
      @configuration = configuration
      @metrics = metrics
      @groups = Hash.new(0)
      @reservations = {}
      @ready = []
      @prefer_retry = false
      %i[pipeline_reservations retry_reservations parked_cycles upstream_groups].each { |name| set(name, 0) }
    end

    def group_for(tenant_id)
      callback = @configuration.callbacks[:upstream_group]
      return nil unless callback

      key = callback.call(tenant_id)
      unless key.is_a?(String) && key.bytesize.between?(1, 256)
        raise ConfigurationError, "upstream_group must return a nonempty String of at most 256 bytes"
      end

      Digest::SHA256.hexdigest(key)
    end

    def available?(group = nil)
      @metrics[:pipeline_reservations] < @configuration.pipeline_capacity &&
        (!group || @groups[group] < @configuration.upstream_capacity)
    end

    def fresh(tenant_id, group)
      reservation = Reservation.new(tenant_id: tenant_id, group: group, active: false,
                                    retry: false, ready: Async::Notification.new)
      @reservations[tenant_id] = reservation
      activate(reservation)
      @prefer_retry = true
      reservation
    end

    def retry_candidate
      @ready.find { |reservation| available?(reservation.group) }
    end

    def prefer_retry?
      @prefer_retry
    end

    def resume(reservation)
      @ready.delete(reservation)
      set(:parked_cycles, @metrics[:parked_cycles] - 1)
      activate(reservation)
      @prefer_retry = false
      reservation.ready.signal
    end

    def enqueue(reservation)
      @ready << reservation if @reservations[reservation.tenant_id].equal?(reservation)
    end

    def park(tenant_id)
      reservation = @reservations[tenant_id]
      return unless reservation

      unless reservation.retry
        return if @metrics[:retry_reservations] >= @configuration.retry_capacity

        reservation.retry = true
        set(:retry_reservations, @metrics[:retry_reservations] + 1)
      end
      deactivate(reservation)
      set(:parked_cycles, @metrics[:parked_cycles] + 1)
      reservation
    end

    def release(reservation)
      @reservations.delete(reservation.tenant_id)
      @ready.delete(reservation)
      if reservation.active
        deactivate(reservation)
      else
        set(:parked_cycles, @metrics[:parked_cycles] - 1)
      end
      set(:retry_reservations, @metrics[:retry_reservations] - 1) if reservation.retry
    end

    private

    def activate(reservation)
      reservation.active = true
      @groups[reservation.group] += 1 if reservation.group
      set(:pipeline_reservations, @metrics[:pipeline_reservations] + 1)
      set(:upstream_groups, @groups.length)
    end

    def deactivate(reservation)
      reservation.active = false
      if reservation.group
        @groups[reservation.group] -= 1
        @groups.delete(reservation.group) if @groups[reservation.group].zero?
      end
      set(:pipeline_reservations, @metrics[:pipeline_reservations] - 1)
      set(:upstream_groups, @groups.length)
    end

    def set(name, value)
      @metrics[name] = value
      @metrics[:"max_#{name}"] = [@metrics.fetch(:"max_#{name}", 0), value].max
      @metrics[:pipeline_utilization] = @metrics[:pipeline_reservations].fdiv(@configuration.pipeline_capacity)
    end
  end
end
