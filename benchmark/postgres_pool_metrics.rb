# frozen_string_literal: true

# Observe real pool acquisition without leasing a connection in the observer.
# Durations include connection creation/verification as well as checkout waits.
module PostgresPoolMetrics
  def checkout(...)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    record_checkout(super)
  rescue ActiveRecord::ConnectionTimeoutError
    checkout_metrics[:timeouts] += 1
    raise
  ensure
    record_acquisition(started)
  end

  def record_checkout(connection)
    metrics = checkout_metrics
    metrics[:checkouts] += 1
    metrics[:max_busy] = [metrics[:max_busy], stat[:busy]].max
    connection
  end

  def record_acquisition(started)
    metrics = checkout_metrics
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    metrics[:acquisition_seconds_total] += elapsed
    metrics[:acquisition_seconds_max] = [metrics[:acquisition_seconds_max], elapsed].max
  end

  def checkout_metrics
    @checkout_metrics ||= { checkouts: 0, timeouts: 0, max_busy: 0,
                            acquisition_seconds_total: 0.0, acquisition_seconds_max: 0.0 }
  end
end
