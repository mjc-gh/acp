# frozen_string_literal: true

require "acp/worker"

module Rails
  def self.application
    @application ||= Object.new
  end
end

class WorkerSignalProgress
  def acquire(_tenant_id)
    true
  end

  def release(_tenant_id, _token); end

  def read(_tenant_id)
    @cursor
  end

  def initialize_cursor(_tenant_id, cursor)
    @initialize_cursor ||= cursor
  end

  def acknowledge(_tenant_id, _poll_id, cursor)
    @initialize_cursor = cursor
  end
end

class WorkerSignalProgram < Acp::Program
  interval 0.01
  fetch_concurrency 1
  ingest_concurrency 1
  pipeline_capacity 1
  tenants do |emit|
    emit.call("tenant")
  end
  initial_cursor { |_tenant_id| Time.utc(2025, 1, 1) }
  resolve { |tenant_id| tenant_id }
  fetch do |_tenant, context|
    puts "FETCH_STARTED"
    $stdout.flush
    Kernel.sleep(0.2)
    puts "FETCH_FINISHED"
    $stdout.flush
    Acp::Batch.new(data: [], next_cursor: context.cursor)
  end
  ingest { |_batch, _context| }

  def self.acp_shutdown
    puts "WORKER_SHUTDOWN"
    $stdout.flush
  end
end

class SignalTestWorker < Acp::Worker
  private

  def boot_rails(_path); end

  def configuration_for(_options)
    WorkerSignalProgram.configuration
  end

  def coordinator_for(_options, _configuration)
    WorkerSignalProgress.new
  end

  def call_consumer_shutdown(_options)
    WorkerSignalProgram.acp_shutdown
  end
end

exit SignalTestWorker.new(ARGV).run
