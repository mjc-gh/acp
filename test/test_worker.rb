# frozen_string_literal: true

require "test_helper"
require "acp/worker"
require "open3"
require "rbconfig"
require "timeout"

class TestWorker < Minitest::Test
  def test_help_is_available_without_booting_rails
    output = StringIO.new
    error = StringIO.new

    result = Acp::Worker.new(["--help"], output: output, error: error).run

    assert_equal 0, result
    assert_includes output.string, "Usage: acp-worker"
    assert_includes output.string, "--program"
    assert_empty error.string
  end

  def test_startup_requires_program_and_transaction_owner
    error = StringIO.new

    result = Acp::Worker.new([], environment: {}, error: error).run

    assert_equal 78, result
    assert_includes error.string, "ACP_PROGRAM"
  end

  def test_omitted_capacity_overrides_use_program_defaults
    worker = Acp::Worker.new(["--program", "Sync", "--transaction-owner", "Owner"], environment: {})
    options = worker.send(:parse_options)

    assert_equal 300, worker.send(:integer_option, options, "fetch-concurrency", 300)
    assert_equal 5, worker.send(:integer_option, options, "ingest-concurrency", 5)
    assert_equal 4, worker.send(:integer_option, options, "resolve-concurrency", 4)
  end

  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
  def test_sigterm_drains_the_active_cycle_in_a_subprocess
    fixture = File.expand_path("fixtures/worker_signal.rb", __dir__)
    stdin, stdout, stderr, process = Open3.popen3(
      RbConfig.ruby, "-Ilib", fixture,
      "--rails", "unused", "--program", "WorkerSignalProgram", "--transaction-owner", "FakeOwner"
    )
    stdin.close

    assert_equal "FETCH_STARTED", Timeout.timeout(10) { stdout.gets&.strip }
    Process.kill("TERM", process.pid)
    output = Timeout.timeout(10) { stdout.read }
    status = process.value

    assert_includes output, "FETCH_FINISHED"
    assert_includes output, "WORKER_SHUTDOWN"
    assert status.success?, stderr.read
  ensure
    Process.kill("TERM", process.pid) if process&.alive?
    stdin&.close
    stdout&.close
    stderr&.close
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength
end
