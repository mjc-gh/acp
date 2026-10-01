# frozen_string_literal: true

require "bundler/gem_tasks"
require "minitest/test_task"
require "rbconfig"

Minitest::TestTask.create

namespace :benchmark do
  desc "Run the local seeded Async runtime workload (configure with ACP_BENCH_* variables)"
  task :runtime do
    sh RbConfig.ruby, "benchmark/runtime.rb"
  end

  desc "Run seeded workloads through multiple workers, Redis, and PostgreSQL"
  task :services do
    sh RbConfig.ruby, "integration/benchmark_services.rb"
  end
end

# Package verification inspects and executes the locally installed worker.
# rubocop:disable Metrics/BlockLength
namespace :package do
  desc "Build the gem, inspect its contents, and install its executable into a temporary directory"
  task :verify do
    require "tmpdir"
    require "open3"
    require "rubygems/package"
    require "rubygems/installer"

    Dir.mktmpdir("acp-package-check") do |directory|
      gem_path = File.join(directory, "acp.gem")
      sh Gem.ruby, "-S", "gem", "build", "acp.gemspec", "--output", gem_path
      contents = Gem::Package.new(gem_path).contents
      required_files = %w[README.md CHANGELOG.md exe/acp-worker lib/acp.rb lib/acp/runtime.rb lib/acp/redis.rb]
      missing_files = required_files - contents
      raise "gem is missing required files: #{missing_files.join(", ")}" unless missing_files.empty?

      install_path = File.join(directory, "installed")
      Gem::Installer.at(gem_path, install_dir: install_path, wrappers: true).install
      executable = File.join(install_path, "bin", "acp-worker")
      raise "installed gem did not create acp-worker" unless File.executable?(executable)

      gem_path_value = ([install_path] + Gem.path).uniq.join(File::PATH_SEPARATOR)
      output, error, status = Open3.capture3(
        { "GEM_HOME" => install_path, "GEM_PATH" => gem_path_value }, executable, "--help"
      )
      raise "installed acp-worker did not run: #{error}" unless status.success? && output.include?("Usage: acp-worker")

      puts "gem contents (#{contents.length} files):"
      puts(contents.sort.map { |file| "  #{file}" })
      puts "installed executable verified: #{executable} --help"
    end
  end
end
# rubocop:enable Metrics/BlockLength

namespace :test do
  Minitest::TestTask.create(:postgres) do |task|
    task.libs << "integration"
    task.test_globs = ["integration/**/*_test.rb"]
  end

  Minitest::TestTask.create(:redis) do |task|
    task.libs << "integration"
    task.test_globs = ["integration/redis_test.rb"]
  end
end

require "rubocop/rake_task"

RuboCop::RakeTask.new

task default: %i[test rubocop]
