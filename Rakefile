# frozen_string_literal: true

require "bundler/gem_tasks"
require "minitest/test_task"

Minitest::TestTask.create

namespace :test do
  Minitest::TestTask.create(:postgres) do |task|
    task.libs << "integration"
    task.test_globs = ["integration/**/*_test.rb"]
  end
end

require "rubocop/rake_task"

RuboCop::RakeTask.new

task default: %i[test rubocop]
