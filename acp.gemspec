# frozen_string_literal: true

require_relative "lib/acp/version"

Gem::Specification.new do |spec|
  spec.name = "acp"
  spec.version = Acp::VERSION
  spec.authors = ["Michael Coyne"]
  spec.email = ["mjc@hey.com"]

  spec.summary = "A bounded asynchronous tenant-polling runtime."
  spec.description = "Coordinate tenant polling with Async, transactional Rails ingestion, and Redis-backed " \
                     "ownership and progress."
  spec.required_ruby_version = ">= 3.2.0"

  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) || %w[AGENTS.md Rakefile compose.yaml].include?(f) ||
        f.start_with?(*%w[bin/ Gemfile .gitignore test/ integration/ benchmark/ plans/ .github/ .rubocop.yml])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "async", ">= 2.35", "< 2.38"
  spec.add_dependency "redis", ">= 5.0", "< 6.0"

  # For more information and examples about making a new gem, check out our
  # guide at: https://guides.rubygems.org/make-your-own-gem/
end
