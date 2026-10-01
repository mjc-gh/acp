# frozen_string_literal: true

source "https://rubygems.org"

# Specify your gem's dependencies in acp.gemspec
gemspec

gem "irb"
gem "rake", "~> 13.0"

gem "minitest", "~> 5.16"

# Rails/PostgreSQL are only loaded by the explicit Acp::Rails integration.
gem "async", *ENV.fetch("ACP_ASYNC_CONSTRAINT", ">= 2.35, < 2.38").split(",").map(&:strip)
gem "pg", *ENV.fetch("ACP_PG_CONSTRAINT", "~> 1.5").split(",").map(&:strip)
gem "rails", *ENV.fetch("ACP_RAILS_CONSTRAINT", ">= 8.0, < 9.0").split(",").map(&:strip)

gem "rubocop", "~> 1.21"
gem "rubocop-minitest"
gem "rubocop-rake"
