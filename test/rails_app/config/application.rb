# frozen_string_literal: true

require "rails"
require "active_record/railtie"

module AcpRailsTestApp
  class Application < Rails::Application
    config.root = File.expand_path("..", __dir__)
    config.eager_load = false
    config.active_support.isolation_level = :fiber
    config.active_record.dump_schema_after_migration = false
  end
end
