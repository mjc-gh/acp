# Repository guidance

| Command | Purpose |
| --- | --- |
| `bundle exec rake` | Run tests and RuboCop (CI/default check) |
| `bundle exec rake test` | Run the test suite |
| `bundle exec ruby -Itest test/test_acp.rb` | Run the focused Acp test file |
| `bundle exec rake rubocop` | Run RuboCop linting |

- This is a Ruby gem (`Acp` namespace); gem code belongs in `lib/acp/`, tests in `test/`. RubyGems requires Ruby >= 3.2 (`acp.gemspec`); CI currently runs Ruby 4.0.6.
- The gem is still a scaffold. `plans/` documents intended design and sequenced implementation phases, not existing APIs or behavior; check code before relying on a plan proposal.
- Set up dependencies with `bin/setup` (`bundle install`).
- Keep Ruby string literals double-quoted as required by `.rubocop.yml`.
- The test scaffold currently contains a deliberately failing placeholder; replace it with meaningful coverage as functionality is added.
