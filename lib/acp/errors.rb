# frozen_string_literal: true

module Acp
  class Error < StandardError; end
  class ConfigurationError < Error; end
  class DefinitionError < ConfigurationError; end
  class RuntimeError < Error; end
  class InvalidBatchError < RuntimeError; end
  class CursorRegressionError < RuntimeError; end
  class CancellationError < StandardError; end
end
