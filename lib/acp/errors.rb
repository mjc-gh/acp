# frozen_string_literal: true

require "timeout"

module Acp
  class Error < StandardError; end
  class ConfigurationError < Error; end
  class DefinitionError < ConfigurationError; end
  class RuntimeError < Error; end
  class InvalidBatchError < RuntimeError; end
  class CursorRegressionError < RuntimeError; end
  class CancellationError < StandardError; end
  class LeaseLostError < Error; end
  class ProgressConflictError < Error; end
  class MissingProgressError < Error; end
  class StageTimeoutError < Timeout::Error; end
end
