require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

require 'minitest'
require 'minitest/autorun'

require_relative '../lib/ir/live/live.rb'

module ConvergDB
  module LiveState
    class IRTest < Minitest::Test

    end
  end
end