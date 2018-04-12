require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

require 'minitest'
require 'minitest/autorun'

class ConvergDBTest < Minitest::Test
  def catch_error
    yield
  rescue => e
    return e
  end

  # not a robust test... but it insures that the files to be required
  # are real files that exists in the codebase.
  def test_require
    e = catch_error { require_relative '../lib/convergdb.rb' }
    assert_equal(
      true,
      [true, false].include?(e)
    )
  end
end
