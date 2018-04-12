require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

require 'minitest'
require 'minitest/autorun'
require_relative '../lib/exceptions.rb'

module ConvergDB
  module Exceptions
    class ExceptionsTest < Minitest::Test
      include ConvergDB::ErrorHandling

      # log and raise error test
      def test_log_and_raise_error
        assert_raises(ZeroDivisionError) do
          log_and_raise_error do
            1 / 0
          end
        end
      end

      # log and warning error test
      def test_log_warning
        res = capture_subprocess_io do
          log_warning do
            1 / 0
          end
        end
        assert_match %r%WARN -- : ZeroDivisionError%, res.to_s
      end

      # ingore error test
      def test_ignore_error
        res = ignore_error do
          1 / 0
        end
        assert_equal(false, res.class == Exception)
      end
    end
  end
end
