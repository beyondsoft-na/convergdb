require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

# required for testing
require 'minitest'
require 'minitest/autorun'

# imports the ruby file we are testing
require_relative '../lib/generators/generate.rb'
require_relative 'helpers/dsd_ddd_ir/test_dsd_ddd_irs.rb'

module ConvergDB
  module Generators
    class TestMaster < Minitest::Test
      def master_generator
        MasterGenerator.new(TestIR.dsd_ddd_test_03)
      end

      def test_master_generator
        assert_equal(
          MasterGenerator,
          master_generator.class
        )
      end

      def test_create_generators
        g = master_generator
        a = g.create_generators(
          g.structure
        )

        assert_equal(
          Array,
          a.class
        )

        a.each do |this_g|
          assert_equal(
            true,
            this_g.is_a?(BaseGenerator)
          )
        end
      end
    end
  end
end
