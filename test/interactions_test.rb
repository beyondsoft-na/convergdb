require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

# required for testing
require 'minitest'
require 'minitest/autorun'
require 'json'
require 'hashdiff'

# imports the ruby file we are testing
require_relative '../lib/interactions/interactions.rb'
require_relative '../lib/convergdb.rb'
require_relative '../lib/version.rb'

module ConvergDB
  module Interactions
    class TestGenerate < Minitest::Test
      def dsd_paths
        ["#{File.dirname(__FILE__)}/fixtures/interactions/generate/books.schema"]
      end

      def ddd_paths
        ["#{File.dirname(__FILE__)}/fixtures/interactions/generate/books.deployment"]
      end

      def interaction_generate
        ConvergDB::Interactions::Generate.new(
          dsd_paths,
          ddd_paths,
          '/tmp'
        )
      end

      def dsd_structure
        JSON.parse(
          File.read(
            "#{File.dirname(__FILE__)}/fixtures/interactions/generate/dsd.json"
          ),
          :symbolize_names => true
        )
      end

      def ddd_structure
        JSON.parse(
          File.read(
            "#{File.dirname(__FILE__)}/fixtures/interactions/generate/ddd.json"
          ),
          :symbolize_names => true
        )
      end

      def ir_structure
        JSON.parse(
          File.read(
            "#{File.dirname(__FILE__)}/fixtures/interactions/generate/ir.json"
          ),
          :symbolize_names => true
        )
      end

      def test_dsd
        t = interaction_generate
        dsd = t.dsd(dsd_paths)

        assert_equal(
          dsd_structure.to_json,
          dsd.to_json
        )
      end

      def test_ddd
        t = interaction_generate
        ddd = t.ddd(ddd_paths)

        assert_equal(
          ddd_structure.to_json,
          ddd.to_json,
          JSON.pretty_generate(
            HashDiff.diff(
              ddd_structure, 
              ddd
            )
          )
        )
      end

      def test_ir
        t = interaction_generate
        ir = t.ir(
          t.dsd(dsd_paths),
          t.ddd(ddd_paths)
        )

        assert_equal(
          JSON.parse(ir_structure.to_json),
          JSON.parse(ir.to_json),
          HashDiff.diff(
            JSON.parse(ir_structure.to_json), 
            JSON.parse(ir.to_json)
          )
        )
      end

      def test_apply_working_path!
        t = interaction_generate
        ir = t.ir(
          t.dsd(dsd_paths),
          t.ddd(ddd_paths)
        )

        t.apply_working_path!(
          ir,
          '/tmp/test'
        )

        ir.each_key do |k|
          assert_equal(
            '/tmp/test',
            ir[k][:working_path]
          )
        end
      end

      def test_initialize
        t = interaction_generate

        assert_equal(
          ConvergDB::Interactions::Generate,
          t.class
        )

        assert_equal(
          ConvergDB::Generators::MasterGenerator,
          t.master_generator.class
        )
      end

      def test_banner
        t = interaction_generate

        assert_equal(
          %q{
                                         _ _
                                        | | |
   ___ ___  _ ____   _____ _ __ __ _  __| | |__
  / __/ _ \| '_ \ \ / / _ \ '__/ _` |/ _` | '_ \
 | (_| (_) | | | \ V /  __/ | | (_| | (_| | |_) |
  \___\___/|_| |_|\_/ \___|_|  \__, |\__,_|_.__/
                                __/ |
                               |___/             },
          t.banner
        )
      end
    end
  end
end
