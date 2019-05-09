require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

# required for testing
require 'minitest'
require 'minitest/autorun'

require 'fileutils'

# imports the ruby files we are testing
require_relative '../lib/generators/generate.rb'
require_relative '../lib/generators/athena/athena.rb'
require_relative 'helpers/dsd_ddd_ir/test_dsd_ddd_irs.rb'
require_relative '../lib/deployment/terraform/terraform.rb'

module ConvergDB
  module Generators
    class TestAWSAthenaControlTableGenerator < Minitest::Test
      # creates an AWSAthena generator object for you
      def athena_control_table_generator
        AWSAthenaControlTableGenerator.new(
          TestIR.dsd_ddd_test_02,
          ConvergDB::Deployment::TerraformBuilder.new,
          nil
        )
      end

      # keep
      def test_generate!
        # no test here because this is a major state changer.
        # create_static_artifacts has it's own test
        # terraform_builder calls have their own tests
        # params for tf builder calls have their own tests
      end

      def file_contents_match?(file1, file2)
        File.read(file1) == File.read(file2)
      end

      def test_table_name
        a = athena_control_table_generator
        assert_equal(
          'this__is__hello',
          a.table_name('this.is.hello')
        )
      end

      def test_s3_storage_location
        a = athena_control_table_generator
        assert_equal(
          "s3://fakedata-state.beyondsoft.us/${var.deployment_id}/state/production.test_database.test_schema.books_target/control",
          a.s3_storage_location(a.structure)
        )
      end

      def test_tblproperties
        a = athena_control_table_generator
        assert_equal(
          {
            # required by glue
            classification: 'json',

            # required by athena
            EXTERNAL: 'TRUE',

            # required by convergdb
            convergdb_full_relation_name: 'production.test_database.test_schema.books_target',
            convergdb_dsd: 'test_database.test_schema.books_target',
            convergdb_storage_bucket: 'fakedata-state.beyondsoft.us',
            convergdb_storage_format: 'parquet',
            convergdb_etl_job_name: 'test_etl_job',
            convergdb_deployment_id: %{${var.deployment_id}}
          },
          a.tblproperties(a.structure)
        )
      end

      def test_athena_database_name
        a = athena_control_table_generator
        assert_equal(
          "convergdb_control_${var.deployment_id}",
          a.athena_database_name(nil)
        )
      end

      def test_control_table_attributes
        a = athena_control_table_generator
        assertion =         [
          {
            name: 'convergdb_batch_id',
            data_type: 'varchar(64)',
            expression: ''
          },
          {
            name: 'batch_start_time',
            data_type: 'timestamp',
            expression: ''
          },
          {
            name: 'batch_end_time',
            data_type: 'timestamp',
            expression: ''
          },
          {
            name: 'source_type',
            data_type: 'varchar(64)',
            expression: ''
          },
          {
            name: 'source_format',
            data_type: 'varchar(64)',
            expression: ''
          },
          {
            name: 'source_relation',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'source_bucket',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'source_key',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'load_type',
            data_type: 'varchar(64)',
            expression: ''
          },
          {
            name: 'status',
            data_type: 'varchar(64)',
            expression: ''
          }
        ]

        assert_equal(
          assertion,
          a.control_table_attributes
        )
      end

      def test_storage_format
        a = athena_control_table_generator

        assert_equal(
          'json',
          a.storage_format
        )
      end
    end
  end
end
