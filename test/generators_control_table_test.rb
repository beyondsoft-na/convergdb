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
          ConvergDB::Deployment::TerraformBuilder.new
        )
      end

      # keep
      def test_generate!
        # no test here because this is a major state changer.
        # create_static_artifacts has it's own test
        # terraform_builder calls have their own tests
        # params for tf builder calls have their own tests
      end

      def test_aws_glue_table_module_params
        a = athena_control_table_generator
        assert_equal(
          {
            resource_id: a.aws_glue_table_module_resource_id(
              a.structure[:full_relation_name]
            ),
            region: "${var.region}",
            athena_relation_module_name: a.terraform_builder.to_underscore(
              a.structure[:full_relation_name]
            ),
            structure: a.cfn_table_resource(a.structure),
            working_path: './'
          },
          a.aws_glue_table_module_params(a.structure, a.terraform_builder)
        )
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
          "s3://fakedata-state.beyondsoft.us/${deployment_id}/state/production.test_database.test_schema.books_target/control",
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
            convergdb_deployment_id: %{${deployment_id}},
            convergdb_database_cf_id: "${database_stack_id}"
          },
          a.tblproperties(a.structure)
        )
      end

      def test_athena_database_name
        a = athena_control_table_generator
        assert_equal(
          "convergdb_control_${deployment_id}",
          a.athena_database_name
        )
      end

      def test_cfn_table_resource
        a = athena_control_table_generator
        assertion = {
          # resource name is hashed from the :full_relation_name to avoid conflicts
          "convergdbControlTable#{Digest::SHA256.hexdigest(a.structure[:full_relation_name])}" => {
            "Type" => "AWS::Glue::Table",
            "Properties" => {
              # terraform will populate this for you based upon the aws account
              "CatalogId" => "${aws_account_id}",
              "DatabaseName" => a.athena_database_name,
              "TableInput" => {
                "StorageDescriptor" => {
                  "OutputFormat" => a.output_format(a.storage_format),
                  "SortColumns" => [],
                  "InputFormat" => a.input_format(a.storage_format),
                  "SerdeInfo" => {
                    "SerializationLibrary" => a.serialization_library(
                      a.storage_format
                    ),
                    "Parameters" => {
                      "serialization.format" => "1"
                    }
                  },
                  "BucketColumns" => [],
                  "Parameters" => {},
                  "SkewedInfo" => {
                    "SkewedColumnNames" => [],
                    "SkewedColumnValueLocationMaps" => {},
                    "SkewedColumnValues" => []},
                  "Location" => a.s3_storage_location(a.structure),
                  "NumberOfBuckets" => -1,
                  "StoredAsSubDirectories" => false,
                  'Columns' => a.control_table_attributes.map do |b|
                    {
                      'Name' => b[:name],
                      'Type' => a.athena_data_type(b[:data_type]),
                      'Comment' => b[:expression] || ''
                    }
                  end,
                  "Compressed" => false
                },
                "PartitionKeys" => [],
                "Name" => a.table_name(a.structure[:full_relation_name]),
                "Parameters" => a.tblproperties(a.structure),
                "TableType" => "EXTERNAL_TABLE",
                "Owner" => "hadoop",
                "Retention" => 0
              }
            }
          }
        }

        assert_equal(
          assertion,
          a.cfn_table_resource(a.structure),
          puts(
            HashDiff.diff(
              assertion,
              a.cfn_table_resource(a.structure)
            )
          )
        )
      end

      def test_cfn_database_resource
        a = athena_control_table_generator

        assertion = {
          # resource name is hashed from the :full_relation_name to avoid conflicts
          "convergdbDatabase#{Digest::SHA256.hexdigest(a.athena_database_name)}" => {
            "Type" => "AWS::Glue::Database",
            "Properties" => {
              # terraform will populate this for you based upon the aws account
              "CatalogId" => "${data.aws_caller_identity.current.account_id}",
              "DatabaseInput" => {
                "Name" => a.database_name,
                "Parameters" => {
                  "convergdb_deployment_id" => '${var.deployment_id}'
                }
              }
            }
          }
        }

        assert_equal(
          assertion,
          a.cfn_database_resource(a.structure),
          puts(
            HashDiff.diff(
              assertion,
              a.cfn_database_resource(a.structure)
            )
          )
        )
      end

      def test_aws_glue_table_module_resource_id
        a = athena_control_table_generator

        [
          {input: 'abc', expected: 'relations-62'},
          {input: 'abc', expected: 'relations-62'},
          {input: 'chicken', expected: 'relations-68'},
          {input: 'environment.domain.schema.relation', expected: 'relations-3a'}
        ].each do |t|
          assert_equal(
            t[:expected],
            a.aws_glue_table_module_resource_id(t[:input])
          )
        end
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

      def test_database_name
        a = athena_control_table_generator

        assert_equal(
          "convergdb_control_${var.deployment_id}",
          a.database_name
        )
      end

      def test_storage_format
        a = athena_control_table_generator

        assert_equal(
          'json',
          a.storage_format
        )
      end

      def test_athena_database_tf_module_name
        a = athena_control_table_generator

        assert_equal(
          'convergdb_athena_databases_stack',
          a.athena_database_tf_module_name
        )
      end
    end
  end
end
