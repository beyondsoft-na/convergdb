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
    class TestStreamingInventoryTableGenerator < Minitest::Test
      # creates an StreamingInventory generator object for you
      def streaming_inventory_generator
        StreamingInventoryTableGenerator.new(
          TestIR.dsd_ddd_test_s3_source,
          ConvergDB::Deployment::TerraformBuilder.new,
          nil
        )
      end

      def test_generate!
        # no test here because this is a major state changer.
        # create_static_artifacts has it's own test
        # terraform_builder calls have their own tests
        # params for tf builder calls have their own tests
      end

      def test_create_static_artifacts!

      end

      def test_storage_format
        t = streaming_inventory_generator

        assert_equal(
          'json',
          t.storage_format
        )
      end

      def test_aws_glue_table_module_resource_id

      end

      def test_streaming_inventory_attributes
        t = streaming_inventory_generator

        expected =         [
          {
            name: 'last_modified_timestamp',
            data_type: 'timestamp',
            expression: ''
          },
          {
            name: 'bucket',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'key',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'size',
            data_type: 'bigint',
            expression: ''
          },
          {
            name: 'e_tag',
            data_type: 'varchar(32)',
            expression: ''
          },
          {
            name: 'sequencer',
            data_type: 'varchar(16)',
            expression: ''
          }
        ]

        assert_equal(
          expected,
          t.streaming_inventory_attributes
        )
      end

      def test_streaming_inventory_module_params
        t = streaming_inventory_generator

        expected = {
          resource_id: t.streaming_inventory_resource_id(t.structure),
          source: './modules/streaming_inventory',
          storage_bucket: 'fakedata-source.beyondsoft.us',
          streaming_inventory_output_bucket: 'bucket/location'
        }

        assert_equal(
          expected,
          t.streaming_inventory_module_params(
            t.structure,
            t.terraform_builder
          )
        )
      end

      def test_streaming_inventory_resource_id
        t = streaming_inventory_generator

        assert_equal(
          'streaming_inventory_fakedata__source__beyondsoft__us',
          t.streaming_inventory_resource_id(t.structure)
        )
      end

      def test_athena_database_tf_module_name
        t = streaming_inventory_generator

        assert_equal(
          'convergdb_athena_databases_stack',
          t.athena_database_tf_module_name
        )
      end

      def test_table_name
        a = streaming_inventory_generator

        assert_equal(
          'this__is__hello',
          a.table_name('this.is.hello')
        )
      end

      def test_inventory_s3_bucket
        t = streaming_inventory_generator

        [
          { input: 'test', expected: 'test' },
          { input: 'test/prefix', expected: 'test' },
          { input: 'this.that', expected: 'this.that' },
          { input: 'this.that/the_other', expected: 'this.that' }
        ].each do |i|
          assert_equal(
            i[:expected],
            t.inventory_s3_bucket(i[:input])
          )
        end
      end

      def s3_storage_location_structure(bucket)
        {
          streaming_inventory_output_bucket: bucket
        }
      end

      def test_s3_storage_location
        t = streaming_inventory_generator

        [
          {
            input: s3_storage_location_structure('bucket'),
            expected: 's3://bucket'
          },
          {
            input: s3_storage_location_structure('bucket/prefix'),
            expected: 's3://bucket/prefix'
          },
          {
            input: s3_storage_location_structure(
              '${var.admin_bucket}/$(var.deployment_id}'
            ),
            expected: 's3://${admin_bucket}/$(deployment_id}'
          }
        ].each do |i|
          assert_equal(
            i[:expected],
            t.s3_storage_location(i[:input])
          )
        end
      end

      def test_tblproperties
        t = streaming_inventory_generator

        expected = {
          classification: 'json',
          EXTERNAL: 'TRUE',
          convergdb_storage_format: 'json',
          convergdb_etl_job_name: '',
          convergdb_deployment_id: %(${deployment_id}),
          convergdb_database_cf_id:
            %(${database_stack_id})
        }

        assert_equal(
          expected,
          t.tblproperties(t.structure)
        )
      end

      def test_athena_database_name
        t = streaming_inventory_generator
        assert_equal(
          "convergdb_inventory_${deployment_id}",
          t.athena_database_name
        )
      end

      def test_database_name
        t = streaming_inventory_generator
        assert_equal(
          "convergdb_inventory_${var.deployment_id}",
          t.database_name
        )
      end

      def test_cfn_table_resource
        t = streaming_inventory_generator

        expected = {
          # hashed from the :full_relation_name to avoid conflicts
          %(convergdbInventoryTablebb5ac62c5d4a9de00207f1add822b2c8d4cdeb48c62f226fbdbe3bd491c5a0f0) => {
            'Type' => 'AWS::Glue::Table',
            'Properties' => {
              # terraform will populate this for you based upon the aws account
              'CatalogId' => '${aws_account_id}',
              'DatabaseName' => "convergdb_inventory_${deployment_id}",
              'TableInput' => {
                'StorageDescriptor' => {
                  'OutputFormat' => t.output_format('json'),
                  'SortColumns' => [],
                  'InputFormat' => t.input_format('json'),
                  'SerdeInfo' => {
                    'SerializationLibrary' => t.serialization_library(
                      'json'
                    ),
                    'Parameters' => {
                      'serialization.format' => '1'
                    }
                  },
                  'BucketColumns' => [],
                  'Parameters' => {},
                  'SkewedInfo' => {
                    'SkewedColumnNames' => [],
                    'SkewedColumnValueLocationMaps' => {},
                    'SkewedColumnValues' => []
                  },
                  'Location' => "s3://bucket/location",
                  'NumberOfBuckets' => -1,
                  'StoredAsSubDirectories' => false,
                  'Columns' => t.streaming_inventory_attributes.map do |a|
                    {
                      'Name' => a[:name],
                      'Type' => t.athena_data_type(a[:data_type]),
                      'Comment' => a[:expression] || ''
                    }
                  end,
                  'Compressed' => false
                },
                'PartitionKeys' => [],
                'Name' => 'fakedata__source__beyondsoft__us',
                # 'Parameters' => tblproperties(structure),
                'TableType' => 'EXTERNAL_TABLE',
                'Owner' => 'hadoop',
                'Retention' => 0
              }
            }
          }
        }

        assert_equal(
          expected,
          t.cfn_table_resource(t.structure)
        )
      end

      def test_cfn_database_resource
        t = streaming_inventory_generator
        expected = {
          # hashed from the :full_relation_name to avoid conflicts
          %(convergdbInventoryDatabase${var.deployment_id}) =>
          {
            'Type' => 'AWS::Glue::Database',
            'Properties' => {
              # terraform will populate this for you based upon the aws account
              'CatalogId' => '${data.aws_caller_identity.current.account_id}',
              'DatabaseInput' => {
                'Name' => "convergdb_inventory_${var.deployment_id}",
                'Parameters' => {
                  'convergdb_deployment_id' =>
                    '${var.deployment_id}'
                }
              }
            }
          }
        }
        assert_equal(
          expected,
          t.cfn_database_resource(t.structure)
        )
      end

      def file_contents_match?(file1, file2)
        File.read(file1) == File.read(file2)
      end
    end
  end
end
