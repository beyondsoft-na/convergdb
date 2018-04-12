require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

# required for testing
require 'minitest'
require 'minitest/autorun'
require 'digest'
require 'fileutils'

# imports the ruby files we are testing
require_relative '../lib/generators/generate.rb'
require_relative '../lib/generators/athena/athena.rb'
require_relative 'helpers/dsd_ddd_ir/test_dsd_ddd_irs.rb'
require_relative '../lib/deployment/terraform/terraform.rb'

module ConvergDB
  module Generators
    class TestAWSAthena < Minitest::Test
      # creates an AWSAthena generator object for you
      def athena_generator
        AWSAthena.new(
          TestIR.dsd_ddd_test_02,
          ConvergDB::Deployment::TerraformBuilder.new
        )
      end

      # there needs to be more here.
      def test_message
        a = athena_generator
        a.current_state = {}
        msg = a.message(
          a.structure,
          a.current_state,
          HashDiff.diff(a.current_state, a.comparable(a.structure))
        )

        # adding a fresh relation
        expected = [
          Rainbow('athena: production.test_database.test_schema.books_target').bright.green + "\n",
          Rainbow("  new relation").green + "\n",
          Rainbow("  + attributes = [{:name=>\"title\", :data_type=>\"string\", :expression=>\"#{Digest::MD5.new.hexdigest('title')}\"}, {:name=>\"author\", :data_type=>\"string\", :expression=>\"#{Digest::MD5.new.hexdigest('author')}\"}, {:name=>\"publisher\", :data_type=>\"string\", :expression=>\"#{Digest::MD5.new.hexdigest('publisher')}\"}, {:name=>\"genre\", :data_type=>\"string\", :expression=>\"#{Digest::MD5.new.hexdigest('genre')}\"}]").green + "\n",
          Rainbow("  + dsd = test_database.test_schema.books_target").green + "\n",
          Rainbow("  + etl_job_name = test_etl_job").green + "\n",
          Rainbow("  + full_relation_name = production.test_database.test_schema.books_target").green + "\n",
          Rainbow("  + state_bucket = fakedata-state.beyondsoft.us").green + "\n",
          Rainbow("  + storage_bucket = fakedata-target.beyondsoft.us").green + "\n",
          Rainbow("  + storage_format = parquet").green + "\n",
          "\n"
        ]

        assert_equal(
          expected,
          msg,
          puts(expected)
        )
      end

      def test_diff_item_coloring
        a = athena_generator

        [
          {
            diff_record: ['+', 'this', 'that'],
            colored: (Rainbow("  + this = that").green + "\n")
          },
          {
            diff_record: ['-', 'this', 'that'],
            colored: Rainbow("  - this = that").red + "\n"
          },
          {
            diff_record: ['~', 'this', 'that', 'the other'],
            colored: "  ~ this from 'that' to 'the other'" + "\n"
          }
        ].each do |d|
          assert_equal(
            d[:colored],
            a.diff_item_coloring(
              d[:diff_record]
            )
          )
        end
      end

      def test_generate!
        # no test here because this is a major state changer.
        # create_static_artifacts has it's own test
        # terraform_builder calls have their own tests
        # params for tf builder calls have their own tests
      end

      def test_aws_glue_database_module_params
        a = athena_generator
        assert_equal(
          {
            resource_id: "convergdb_athena_databases_stack",
            region: "${var.region}",
            athena_database_tf_module_name: 'convergdb_athena_databases_stack',
            structure: a.cfn_database_resource(a.structure)
          },
          a.aws_glue_database_module_params(a.structure)
        )
      end

      def test_aws_glue_table_module_params
        a = athena_generator
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

      def test_athena_database_tf_module_name
        a = athena_generator
        assert_equal(
          'convergdb_athena_databases_stack',
          a.athena_database_tf_module_name
        )
      end

      def file_contents_match?(file1, file2)
        File.read(file1) == File.read(file2)
      end

      def test_create_static_artifacts!
        test_working_path = '/tmp/athena_test_generate'
        FileUtils.rm_r(test_working_path) rescue nil
        FileUtils.mkdir_p(test_working_path)

        a = athena_generator
        # files should be created at this point

        a.create_static_artifacts!(test_working_path)

        # validate that modules were copied
        [
          'modules/aws_athena_database/main.tf',
          'modules/aws_athena_database/variables.tf',
          'modules/aws_athena_relations/main.tf',
          'modules/aws_athena_relations/variables.tf'
        ].each do |tf|
          orig = File.expand_path(
            "#{File.dirname(__FILE__)}/../lib/generators/athena/#{tf}"
          )
          assert_equal(
            true,
            file_contents_match?(
              orig,
              "#{test_working_path}/terraform/#{tf}"
            )
          )
        end
      ensure
        FileUtils.rm_r(test_working_path) rescue nil
      end

      def test_table_name
        a = athena_generator
        assert_equal(
          'hello',
          a.table_name('this.is.hello')
        )
      end

      def test_s3_storage_location
        a = athena_generator
        assert_equal(
          "s3://fakedata-target.beyondsoft.us/",
          a.s3_storage_location(a.structure)
        )
      end

      def test_athena_data_type
        g = athena_generator
        [
          {t: 'Varchar(100)', r: 'string'},
          {t: 'Char(100)', r: 'string'},
          {t: 'Character varying(100)', r: 'string'},
          {t: 'Tinyint', r: 'tinyint'},
          {t: 'Smallint', r: 'smallint'},
          {t: 'Int', r: 'int'},
          {t: 'Integer', r: 'int'},
          {t: 'Bigint', r: 'bigint'},
          {t: 'Boolean', r: 'boolean'},
          {t: 'Float', r: 'double'},
          {t: 'Double', r: 'double'},
          {t: 'Timestamp', r: 'timestamp'},
          {t: 'Datetime', r: 'timestamp'},
          {t: 'Date', r: 'date'},
          {t: 'Numeric(10,2)', r: 'decimal(10,2)'},
          {t: 'Decimal(10,2)', r: 'decimal(10,2)'},
          {t: 'numeric(10,2)', r: 'decimal(10,2)'},
          {t: 'decimal(10,2)', r: 'decimal(10,2)'}
        ].each do |h|
          assert_equal(
            h[:r],
            g.athena_data_type(h[:t])
          )
        end
      end

      def test_tblproperties
        a = athena_generator
        assert_equal(
          {
            # required by glue
            classification: 'parquet',

            # required by athena
            EXTERNAL: 'TRUE',

            # required by convergdb
            convergdb_full_relation_name: 'production.test_database.test_schema.books_target',
            convergdb_dsd: 'test_database.test_schema.books_target',
            convergdb_storage_bucket: 'fakedata-target.beyondsoft.us',
            convergdb_state_bucket: 'fakedata-state.beyondsoft.us',
            convergdb_storage_format: 'parquet',
            convergdb_etl_job_name: 'test_etl_job',
            convergdb_deployment_id: %{${deployment_id}},
            convergdb_database_cf_id: "${database_stack_id}"
          },
          a.tblproperties(a.structure)
        )
      end

      def test_athena_database_name
        a = athena_generator
        assert_equal(
          'production__test_database__test_schema',
          a.athena_database_name(a.structure)
        )
      end

      def test_comparable
        a = athena_generator
        assert_equal(
          {
            full_relation_name: 'production.test_database.test_schema.books_target',
            dsd: 'test_database.test_schema.books_target',
            # temporarily removed until i can figure out how to
            # handle terraform vars
            storage_bucket: 'fakedata-target.beyondsoft.us',
            state_bucket: 'fakedata-state.beyondsoft.us',
            storage_format: 'parquet',
            etl_job_name: 'test_etl_job',
            attributes: [
              {
                name: 'title',
                data_type: a.athena_data_type('varchar(100)'),
                expression: Digest::MD5.new.hexdigest('title')
              },
              {
                name: 'author',
                data_type: a.athena_data_type('varchar(100)'),
                expression: Digest::MD5.new.hexdigest('author')
              },
              {
                name: 'publisher',
                data_type: a.athena_data_type('varchar(100)'),
                expression: Digest::MD5.new.hexdigest('publisher')
              },
              {
                name: 'genre',
                data_type: a.athena_data_type('varchar(100)'),
                expression: Digest::MD5.new.hexdigest('genre')
              }
            ]
          },
          a.comparable(a.structure)
        )
      end

      def test_output_format
        a = athena_generator

        assert_equal(
          'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
          a.output_format('json')
        )

        assert_equal(
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          a.output_format('parquet')
        )
      end

      def test_input_format
        a = athena_generator

        assert_equal(
          'org.apache.hadoop.mapred.TextInputFormat',
          a.input_format('json')
        )

        assert_equal(
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          a.input_format('parquet')
        )
      end

      def test_serialization_library
        a = athena_generator

        assert_equal(
          'org.apache.hive.hcatalog.data.JsonSerDe',
          a.serialization_library('json')
        )

        assert_equal(
          'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          a.serialization_library('parquet')
        )
      end

      def test_cfn_table_resource
        a = athena_generator
        assertion = {
          # resource name is hashed from the :full_relation_name to avoid conflicts
          "convergdbTable#{Digest::SHA256.hexdigest(a.structure[:full_relation_name])}" => {
            "Type" => "AWS::Glue::Table",
            "Properties" => {
              # terraform will populate this for you based upon the aws account
              "CatalogId" => "${aws_account_id}",
              "DatabaseName" => a.athena_database_name(a.structure),
              "TableInput" => {
                "StorageDescriptor" => {
                  "OutputFormat" => a.output_format(a.structure[:storage_format]),
                  "SortColumns" => [],
                  "InputFormat" => a.input_format(a.structure[:storage_format]),
                  "SerdeInfo" => {
                    "SerializationLibrary" => a.serialization_library(
                      a.structure[:storage_format]
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
                  "Columns" => [
                    {
                      'Name' => 'title',
                      'Type' => a.athena_data_type('varchar(100)'),
                      'Comment' => Digest::MD5.new.hexdigest('title')
                    },
                    {
                      'Name' => 'author',
                      'Type' => a.athena_data_type('varchar(100)'),
                      'Comment' => Digest::MD5.new.hexdigest('author')
                    },
                    {
                      'Name' => 'publisher',
                      'Type' => a.athena_data_type('varchar(100)'),
                      'Comment' => Digest::MD5.new.hexdigest('publisher')
                    },
                    {
                      'Name' => 'genre',
                      'Type' => a.athena_data_type('varchar(100)'),
                      'Comment' => Digest::MD5.new.hexdigest('genre')
                    }
                  ],
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
        a = athena_generator

        assertion = {
          # resource name is hashed from the :full_relation_name to avoid conflicts
          "convergdbDatabase#{Digest::SHA256.hexdigest(a.athena_database_name(a.structure))}" => {
            "Type" => "AWS::Glue::Database",
            "Properties" => {
              # terraform will populate this for you based upon the aws account
              "CatalogId" => "${data.aws_caller_identity.current.account_id}",
              "DatabaseInput" => {
                "Name" => a.athena_database_name(a.structure),
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
        a = athena_generator

        [
          {input: 'abc', expected: 'relations-10'},
          {input: 'abc', expected: 'relations-10'},
          {input: 'chicken', expected: 'relations-74'},
          {input: 'environment.domain.schema.relation', expected: 'relations-68'}
        ].each do |t|
          assert_equal(
            t[:expected],
            a.aws_glue_table_module_resource_id(t[:input])
          )
        end
      end

      def test_aws_glue_table_module_resource_id_bucket
        a = athena_generator

        [
          {input: 'abc', expected: '10'},
          {input: 'abc', expected: '10'},
          {input: 'chicken', expected: '74'},
          {input: 'environment.domain.schema.relation', expected: '68'}
        ].each do |t|
          assert_equal(
            t[:expected],
            a.aws_glue_table_module_resource_id_bucket(t[:input])
          )
        end
      end

      def test_non_partition_attributes
        a = athena_generator

        # no partitions defined so all attributes returned
        assert_equal(
          a.structure[:attributes],
          a.non_partition_attributes(a.structure)
        )

        t = a.structure

        t[:partitions] = ['title']

        assert_equal(
          [
            {name: 'author', data_type: 'varchar(100)', expression: 'author'},
            {name: 'publisher', data_type: 'varchar(100)', expression: 'publisher'},
            {name: 'genre', data_type: 'varchar(100)', expression: 'genre'}
          ],
          a.non_partition_attributes(t)
        )
      end

      def test_partition_attributes
        a = athena_generator

        # no partitions defined so no attributes returned
        assert_equal(
          [],
          a.partition_attributes(a.structure)
        )

        t = a.structure

        t[:partitions] = ['title']

        assert_equal(
          [
            {name: 'title', data_type: 'varchar(100)', expression: 'title'}
          ],
          a.partition_attributes(t)
        )
      end

    end
  end
end
