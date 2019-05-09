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
          ConvergDB::Deployment::TerraformBuilder.new,
          nil
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
            colored: Rainbow("  ~ this from 'that' to 'the other'").yellow + "\n"
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


      #! DIFF METHODS
      
      def get_table_response
        {:table=>
          {:name=>"clicks",
           :owner=>"hadoop",
           :create_time=>'2018-03-27 12:19:12 -0700',
           :update_time=>'2018-03-27 12:19:12 -0700',
           :retention=>0,
           :storage_descriptor=>
            {:columns=>
              [{:name=>"id",
                :type=>"string",
                :comment=>"b80bb7740288fda1f201890375a60c8f"},
               {:name=>"event_timestamp",
                :type=>"timestamp",
                :comment=>"a92e415b04d7bdcb9a78d75059e7ae66"},
               {:name=>"impression_id",
                :type=>"string",
                :comment=>"41fd60e1d4c4bc54fae50625fbc018e7"}],
             :location=>
              "s3://convergdb-data-9083c59b16173549/9083c59b16173549/demo.ad_tech.events.clicks/",
             :input_format=>
              "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
             :output_format=>
              "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
             :compressed=>false,
             :number_of_buckets=>-1,
             :serde_info=>
              {:serialization_library=>
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
               :parameters=>{"serialization.format"=>"1"}},
             :bucket_columns=>[],
             :sort_columns=>[],
             :parameters=>{},
             :skewed_info=>
              {:skewed_column_names=>[],
               :skewed_column_values=>[],
               :skewed_column_value_location_maps=>{}},
             :stored_as_sub_directories=>false},
           :partition_keys=>
            [{:name=>"event_date",
              :type=>"date",
              :comment=>"584a94b34670c757e9380efdb61b223d"}],
           :table_type=>"EXTERNAL_TABLE",
           :parameters=>
            {"EXTERNAL"=>"TRUE",
             "classification"=>"parquet",
             "convergdb_database_cf_id"=>
              "arn:aws:cloudformation:us-west-2:692977618922:stack/convergdb-tf-db-9083c59b16173549-11941373370256685717/b0b5ab70-31f3-11e8-80e1-503ac9841a99",
             "convergdb_deployment_id"=>"9083c59b16173549",
             "convergdb_dsd"=>"ad_tech.events.clicks",
             "convergdb_etl_job_name"=>"demo_ad_tech_etl_job",
             "convergdb_full_relation_name"=>"demo.ad_tech.events.clicks",
             "convergdb_state_bucket"=>"convergdb-admin-9083c59b16173549",
             "convergdb_storage_bucket"=>
              "convergdb-data-9083c59b16173549/9083c59b16173549/demo.ad_tech.events.clicks",
             "convergdb_storage_format"=>"parquet"},
           :created_by=>"arn:aws:iam::692977618922:user/zuqing"}}
      end
      
      def test_comparable_aws_table
        a = athena_generator
        expected = {
          dsd: 'ad_tech.events.clicks',
          storage_bucket: a.convergdb_bucket_reference(
            'convergdb-data-9083c59b16173549/9083c59b16173549/demo.ad_tech.events.clicks',
            '9083c59b16173549'
          ),
          state_bucket: a.convergdb_bucket_reference(
            'convergdb-admin-9083c59b16173549',
            '9083c59b16173549'
          ),
          storage_format: 'parquet',
          etl_job_name: 'demo_ad_tech_etl_job',
          attributes: [
            {
              name: "id",
              data_type: "string",
              expression: "b80bb7740288fda1f201890375a60c8f"
            },
            {
              name: "event_timestamp",
              data_type: "timestamp",
              expression: "a92e415b04d7bdcb9a78d75059e7ae66"
            },
            {
              name: "impression_id",
              data_type: "string",
              expression: "41fd60e1d4c4bc54fae50625fbc018e7"
            }
          ],
          partitions: [
            {
              name: 'event_date',
              data_type: 'date',
              expression: '584a94b34670c757e9380efdb61b223d'
            }
          ]
        }
        
        assert_equal(
          expected,
          a.comparable_aws_table(get_table_response)
        )
      end

      def test_aws_glue_database_module_params
        a = athena_generator
        assert_equal(
          {
            resource_id: a.terraform_builder.database_module_name('production__test_database__test_schema'),
            region: "${var.region}",
            structure: {
              database_name: "production__test_database__test_schema"
            }
          },
          a.aws_glue_database_module_params(a.structure)
        )
      end

      def test_aws_glue_table_module_params
        a = athena_generator
        assert_equal(
          {
            resource_id: a.structure[:full_relation_name],
            region: "${var.region}",
            athena_relation_module_name: a.terraform_builder.to_underscore(
              a.structure[:full_relation_name]
            ),
            structure: a.table_parameters(a.structure),
            working_path: './'
          },
          a.aws_glue_table_module_params(a.structure, a.terraform_builder)
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
            convergdb_deployment_id: %{${var.deployment_id}}
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
                expression: "#{Digest::MD5.new.hexdigest('title')}_#{Digest::MD5.new.hexdigest('varchar(100)')}"
              },
              {
                name: 'author',
                data_type: a.athena_data_type('varchar(100)'),
                expression: "#{Digest::MD5.new.hexdigest('author')}_#{Digest::MD5.new.hexdigest('varchar(100)')}"
              },
              {
                name: 'publisher',
                data_type: a.athena_data_type('varchar(100)'),
                expression: "#{Digest::MD5.new.hexdigest('publisher')}_#{Digest::MD5.new.hexdigest('varchar(100)')}"
              },
              {
                name: 'genre',
                data_type: a.athena_data_type('varchar(100)'),
                expression: "#{Digest::MD5.new.hexdigest('genre')}_#{Digest::MD5.new.hexdigest('varchar(100)')}"
              }
            ],
            partitions: []
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

      def test_table_parameters
        a = athena_generator
        
        expected = {
          # database name uses module output to force
          database_name: "${module.#{a.terraform_builder.database_module_name('production__test_database__test_schema')}.database_name}",
          table_name: 'books_target',
          columns: [
            {
              'name' => 'title',
              'type' => a.athena_data_type('varchar(100)'),
              'comment' => "#{Digest::MD5.new.hexdigest('title')}_#{Digest::MD5.new.hexdigest('varchar(100)')}"
            },
            {
              'name' => 'author',
              'type' => a.athena_data_type('varchar(100)'),
              'comment' => "#{Digest::MD5.new.hexdigest('author')}_#{Digest::MD5.new.hexdigest('varchar(100)')}"
            },
            {
              'name' => 'publisher',
              'type' => a.athena_data_type('varchar(100)'),
              'comment' => "#{Digest::MD5.new.hexdigest('publisher')}_#{Digest::MD5.new.hexdigest('varchar(100)')}"
            },
            {
              'name' => 'genre',
              'type' => a.athena_data_type('varchar(100)'),
              'comment' => "#{Digest::MD5.new.hexdigest('genre')}_#{Digest::MD5.new.hexdigest('varchar(100)')}"
            }
          ],
          location: "s3://fakedata-target.beyondsoft.us/",
          input_format: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          output_format: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          compressed: false,
          number_of_buckets: -1,
          ser_de_info_name: 'parquet',
          ser_de_info_serialization_library: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          bucket_columns: [],
          sort_columns: [],
          skewed_column_names: [],
          skewed_column_value_location_maps: {},
          skewed_column_values: [],
          stored_as_sub_directories: false,
          partition_keys: [
          ],
          classification: 'parquet',
          convergdb_full_relation_name: 'production.test_database.test_schema.books_target',
          convergdb_dsd: 'test_database.test_schema.books_target',
          convergdb_storage_bucket: 'fakedata-target.beyondsoft.us',
          convergdb_state_bucket: 'fakedata-state.beyondsoft.us',
          convergdb_storage_format: 'parquet',
          convergdb_etl_job_name: 'test_etl_job',
          convergdb_deployment_id: %(${var.deployment_id})
        } 
        
        assert_equal(
          expected,
          a.table_parameters(a.structure)
        ) 
      end
      
      def test_database_parameters
        a = athena_generator
        
        expected = {
          database_name: "production__test_database__test_schema",
        }
        
        assert_equal(
          expected,
          a.database_parameters(a.structure)
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
