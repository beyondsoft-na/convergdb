require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

require 'minitest'
require 'minitest/autorun'

require_relative '../lib/ir/live/live.rb'

module ConvergDB
  module LiveState
    class IRTest < Minitest::Test
      def books_table
        {:name=>"books",
         :owner=>"hadoop",
         :create_time=>'2018-02-07 15:54:24 -0800',
         :update_time=>'2018-02-07 15:54:24 -0800',
         :retention=>0,
         :storage_descriptor=>
          {:columns=>
            [{:name=>"item_number", :type=>"int", :comment=>"item_number"},
             {:name=>"title", :type=>"string", :comment=>"title"},
             {:name=>"author", :type=>"string", :comment=>"author"},
             {:name=>"price", :type=>"decimal(10,2)", :comment=>"price"},
             {:name=>"retail_markup",
              :type=>"decimal(10,2)",
              :comment=>"price * 0.25"}],
           :location=>
            "s3://convergdb-data-f13f80a0c63d373f/15302509696417528321/production.ecommerce.inventory.books/",
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
          [{:name=>"part_id", :type=>"string", :comment=>"substring(md5(title),1,1)"}],
         :table_type=>"EXTERNAL_TABLE",
         :parameters=>
          {"EXTERNAL"=>"TRUE",
           "classification"=>"parquet",
           "convergdb_database_cf_id"=>
            "arn:aws:cloudformation:us-west-2:692977618922:stack/convergdb-tf-db-15302509696417528321-8115201757802509580/2b9d2f50-0c62-11e8-a93f-50d5ca789e1e",
           "convergdb_deployment_id"=>"15302509696417528321",
           "convergdb_dsd"=>"ecommerce.inventory.books",
           "convergdb_etl_job_name"=>"nightly_batch",
           "convergdb_full_relation_name"=>"production.ecommerce.inventory.books",
           "convergdb_state_bucket"=>"convergdb-admin-f13f80a0c63d373f",
           "convergdb_storage_bucket"=>
            "convergdb-data-f13f80a0c63d373f/15302509696417528321/production.ecommerce.inventory.books",
           "convergdb_storage_format"=>"parquet"},
         :created_by=>"arn:aws:iam::692977618922:user/jeremy"}
      end

      def control_table
        {:name=>"production__ecommerce__inventory__books",
         :owner=>"hadoop",
         :create_time=>'2018-02-07 15:54:24 -0800',
         :update_time=>'2018-02-07 15:54:24 -0800',
         :retention=>0,
         :storage_descriptor=>
          {:columns=>
            [{:name=>"convergdb_batch_id", :type=>"string", :comment=>""},
             {:name=>"batch_start_time", :type=>"timestamp", :comment=>""},
             {:name=>"batch_end_time", :type=>"timestamp", :comment=>""},
             {:name=>"source_type", :type=>"string", :comment=>""},
             {:name=>"source_format", :type=>"string", :comment=>""},
             {:name=>"source_relation", :type=>"string", :comment=>""},
             {:name=>"source_bucket", :type=>"string", :comment=>""},
             {:name=>"source_key", :type=>"string", :comment=>""},
             {:name=>"load_type", :type=>"string", :comment=>""},
             {:name=>"status", :type=>"string", :comment=>""}],
           :location=>
            "s3://convergdb-admin-f13f80a0c63d373f/15302509696417528321/state/production.ecommerce.inventory.books/control",
           :input_format=>"org.apache.hadoop.mapred.TextInputFormat",
           :output_format=>
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
           :compressed=>false,
           :number_of_buckets=>-1,
           :serde_info=>
            {:serialization_library=>"org.apache.hive.hcatalog.data.JsonSerDe",
             :parameters=>{"serialization.format"=>"1"}},
           :bucket_columns=>[],
           :sort_columns=>[],
           :parameters=>{},
           :skewed_info=>
            {:skewed_column_names=>[],
             :skewed_column_values=>[],
             :skewed_column_value_location_maps=>{}},
           :stored_as_sub_directories=>false},
         :partition_keys=>[],
         :table_type=>"EXTERNAL_TABLE",
         :parameters=>
          {"EXTERNAL"=>"TRUE",
           "classification"=>"json",
           "convergdb_database_cf_id"=>
            "arn:aws:cloudformation:us-west-2:692977618922:stack/convergdb-tf-db-15302509696417528321-8115201757802509580/2b9d2f50-0c62-11e8-a93f-50d5ca789e1e",
           "convergdb_deployment_id"=>"15302509696417528321",
           "convergdb_dsd"=>"ecommerce.inventory.books",
           "convergdb_etl_job_name"=>"nightly_batch",
           "convergdb_full_relation_name"=>"production.ecommerce.inventory.books",
           "convergdb_storage_bucket"=>"convergdb-admin-f13f80a0c63d373f",
           "convergdb_storage_format"=>"parquet"},
         :created_by=>"arn:aws:iam::692977618922:user/jeremy"}
      end

      def all_tables_for_database_list
        [
          {:name=>"production__ecommerce__inventory__books",
           :owner=>"hadoop",
           :create_time=>'2018-02-07 15:54:24 -0800',
           :update_time=>'2018-02-07 15:54:24 -0800',
           :retention=>0,
           :storage_descriptor=>
            {:columns=>
              [{:name=>"convergdb_batch_id", :type=>"string", :comment=>""},
               {:name=>"batch_start_time", :type=>"timestamp", :comment=>""},
               {:name=>"batch_end_time", :type=>"timestamp", :comment=>""},
               {:name=>"source_type", :type=>"string", :comment=>""},
               {:name=>"source_format", :type=>"string", :comment=>""},
               {:name=>"source_relation", :type=>"string", :comment=>""},
               {:name=>"source_bucket", :type=>"string", :comment=>""},
               {:name=>"source_key", :type=>"string", :comment=>""},
               {:name=>"load_type", :type=>"string", :comment=>""},
               {:name=>"status", :type=>"string", :comment=>""}],
             :location=>
              "s3://convergdb-admin-f13f80a0c63d373f/15302509696417528321/state/production.ecommerce.inventory.books/control",
             :input_format=>"org.apache.hadoop.mapred.TextInputFormat",
             :output_format=>
              "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
             :compressed=>false,
             :number_of_buckets=>-1,
             :serde_info=>
              {:serialization_library=>"org.apache.hive.hcatalog.data.JsonSerDe",
               :parameters=>{"serialization.format"=>"1"}},
             :bucket_columns=>[],
             :sort_columns=>[],
             :parameters=>{},
             :skewed_info=>
              {:skewed_column_names=>[],
               :skewed_column_values=>[],
               :skewed_column_value_location_maps=>{}},
             :stored_as_sub_directories=>false},
           :partition_keys=>[],
           :table_type=>"EXTERNAL_TABLE",
           :parameters=>
            {"EXTERNAL"=>"TRUE",
             "classification"=>"json",
             "convergdb_database_cf_id"=>
              "arn:aws:cloudformation:us-west-2:692977618922:stack/convergdb-tf-db-15302509696417528321-8115201757802509580/2b9d2f50-0c62-11e8-a93f-50d5ca789e1e",
             "convergdb_deployment_id"=>"15302509696417528321",
             "convergdb_dsd"=>"ecommerce.inventory.books",
             "convergdb_etl_job_name"=>"nightly_batch",
             "convergdb_full_relation_name"=>"production.ecommerce.inventory.books",
             "convergdb_storage_bucket"=>"convergdb-admin-f13f80a0c63d373f",
             "convergdb_storage_format"=>"parquet"},
           :created_by=>"arn:aws:iam::692977618922:user/jeremy"},
          {:name=>"books",
           :owner=>"hadoop",
           :create_time=>'2018-02-07 15:54:24 -0800',
           :update_time=>'2018-02-07 15:54:24 -0800',
           :retention=>0,
           :storage_descriptor=>
            {:columns=>
              [{:name=>"item_number", :type=>"int", :comment=>"item_number"},
               {:name=>"title", :type=>"string", :comment=>"title"},
               {:name=>"author", :type=>"string", :comment=>"author"},
               {:name=>"price", :type=>"decimal(10,2)", :comment=>"price"},
               {:name=>"retail_markup",
                :type=>"decimal(10,2)",
                :comment=>"price * 0.25"}],
             :location=>
              "s3://convergdb-data-f13f80a0c63d373f/15302509696417528321/production.ecommerce.inventory.books/",
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
            [{:name=>"part_id", :type=>"string", :comment=>"substring(md5(title),1,1)"}],
           :table_type=>"EXTERNAL_TABLE",
           :parameters=>
            {"EXTERNAL"=>"TRUE",
             "classification"=>"parquet",
             "convergdb_database_cf_id"=>
              "arn:aws:cloudformation:us-west-2:692977618922:stack/convergdb-tf-db-15302509696417528321-8115201757802509580/2b9d2f50-0c62-11e8-a93f-50d5ca789e1e",
             "convergdb_deployment_id"=>"15302509696417528321",
             "convergdb_dsd"=>"ecommerce.inventory.books",
             "convergdb_etl_job_name"=>"nightly_batch",
             "convergdb_full_relation_name"=>"production.ecommerce.inventory.books",
             "convergdb_state_bucket"=>"convergdb-admin-f13f80a0c63d373f",
             "convergdb_storage_bucket"=>
              "convergdb-data-f13f80a0c63d373f/15302509696417528321/production.ecommerce.inventory.books",
             "convergdb_storage_format"=>"parquet"},
           :created_by=>"arn:aws:iam::692977618922:user/jeremy"},
          {:name=>"not_a_convergdb_table",
           :owner=>"hadoop",
           :create_time=>'2018-02-07 15:54:24 -0800',
           :update_time=>'2018-02-07 15:54:24 -0800',
           :retention=>0,
           :storage_descriptor=>
            {:columns=>
              [{:name=>"item_number", :type=>"int", :comment=>"item_number"},
               {:name=>"title", :type=>"string", :comment=>"title"},
               {:name=>"author", :type=>"string", :comment=>"author"},
               {:name=>"price", :type=>"decimal(10,2)", :comment=>"price"},
               {:name=>"retail_markup",
                :type=>"decimal(10,2)",
                :comment=>"price * 0.25"}],
             :location=>
              "s3://convergdb-data-f13f80a0c63d373f/15302509696417528321/production.ecommerce.inventory.books/",
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
            [{:name=>"part_id", :type=>"string", :comment=>"substring(md5(title),1,1)"}],
           :table_type=>"EXTERNAL_TABLE",
           :parameters=>
            {"EXTERNAL"=>"TRUE",
             "classification"=>"parquet"},
           :created_by=>"arn:aws:iam::692977618922:user/jeremy"},
        ]
      end

      def live_ir
        IR.new
      end

      def tf_state
        File.read(
          "#{File.dirname(__FILE__)}/fixtures/live_ir/convergdb.tfstate"
        )
      end

      def backend_json
        File.read(
          "#{File.dirname(__FILE__)}/fixtures/live_ir/terraform.tf.json"
        )
      end

      def convergdb_table_json
        File.read(
          "#{File.dirname(__FILE__)}/fixtures/live_ir/convergdb_table.json"
        )
      end

      def convergdb_table_structure
        JSON.parse(
          convergdb_table_json,
          :symbolize_keys => true
        )
      end

      def test_s3_object_to_string
        # no test here because it calls AWS
      end

      def test_get_deployment_id
        t = live_ir

        assert_equal(
          '15302509696417528321',
          t.get_deployment_id(
            "#{File.dirname(__FILE__)}/fixtures/live_ir/convergdb.tfstate",
            '/some/nofile'
          )
        )
      end

      def test_deployment_id_from_tfstate_json
        t = live_ir

        assert_equal(
          '15302509696417528321',
          t.deployment_id_from_tfstate_json(
            tf_state
          )
        )

        assert_nil(
          t.deployment_id_from_tfstate_json('')
        )
      end

      def test_s3_attributes_from_tf_backend
        t = live_ir

        assert_equal(
          {
            bucket: 'convergdb-admin-f13f80a0c63d373f',
            key: 'terraform/convergdb.tfstate',
            region: 'us-west-2'
          },
          t.s3_attributes_from_tf_backend(
            backend_json
          )
        )

        assert_nil(
          t.s3_attributes_from_tf_backend('')
        )
      end

      def test_tfstate_path
        t = live_ir

        assert_equal(
          './terraform/terraform.tfstate',
          t.tfstate_path
        )
      end

      def test_tfjson_path
        t = live_ir

        assert_equal(
          './terraform/terraform.tf.json',
          t.tfjson_path
        )
      end

      def test_glue_client
        # no test requires aws connection
      end

      def test_s3_client
        # no test requires aws connection
      end

      def test_database_names
        # no test requires aws connection
      end

      def test_tables_for_database
        # no test requires aws connection
      end

      def test_all_tables_for_database
        # no test requires aws connetion
      end

      def test_convergdb_relations_only
        t = live_ir

        # insures that the non-convergdb table in the test stub does not
        # come through this function
        assert_equal(
          ['production__ecommerce__inventory__books', 'books'],
          t.convergdb_relations_only(
            all_tables_for_database_list
          ).map { |tbl| tbl[:name] }
        )
      end

      def test_comparable_table
        t = live_ir

        # the first test is applied to what would be a target relation
        # in parquet format.
        expected = {
          full_relation_name: 'production.ecommerce.inventory.books',
          dsd: 'ecommerce.inventory.books',
          storage_bucket:
            '${data_bucket}/${deployment_id}/production.ecommerce.inventory.books',
          state_bucket: '${admin_bucket}',
          storage_format: 'parquet',
          etl_job_name: 'nightly_batch',
          attributes: [
            {:name=>"item_number", :data_type=>"int", :expression=>"item_number"},
            {:name=>"title", :data_type=>"string", :expression=>"title"},
            {:name=>"author", :data_type=>"string", :expression=>"author"},
            {:name=>"price", :data_type=>"decimal(10,2)", :expression=>"price"},
            {:name=>"retail_markup", :data_type=>"decimal(10,2)", :expression=>"price * 0.25"}
          ]
        }

        assert_equal(
          expected,
          t.comparable_table(books_table, '15302509696417528321')
        )
      end

      def test_comparable_athena_relations
        # no test because of embedded external dependencies
      end

      def test_convergdb_bucket_reference
        t = live_ir

        assert_equal(
          '${data_bucket}',
          t.convergdb_bucket_reference(
            'convergdb-data-1234567890abcdef', '4321432143214321'
          )
        )

        assert_equal(
          '${admin_bucket}',
          t.convergdb_bucket_reference(
            'convergdb-admin-1234567890abcdef', '4321432143214321'
          )
        )

        assert_equal(
          '${admin_bucket}/${deployment_id}',
          t.convergdb_bucket_reference(
            'convergdb-admin-1234567890abcdef/4321432143214321', '4321432143214321'
          )
        )

        assert_equal(
          4,
          t.convergdb_bucket_reference(
            4, '4321432143214321'
          )
        )

        assert_nil(
          t.convergdb_bucket_reference(
            nil, '4321432143214321'
          )
        )
      end

      def test_glue_triggers
        # no test aws interaction
      end

      def triggers
        {:triggers=>
          [
            {
              :name=>"convergdb-nightly_batch",
              :type=>"SCHEDULED",
              :state=>"CREATED",
              :schedule=>"cron(0 0 * * ? *)",
              :actions=>[{:job_name=>"nightly_batch"}]
            },
            {
              :name=>"some_other_trigger",
              :type=>"SCHEDULED",
              :state=>"CREATED",
              :schedule=>"cron(0 0 * * ? *)",
              :actions=>[{:job_name=>"nobodycares"}]
            }
          ]
        }
      end

      def test_convergdb_glue_triggers
        t = live_ir

        expected = [
          {:name=>"convergdb-nightly_batch",
            :type=>"SCHEDULED",
            :state=>"CREATED",
            :schedule=>"cron(0 0 * * ? *)",
            :actions=>[{:job_name=>"nightly_batch"}]}
        ]

        assert_equal(
          expected,
          t.convergdb_glue_triggers(
            triggers
          )
        )
      end

      def test_schedules_by_job_name
        t = live_ir

        expected = {
          'nightly_batch' => 'cron(0 0 * * ? *)'
        }

        assert_equal(
          expected,
          t.schedules_by_job_name(
            t.convergdb_glue_triggers(
              triggers
            )
          )
        )
      end

      def glue_etl_jobs
        [
          {
            :name=>"nightly_batch",
            :role=>"convergdb-nightly_batch-2823741295544373327",
            :created_on=>'2018-02-07 15:54:10 -0800',
            :last_modified_on=>'2018-02-07 15:54:10 -0800',
            :execution_property=>{:max_concurrent_runs=>1},
            :command=>
             {:name=>"glueetl",
              :script_location=>
               "s3://convergdb-admin-f13f80a0c63d373f/15302509696417528321/scripts/aws_glue/nightly_batch/nightly_batch.py"},
            :default_arguments=>
             {"--convergdb_deployment_id"=>"15302509696417528321",
              "--extra-py-files"=>
               "s3://convergdb-admin-f13f80a0c63d373f/15302509696417528321/scripts/aws_glue/nightly_batch/convergdb_pyspark_library.py"},
            :connections=>{},
            :max_retries=>0,
            :allocated_capacity=>2
          },
          {
            :name=>"not_a_convergdb_job",
            :role=>"convergdb-nightly_batch-2823741295544373327",
            :created_on=>'2018-02-07 15:54:10 -0800',
            :last_modified_on=>'2018-02-07 15:54:10 -0800',
            :execution_property=>{:max_concurrent_runs=>1},
            :command=>
             {:name=>"glueetl",
              :script_location=>
               "s3://convergdb-admin-f13f80a0c63d373f/15302509696417528321/scripts/aws_glue/nightly_batch/nightly_batch.py"},
            :default_arguments=>
             {},
            :connections=>{},
            :max_retries=>0,
            :allocated_capacity=>2
          }
        ]
      end

      def test_glue_etl_jobs
        # no test calls AWS
      end

      def test_convergdb_etl_jobs
        t = live_ir

        assert_equal(
          ['nightly_batch'],
          t.convergdb_etl_jobs(
            glue_etl_jobs
          ).map { |j| j[:name] }
        )
      end

      def test_convergdb_this_deployment_etl_jobs
        t = live_ir

        assert_equal(
          ['nightly_batch'],
          t.convergdb_this_deployment_etl_jobs(
            t.convergdb_etl_jobs(
              glue_etl_jobs
            ),
            '15302509696417528321'
          ).map { |j| j[:name] }
        )
      end

      def test_comparable_glue_etl_jobs
        t = live_ir

        expected = [
          {
            name: 'nightly_batch',
            schedule: 'cron(0 0 * * ? *)'
          }
        ]

        assert_equal(
          expected,
          t.comparable_glue_etl_jobs(
            glue_etl_jobs,
            t.schedules_by_job_name(
              t.convergdb_glue_triggers(
                triggers
              )
            ),
            '15302509696417528321'
          ),
          pp(
            t.schedules_by_job_name(
              t.convergdb_glue_triggers(
                triggers
              )
            )
          )
        )
      end
    end
  end
end