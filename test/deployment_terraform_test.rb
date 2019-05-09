require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

# required for testing
require 'minitest'
require 'minitest/autorun'
require 'hashdiff'

# imports the ruby file we are testing
require_relative '../lib/deployment/terraform/terraform.rb'
require_relative '../lib/version.rb'

module ConvergDB
  module Deployment
    class BaseTerraformExtendedForTest < BaseTerraform
      def validation_regex
        {
          resource_id: { regex: /abc/, mandatory: true }
        }
      end
    end

    class TestBaseTerraform < Minitest::Test
      def base_terraform
        BaseTerraform.new
      end

      def catch_error
        yield
      rescue => e
        return e
      end

      # attr_reader
      def test_resource_type
        nil
      end

      # attr_reader
      def test_validation_regex
        nil
      end

      def test_resolve!
        t = base_terraform
        e = catch_error { t.resolve! }

        assert_equal(
          'resolve! must be implemented for class ConvergDB::Deployment::BaseTerraform',
          e.message
        )
      end

      def test_validate
        # this method only calls a single method which is already tested
      end

      def test_structure
        t = base_terraform
        e = catch_error { t.structure }

        assert_equal(
          'structure must be implemented for class ConvergDB::Deployment::BaseTerraform',
          e.message
        )
      end

      def test_to_underscore
        t = base_terraform
        [
          ['hello', 'hello'],
          ['hello.again', 'hello__again'],
          ['yo-mama', 'yo__mama'],
          ['yo_mama', 'yo_mama'],
          ['chicken_turkey.pheasant-squab', 'chicken_turkey__pheasant__squab']
        ].each do |f|
          assert_equal(
            f[1],
            t.to_underscore(f[0])
          )
        end
      end

      def test_to_dash
        t = base_terraform
        [
          ['hello', 'hello'],
          ['hello.again', 'hello--again'],
          ['yo-mama', 'yo-mama'],
          ['yo_mama', 'yo--mama'],
          ['chicken_turkey.pheasant-squab', 'chicken--turkey--pheasant-squab']
        ].each do |f|
          assert_equal(
            f[1],
            t.to_dash(f[0])
          )
        end
      end

      def test_to_nothing
        t = base_terraform
        [
          ['hello', 'hello'],
          ['hello.again', 'helloagain'],
          ['yo-mama', 'yomama'],
          ['yo_mama', 'yomama'],
          ['chicken_turkey.pheasant-squab', 'chickenturkeypheasantsquab']
        ].each do |f|
          assert_equal(
            f[1],
            t.to_nothing(f[0])
          )
        end
      end

      def test_tf_json
        # no test needed... see test_to_tf_json
      end

      def test_to_tf_json
        t = base_terraform
        s = {
          structure: {
            module: {
              module_name: {
                a: '22',
                b: 'hello'
              }
            }
          }
        }

        expected = [
          %{"module" : \{ },
          %{"module_name" : \{\n  "a": "22",\n  "b": "hello"\n\}},
          %{\},}
        ]

        assert_equal(
          expected,
          t.to_tf_json(s)
        )
      end

      def test_valid_string_match?
        t = base_terraform

        # test nil value provided to mandatory field
        assert_equal(
          false,
          t.valid_string_match?(nil, /.*/, true)
        )

        # test nil value provided to non-mandatory field
        assert_equal(
          true,
          t.valid_string_match?(nil, /.*/)
        )

        # test matching string
        assert_equal(
          true,
          t.valid_string_match?('abc', /abc/)
        )

        # test non-matching string
        assert_equal(
          false,
          t.valid_string_match?('def', /abc/)
        )

        # test matching string mandatory field
        assert_equal(
          true,
          t.valid_string_match?('abc', /abc/, true)
        )

        # test non-matching string mandatory field
        assert_equal(
          false,
          t.valid_string_match?('def', /abc/, true)
        )
      end

      def test_validate_string_attributes
        t = BaseTerraformExtendedForTest.new

        t.resource_id = 'abc'
        e = catch_error { t.validate_string_attributes }
        assert_equal(
          false,
          e.is_a?(Exception)
        )

        t.resource_id = 'burp'
        e = catch_error { t.validate_string_attributes }
        assert_equal(
          true,
          e.is_a?(Exception)
        )
      end
    end

    class TestAWSGlueTableModule < Minitest::Test
      # used to initialize the object for testing
      def initializer
        {
          resource_id: 'id123456',
          working_path: '/tmp',
          region: '${var.region}',
          structure: {
            database_name: 'database_name',
            table_name: 'table_name',
            columns: [],
            location: 'location',
            input_format: 'input_format',
            output_format: 'output_format',
            compressed: 'compressed',
            number_of_buckets: 'number_of_buckets',
            ser_de_info_name: 'ser_de_info_name',
            ser_de_info_serialization_library: 'ser_de_info_serialization_library',
            bucket_columns: [],
            sort_columns: [],
            skewed_column_names: [],
            skewed_column_value_location_maps: [],
            skewed_column_values: [],
            stored_as_sub_directories: 'stored_as_sub_directories',
            partition_keys: [],
            classification: 'classification',
            convergdb_full_relation_name: 'convergdb_full_relation_name',
            convergdb_dsd: 'convergdb_dsd',
            convergdb_storage_bucket: 'convergdb_storage_bucket',
            convergdb_state_bucket: 'convergdb_state_bucket',
            convergdb_storage_format: 'convergdb_storage_format',
            convergdb_etl_job_name: 'convergdb_etl_job_name',
            convergdb_deployment_id: 'convergdb_deployment_id'
          }
        }
      end

      def aws_glue_table_module
        AWSGlueTableModule.new(initializer)
      end

      def test_initialize
        t = aws_glue_table_module

        assert_equal(
          initializer[:resource_id],
          t.resource_id
        )

        assert_equal(
          initializer[:region],
          t.region
        )
        
        assert_equal(
          ConvergDB::TERRAFORM_MODULES[:aws_glue_table],
          t.source
        )
      end

      def test_validation_regex
        # this tests the hash contents, not the validation process
        t = aws_glue_table_module
        assert_equal(
          { regex: /.*/, mandatory: true },
          t.validation_regex[:resource_id]
        )
      end

      def test_structure
        t = aws_glue_table_module

        expected = {
          resource_id: 'id123456',
          resource_type: :aws_glue_table_module,
          structure: {
            module: {
              'id123456' => {
                source: ConvergDB::TERRAFORM_MODULES[:aws_glue_table],
                database_name: 'database_name',
                table_name: 'table_name',
                columns: [],
                location: 'location',
                input_format: 'input_format',
                output_format: 'output_format',
                compressed: 'compressed',
                number_of_buckets: 'number_of_buckets',
                ser_de_info_name: 'ser_de_info_name',
                ser_de_info_serialization_library: 'ser_de_info_serialization_library',
                bucket_columns: [],
                sort_columns: [],
                skewed_column_names: [],
                skewed_column_value_location_maps: [],
                skewed_column_values: [],
                stored_as_sub_directories: 'stored_as_sub_directories',
                partition_keys: [],
                classification: 'classification',
                convergdb_full_relation_name: 'convergdb_full_relation_name',
                convergdb_dsd: 'convergdb_dsd',
                convergdb_storage_bucket: 'convergdb_storage_bucket',
                convergdb_state_bucket: 'convergdb_state_bucket',
                convergdb_storage_format: 'convergdb_storage_format',
                convergdb_etl_job_name: 'convergdb_etl_job_name',
                convergdb_deployment_id: 'convergdb_deployment_id'
              }
            }
          }
        }

        assert_equal(
          expected,
          t.structure
        )
      end
    end

    class TestAWSGlueDatabaseModule < Minitest::Test
      def initializer
        {
          resource_id: 'id123456',
          region: 'us-west-2',
          structure: {
            database_name: 'test'
          }
        }
      end

      def aws_glue_database_module
        AWSGlueDatabaseModule.new(initializer)
      end

      def test_initialize
        t = aws_glue_database_module

        assert_equal(
          initializer[:resource_id],
          t.resource_id
        )

        assert_equal(
          initializer[:region],
          t.region
        )

        assert_equal(
          ConvergDB::TERRAFORM_MODULES[:aws_glue_database],
          t.source
        )

        assert_equal(
          initializer[:structure][:database_name],
          t.database_name
        )
      end

      def test_validation_regex
        # this tests the hash contents, not the validation process
        t = aws_glue_database_module
        assert_equal(
          { regex: /.*/, mandatory: true },
          t.validation_regex[:resource_id]
        )
      end

      def test_structure
        t = aws_glue_database_module

        expected = {
          resource_id: 'id123456',
          resource_type: :aws_glue_database_module,
          structure: {
            module: {
              'id123456' => {
                source: ConvergDB::TERRAFORM_MODULES[:aws_glue_database],
                database_name:  "test",
                deployment_id: "${var.deployment_id}"
              }
            }
          }
        }

        assert_equal(
          expected,
          t.structure
        )
      end
    end

    class TestAWSGlueETLJobModule < Minitest::Test
      def initializer
        {
          resource_id: 'id123456',
          region: 'us-west-2',
          job_name: 'etl_job_name',
          local_script: 'path/to/local',
          local_pyspark_library: 'path/to/pyspark/library',
          script_key: 'script-key',
          pyspark_library_key: 'pyspark-library-key',
          schedule: 'cron(0 * * * ? *)',
          stack_name: 'etlJobStackName',
          service_role: 'glueService',
          dpu: 2
        }
      end

      def aws_glue_etl_job_module
        AWSGlueETLJobModule.new(initializer)
      end

      def test_initialize
        t = aws_glue_etl_job_module

        assert_equal('id123456', t.resource_id)
        assert_equal('us-west-2', t.region)
        assert_equal('etl_job_name', t.job_name)
        assert_equal('path/to/local', t.local_script)
        assert_equal('path/to/pyspark/library', t.local_pyspark_library)
        assert_equal(nil, t.script_bucket)
        assert_equal('script-key', t.script_key)
        assert_equal('pyspark-library-key', t.pyspark_library_key)
        assert_equal('cron(0 * * * ? *)', t.schedule)
        assert_equal('etlJobStackName', t.stack_name)
        assert_equal('glueService', t.service_role)
        assert_equal(2, t.dpu)
      end

      def test_validation_regex
        t = aws_glue_etl_job_module
        assert_equal(
          { regex: /.*/, mandatory: true },
          t.validation_regex[:resource_id]
        )
      end

      def test_structure
        t = aws_glue_etl_job_module

        expected = {
          resource_id: 'id123456',
          resource_type: :aws_glue_etl_job_module,
          structure: {
            module: {
              'id123456' => {
                source: ConvergDB::TERRAFORM_MODULES[:aws_glue_etl_job],
                stack_name: 'etlJobStackName',
                region: 'us-west-2',
                job_name: 'etl_job_name',
                local_script: 'path/to/local',
                local_pyspark_library: 'path/to/pyspark/library',
                script_bucket: '${var.admin_bucket}',
                script_key: 'script-key',
                pyspark_library_key: 'pyspark-library-key',
                schedule: 'cron(0 * * * ? *)',
                service_role: 'glueService',
                deployment_id: %{${var.deployment_id}},
                admin_bucket: "${var.admin_bucket}",
                data_bucket: "${var.data_bucket}",
                dpu: 2,
                cloudwatch_namespace: "convergdb/${var.deployment_id}",
                sns_topic: "${aws_sns_topic.convergdb-notifications.arn}",
                etl_lock_table: "${var.etl_lock_table}"
              }
            }
          }
        }

        assert_equal(
          expected,
          t.structure,
          puts(
          HashDiff.diff(
            expected,
            t.structure)
          )
        )
      end
    end

    class TestAWSFargateETLJobModule < Minitest::Test
      def initializer
        {
          resource_id: 'id123456',
          region: 'us-west-2',
          etl_job_name: 'test_job_name',
          etl_job_schedule: 'cron(0 0 * * ? *)',
          local_script: '/tmp/local.py',
          local_pyspark_library: '/tmp/pyspark.zip',
          script_bucket: 'myscript.bucket/path',
          script_key: 's3_prefix/script.py',
          pyspark_library_key: 's3_prefix/library.zip',
          lambda_trigger_key: 's3_prefix/lambda.py',
          docker_image: 'beyondsoftna/convergdb',
          docker_image_digest: 'sha256:abc123'
        }
      end

      def aws_fargate_etl_job_module
        AWSFargateETLJobModule.new(initializer)
      end

      def test_initialize
        t = aws_fargate_etl_job_module

        assert_equal(t.resource_id, 'id123456')
        assert_equal(t.region, 'us-west-2')
        assert_equal(t.etl_job_name, 'test_job_name')
        assert_equal(t.etl_job_schedule, 'cron(0 0 * * ? *)')
        assert_equal(t.local_script, '/tmp/local.py')
        assert_equal(t.local_pyspark_library, '/tmp/pyspark.zip')
        assert_equal(t.script_bucket, 'myscript.bucket/path')
        assert_equal(t.script_key, 's3_prefix/script.py')
        assert_equal(t.pyspark_library_key, 's3_prefix/library.zip')
        assert_equal(t.lambda_trigger_key, 's3_prefix/lambda.py')
      end

      def test_validation_regex
        t = aws_fargate_etl_job_module
        assert_equal(
          { regex: /.*/, mandatory: true },
          t.validation_regex[:resource_id]
        )
      end

      def test_structure
        t = aws_fargate_etl_job_module

        expected = {
          resource_id: 'id123456',
          resource_type: :aws_fargate_etl_job_module,
          structure: {
            module: {
              'id123456' => {
                source: ConvergDB::TERRAFORM_MODULES[:aws_fargate_etl_job],
                region: 'us-west-2',
                deployment_id: '${var.deployment_id}',
                etl_job_name: 'test_job_name',
                etl_job_schedule: 'cron(0 0 * * ? *)',
                local_script: '/tmp/local.py',
                local_pyspark_library: '/tmp/pyspark.zip',
                script_bucket: 'myscript.bucket/path',
                script_key: 's3_prefix/script.py',
                pyspark_library_key: 's3_prefix/library.zip',
                lambda_trigger_key: 's3_prefix/lambda.py',
                admin_bucket: '${var.admin_bucket}',
                data_bucket: '${var.data_bucket}',
                cloudwatch_namespace: 'convergdb/${var.deployment_id}',
                sns_topic: '${aws_sns_topic.convergdb-notifications.arn}',
                ecs_subnet: '${var.fargate_subnet}',
                ecs_cluster: '${var.fargate_cluster}', 
                ecs_log_group: '${var.ecs_log_group}',
                docker_image: "beyondsoftna/convergdb@sha256:abc123",
                execution_task_role: '${var.ecs_execution_role}',
                etl_lock_table: "${var.etl_lock_table}"
              }
            }
          }
        }

        assert_equal(
          expected,
          t.structure,
          puts(
          HashDiff.diff(
            expected,
            t.structure)
          )
        )
      end
    end
    
    class TestStreamingInventoryModule < Minitest::Test
      def initializer
        {
          resource_id: 'streaming_inventory_some__bucket',
          storage_bucket: 'some.bucket/prefix',
          streaming_inventory_output_bucket: 'inventory.bucket/prefix/'
        }
      end

      def streaming_inventory_module(params=initializer)
        StreamingInventoryModule.new(params)
      end

      def test_initialize
        t = streaming_inventory_module

        assert_equal(
          'streaming_inventory_some__bucket',
          t.resource_id
        )

        assert_equal(
          "${var.region}",
          t.region
        )

        assert_equal(
          "some.bucket",
          t.source_bucket
        )

        assert_equal(
          "convergdb-${var.deployment_id}-eaccd2f51cbee31dfd8adf145c26a246",
          t.firehose_stream_name
        )

        assert_equal(
          "inventory.bucket",
          t.destination_bucket
        )

        assert_equal(
          'prefix/',
          t.destination_prefix
        )

        assert_equal(
          "convergdb-${var.deployment_id}-eaccd2f51cbee31dfd8adf145c26a246",
          t.lambda_name
        )
      end

      def test_inventory_stream_name
        t = streaming_inventory_module

        assert_equal(
          'convergdb-${var.deployment_id}-c7e1a9d9fe4a685973e87738807335dd',
          t.inventory_stream_name('storage_bucket')
        )
      end

      def test_lambda_function_name
        t = streaming_inventory_module

        assert_equal(
          'convergdb-${var.deployment_id}-c7e1a9d9fe4a685973e87738807335dd',
          t.lambda_function_name('storage_bucket')
        )
      end

      def test_structure
        t = streaming_inventory_module

        expected = {
          resource_id: 'streaming_inventory_some__bucket',
          resource_type: :streaming_inventory_module,
          structure: {
            module: {
              'streaming_inventory_some__bucket' => {
                source: ConvergDB::TERRAFORM_MODULES[:aws_s3_streaming_inventory],
                region: '${var.region}',
                firehose_stream_name: 'convergdb-${var.deployment_id}-eaccd2f51cbee31dfd8adf145c26a246',
                source_bucket: 'some.bucket',
                destination_bucket: 'inventory.bucket',
                destination_prefix: 'prefix/',
                lambda_name: 'convergdb-${var.deployment_id}-eaccd2f51cbee31dfd8adf145c26a246'
              }
            }
          }
        }

        assert_equal(
          expected,
          t.structure
        )
      end
    end

    class TestTerraformBuilder < Minitest::Test
      def terraform_builder
        TerraformBuilder.new
      end

      def aws_glue_table_params
        {
          resource_id: 'id123456',
          region: 'us-west-2',
          structure: {
            table_name: 'table1'
          }
        }
      end

      def aws_glue_database_params
        {
          resource_id: 'id123456',
          region: 'us-west-2',
          structure: {
            database_name: 'database1'
          }
        }
      end

      def aws_glue_etl_job_params
        {
          resource_id: 'id123456',
          region: 'us-west-2',
          job_name: 'etl_job_name',
          local_script: 'path/to/local',
          local_pyspark_library: 'path/to/pyspark/library',
          script_bucket: 'script-bucket',
          script_key: 'script-key',
          pyspark_library_key: 'pyspark-library-key',
          schedule: 'cron(0 * * * ? *)',
          stack_name: 'etlJobStackName',
          service_role: 'glueService'
        }
      end
      
      def aws_fargate_etl_job_params
        {
          resource_id: 'id123456',
          region: 'us-west-2',
          etl_job_name: 'test_job_name',
          etl_job_schedule: 'cron(0 0 * * ? *)',
          local_script: '/tmp/local.py',
          local_pyspark_library: '/tmp/pyspark.zip',
          script_bucket: 'myscript.bucket/path',
          script_key: 's3_prefix/script.py',
          pyspark_library_key: 's3_prefix/library.zip',
          lambda_trigger_key: 's3_prefix/lambda.py',
          admin_bucket: '${var.admin_bucket}',
          data_bucket: '${var.data_bucket}'
        }
      end

      # attr_reader
      def test_resources
        nil
      end

      def test_initialize
        t = terraform_builder

        assert_equal(
          [],
          t.resources
        )
      end

      def test_resource_id_exists?
        t = terraform_builder

        assert_equal(
          false,
          t.resource_id_exists?('id123456')
        )

        t.aws_glue_table_module!(aws_glue_table_params)

        assert_equal(
          true,
          t.resource_id_exists?('id123456')
        )
      end

      def test_resource_by_id
        t = terraform_builder

        assert_nil(
          t.resource_by_id('id123456')
        )

        t.aws_glue_table_module!(aws_glue_table_params)

        assert_equal(
          AWSGlueTableModule,
          t.resource_by_id('id123456').class
        )
      end

      def test_aws_glue_table_module!
        t = terraform_builder

        t.aws_glue_table_module!(aws_glue_table_params)

        assert_equal(
          AWSGlueTableModule,
          t.resources.first.class
        )

        assert_equal(
          'table1',
          t.resources.first.table_name
        )
      end

      def test_aws_glue_database_module!
        t = terraform_builder

        t.aws_glue_database_module!(aws_glue_database_params)

        assert_equal(
          AWSGlueDatabaseModule,
          t.resources.first.class
        )

        assert_equal(
          'database1',
          t.resources.first.database_name
        )
      end

      def test_aws_glue_etl_job_module!
        t = terraform_builder

        t.aws_glue_etl_job_module!(aws_glue_etl_job_params)

        assert_equal(
          AWSGlueETLJobModule,
          t.resources.first.class
        )
      end

      def test_aws_fargate_etl_job_module!
        t = terraform_builder

        t.aws_fargate_etl_job_module!(aws_fargate_etl_job_params)

        assert_equal(
          AWSFargateETLJobModule,
          t.resources.first.class
        )
      end
      
      def test_streaming_inventory_module!
        t = terraform_builder
        # add a streaming inventory module
        t.streaming_inventory_module!(
          {
            resource_id: 'streaming_inventory_some__bucket',
            storage_bucket: 'some.bucket/prefix',
            streaming_inventory_output_bucket: 'inventory.bucket/prefix/'
          }
        )

        # check that the correct class was created
        assert_equal(
          StreamingInventoryModule,
          t.resources.first.class
        )

        # check that a single resources was created
        assert_equal(
          1,
          t.resources.length
        )

        # apply identical mutation again
        t.streaming_inventory_module!(
          {
            resource_id: 'streaming_inventory_some__bucket',
            storage_bucket: 'some.bucket/prefix',
            streaming_inventory_output_bucket: 'inventory.bucket/prefix/'
          }
        )

        # confirm idempotent handling for identical mutation
        assert_equal(
          1,
          t.resources.length
        )

        # apply a different mutation
        t.streaming_inventory_module!(
          {
            resource_id: 'streaming_inventory_some__bucket__2',
            storage_bucket: 'some.bucket.2/prefix',
            streaming_inventory_output_bucket: 'inventory.bucket/prefix/'
          }
        )

        assert_equal(
          2,
          t.resources.length
        )
      end
    end
  end
end
