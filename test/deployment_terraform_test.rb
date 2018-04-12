require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

# required for testing
require 'minitest'
require 'minitest/autorun'
require 'hashdiff'

# imports the ruby file we are testing
require_relative '../lib/deployment/terraform/terraform.rb'

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

    class TestAWSGlueTablesModule < Minitest::Test
      # used to initialize the object for testing
      def initializer
        {
          resource_id: 'id123456',
          working_path: '/tmp',
          region: '${var.region}'
        }
      end

      def aws_glue_tables_module
        AWSGlueTablesModule.new(initializer)
      end

      def test_initialize
        t = aws_glue_tables_module

        assert_equal(
          initializer[:resource_id],
          t.resource_id
        )

        assert_equal(
          initializer[:region],
          t.region
        )

        assert_equal(
          './modules/aws_athena_relations',
          t.source
        )

        assert_equal(
          {
            "AWSTemplateFormatVersion" => "2010-09-09",
            "Description" => "Create ConvergDB tables in Glue catalog",
            "Resources" => {}
          },
          t.stack
        )
      end

      def test_initialize_stack
        t = aws_glue_tables_module

        assert_equal(
          {
            "AWSTemplateFormatVersion" => "2010-09-09",
            "Description" => "Create ConvergDB tables in Glue catalog",
            "Resources" => {}
          },
          t.initialize_stack
        )
      end

      def test_append_to_stack!
        t = aws_glue_tables_module

        a = { key: 'value' }
        t.append_to_stack!(a)

        assert_equal(
          a,
          t.stack['Resources']
        )

        b = { key2: 'value2' }
        t.append_to_stack!(b)

        assert_equal(
          { key: 'value', key2: 'value2' },
          t.stack['Resources']
        )
      end

      def test_validation_regex
        # this tests the hash contents, not the validation process
        t = aws_glue_tables_module
        assert_equal(
          { regex: /.*/, mandatory: true },
          t.validation_regex[:resource_id]
        )
      end

      def test_stack_to_file!
        t = aws_glue_tables_module

        path = '/tmp/convergdb/test.json'
        stack = {'any' => 'json'}

        t.stack_to_file!(
          path,
          stack
        )

        assert_equal(
          %{{"any":"json"}\n},
          File.read(path)
        )
      end

      def test_structure
        t = aws_glue_tables_module

        expected = {
          resource_id: 'id123456',
          resource_type: :aws_glue_tables_module,
          structure: {
            module: {
              'id123456' => {
                source: './modules/aws_athena_relations',
                region: '${var.region}',
                stack_name: 'id123456',
                deployment_id: %{${var.deployment_id}},
                local_stack_file_path: '/tmp/terraform/cloudformation/id123456.json',
                s3_stack_key: %{${var.deployment_id}/cloudformation/id123456_${var.deployment_id}.json},
                admin_bucket: %{${var.admin_bucket}},
                data_bucket: %{${var.data_bucket}},
                aws_account_id: '${data.aws_caller_identity.current.account_id}',
                database_stack_id: "${module.convergdb_athena_databases_stack.database_stack_id}"
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
          './modules/aws_athena_database',
          t.source
        )

        assert_equal(
          {
            "AWSTemplateFormatVersion" => "2010-09-09",
            "Description" => "Create ConvergDB databases in Glue catalog",
            "Resources" => {}
          },
          t.stack
        )
      end

      def test_initialize_stack
        t = aws_glue_database_module

        assert_equal(
          {
            "AWSTemplateFormatVersion" => "2010-09-09",
            "Description" => "Create ConvergDB databases in Glue catalog",
            "Resources" => {}
          },
          t.initialize_stack
        )
      end

      def test_append_to_stack!
        t = aws_glue_database_module

        a = { key: 'value' }
        t.append_to_stack!(a)

        assert_equal(
          a,
          t.stack["Resources"]
        )

        b = { key2: 'value2' }
        t.append_to_stack!(b)

        assert_equal(
          { key: 'value', key2: 'value2' },
          t.stack["Resources"]
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
                source: './modules/aws_athena_database',
                region: '${var.region}',
                stack:  "{\"AWSTemplateFormatVersion\":\"2010-09-09\",\"Description\":\"Create ConvergDB databases in Glue catalog\",\"Resources\":{}}",
                deployment_id: %{${var.deployment_id}}
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
                source: './modules/aws_glue_etl_job',
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
                sns_topic: "${aws_sns_topic.convergdb-notifications.arn}"
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

    class TestTerraformBuilder < Minitest::Test

      def terraform_builder
        TerraformBuilder.new
      end

      def aws_glue_table_params
        {
          resource_id: 'id123456',
          region: 'us-west-2',
          structure: {
            table: 'table1'
          }
        }
      end

      def aws_glue_database_params
        {
          resource_id: 'id123456',
          region: 'us-west-2',
          structure: {
            database: 'database1'
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
          AWSGlueTablesModule,
          t.resource_by_id('id123456').class
        )
      end

      def test_aws_glue_table_module!
        t = terraform_builder

        t.aws_glue_table_module!(aws_glue_table_params)

        assert_equal(
          AWSGlueTablesModule,
          t.resources.first.class
        )

        assert_equal(
          'table1',
          t.resources.first.stack['Resources'][:table]
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
          t.resources.first.stack['Resources'][:database]
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
    end
  end
end
