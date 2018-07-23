require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

# required for testing
require 'minitest'
require 'minitest/autorun'

# imports the ruby file we are testing
require_relative '../lib/generators/generate.rb'
require_relative '../lib/generators/fargate/fargate.rb'
require_relative 'helpers/dsd_ddd_ir/test_dsd_ddd_irs.rb'

module ConvergDB
  module Generators
    class TestAWSFargate < Minitest::Test
      def test_dir
        File.dirname(File.expand_path(__FILE__))
      end

      def primary_ir_structure
        tmp = JSON.parse(
          File.read(
            "#{test_dir}/fixtures/primary_ir/fargate_ir.json"
          ),
          :symbolize_names => true
        )
        h = {}
        tmp.each_key { |k| h[k.to_s] = tmp[k] }
        h
      end

      def fargate_friendly_structure
        primary_ir = primary_ir_structure
        return primary_ir["production.ecommerce.inventory.books"]
      end

      def fargate_generator(working_path = nil)
        # allow for optional working path for use
        # in artifact creation tests
        test_ir = fargate_friendly_structure
        test_ir[:working_path] = working_path if working_path
        AWSFargate.new(
          test_ir,
          ConvergDB::Deployment::TerraformBuilder.new,
          nil
        )
      end
      
      #! DIFF METHODS
      
      def test_comparable
        g = fargate_generator
        
        assert_equal(
          { etl_job_schedule: 'cron(0 0 * * ? *)' },
          g.comparable(g.structure)
        )
      end
      
      def list_rules_sample
        {:rules=>
          [{:name=>"convergdb-1504d997f4da439a-integration_test_02-trigger",
            :arn=>
             "arn:aws:events:us-west-2:692977618922:rule/convergdb-1504d997f4da439a-integration_test_02-trigger",
            :state=>"DISABLED",
            :description=>"convergdb etl job integration_test_02",
            :schedule_expression=>"cron(0 0 * * ? *)"},
           {:name=>"spaces-gem-codepipeline-event-rule",
            :arn=>
             "arn:aws:events:us-west-2:692977618922:rule/spaces-gem-codepipeline-event-rule",
            :event_pattern=>
             "{\"detail\":{\"pipeline\":[\"spaces-gem-codepipeline\",\"spaces-gem-image-codepipeline\"]},\"detail-type\":[\"CodePipeline Pipeline Execution State Change\"],\"source\":[\"aws.codepipeline\"]}",
            :state=>"ENABLED"}]}
      end
      
      def test_etl_schedule_for_this_job
        g = fargate_generator
        expected = { etl_job_schedule: 'cron(0 0 * * ? *)' }
        assert_equal(
          expected,
          g.etl_schedule_for_this_job(
            list_rules_sample,
            'integration_test_02'
          )
        )
      end

      def test_generate!
        # no test because it is so much state change.
        # all functions and state changing methods in generate!
        # are tested elsewhere
      end

      def test_fargate_etl_job_module_params
        g = fargate_generator

        expected = {
          resource_id: "aws_fargate_nightly_batch",
          region: '${var.region}',
          etl_job_name: "nightly_batch",
          etl_job_schedule: g.structure[:etl_job_schedule],
          local_script: g.etl_job_script_relative_path(g.structure),
          local_pyspark_library: g.pyspark_library_relative_path(g.structure),
          script_bucket: 'demo-utility-us-east-2.beyondsoft.us',
          script_key: g.pyspark_script_key(g.structure),
          pyspark_library_key: g.pyspark_library_key(g.structure),
          lambda_trigger_key: g.pyspark_lambda_trigger_key(g.structure),
          docker_image: 'beyondsoftna/convergdb',
          docker_image_digest: 'abc123' 
          
        }
        
        assert_equal(
          expected,
          g.fargate_etl_job_module_params(g.structure)
        )
      end

      def file_contents_match?(file1, file2)
        File.read(file1) == File.read(file2)
      end

      def test_create_static_artifacts!
        test_working_path = '/tmp/convergdb_fargate_generator_test/'
        g = fargate_generator(test_working_path)

        FileUtils.mkdir_p(test_working_path)

        g.create_static_artifacts!(g.structure)
        
        assert(
          file_contents_match?(
            "#{File.dirname(__FILE__)}/../lib/generators/convergdb.zip",
            "#{test_working_path}/terraform/aws_fargate/convergdb.zip"
          )
        )
      ensure
        FileUtils.rm_r(test_working_path) rescue nil
      end

      def test_tf_fargate_path
        g = fargate_generator

        assert_equal(
          "/tmp/terraform/aws_fargate",
          g.tf_fargate_path(g.structure)
        )
      end

      def test_tf_fargate_relative_path
        g = fargate_generator

        assert_equal(
          "./aws_fargate",
          g.tf_fargate_relative_path
        )
      end
      
      def test_etl_job_script_path
        g = fargate_generator

        assert_equal(
          "#{g.tf_fargate_path(g.structure)}/nightly_batch.py",
          g.etl_job_script_path(g.structure)
        )
      end

      def test_etl_job_script_relative_path
        g = fargate_generator

        assert_equal(
          "#{g.tf_fargate_relative_path}/nightly_batch.py",
          g.etl_job_script_relative_path(g.structure)
        )
      end
      
      def test_pyspark_library_path
        g = fargate_generator

        assert_equal(
          "#{g.tf_fargate_path(g.structure)}/convergdb.zip",
          g.pyspark_library_path(g.structure)
        )
      end

      def test_pyspark_library_relative_path
        g = fargate_generator

        assert_equal(
          "#{g.tf_fargate_relative_path}/convergdb.zip",
          g.pyspark_library_relative_path(g.structure)
        )
      end
      
      def test_create_etl_script_if_not_exists!
        FileUtils.rm(test_path) rescue nil
        g = fargate_generator
        test_path = '/tmp/test_etl_script.py'
        g.create_etl_script_if_not_exists!(test_path)

        assert(File.exist?(test_path))

        assert_equal(
          %{import sys\nsys.path.insert(0, '/tmp/convergdb.zip')\nimport convergdb\nfrom convergdb.local_header import *\n\n},
          File.read(test_path)
        )
      ensure
        FileUtils.rm(test_path) rescue nil
      end

      def test_append_to_job_script!
        FileUtils.rm(test_path) rescue nil
        g = fargate_generator
        test_path = '/tmp/test_etl_script.py'
        g.create_etl_script_if_not_exists!(test_path)
        g.append_to_job_script!(
          test_path,
          'test append'
        )

        assert_equal(
          %{import sys\nsys.path.insert(0, '/tmp/convergdb.zip')\nimport convergdb\nfrom convergdb.local_header import *\n\ntest append\n\n},
          File.read(test_path)
        )
      ensure
        FileUtils.rm(test_path) rescue nil
      end

      def test_pyspark_cast_type
        g = fargate_generator

        [
          { sql: 'varchar(100)', pyspark: 'string' },
          { sql: 'char(32)', pyspark: 'string' },
          { sql: 'decimal(20,3)', pyspark: 'decimal(20,3)' },
          { sql: 'numeric(20,3)', pyspark: 'decimal(20,3)' },
        ].each do |t|
          assert_equal(
            t[:pyspark],
            g.pyspark_cast_type(t[:sql])
          )
        end
      end
      
      def test_deployment_id
        g = fargate_generator
        
        assert_equal(
          '${var.deployment_id}',
          g.deployment_id
        )
      end
      
      def test_pyspark_s3_key_prefix
        g = fargate_generator
        expected = %{#{g.deployment_id}/scripts/aws_fargate/nightly_batch}
        
        assert_equal(
          expected,
          g.pyspark_s3_key_prefix(g.structure)
        )
      end
      
      def test_pyspark_library_key
        g = fargate_generator
        expected = %{#{g.pyspark_s3_key_prefix(g.structure)}/convergdb.zip}

        assert_equal(
          expected,
          g.pyspark_library_key(g.structure)
        )
      end
      
      def test_pyspark_script_key
        g = fargate_generator
        expected = %{#{g.pyspark_s3_key_prefix(g.structure)}/nightly_batch.py}
        
        assert_equal(
          expected,
          g.pyspark_script_key(g.structure)
        )
      end
      
      def test_apply_cast_type!
        g = fargate_generator
        s = fargate_friendly_structure
        
        g.apply_cast_type!(s)
        
        # source
        assert_equal(
          ["integer", "string", "string", "decimal(10,2)", "integer"],
          s[:source_structure][:attributes].map {|a| a[:cast_type] }
        )
        
        # target
        assert_equal(
          ["integer", "string", "string", "decimal(10,2)", "string", "decimal(10,2)"],
          s[:attributes].map {|a| a[:cast_type] }
        )
      end
      
      def test_post_initialize
        g = fargate_generator
        
        # insure that deployment_id is defaulted.
        # this value is resolved with a terraform template variable.
        assert_equal(
          '${deployment_id}',
          g.structure[:deployment_id]
        )
        
        # insure that region is defaulted.
        # this value is resolved with a terraform template variable.
        assert_equal(
          '${region}',
          g.structure[:region]
        )
        
        # get a list of all the source attributes with cast type applied
        source = g.structure[:source_structure][:attributes].select do |a|
          a.key?(:cast_type)
        end
        
        # get a list of all the target attributes with cast type applied
        target = g.structure[:attributes].select { |a| a.key?(:cast_type) }
        
        # we are just making sure that every attribute was mutated with the
        # apply_cast_type! method. we aren't testing the method itself so
        # we only need to check for the count of mutated attributes.
        assert_equal(
          5,
          source.length
        )

        assert_equal(
          6,
          target.length
        )
      end
      
      def test_pyspark_source_to_target
        g = fargate_generator
        
        assert_equal(
          File.read(
            "#{File.dirname(__FILE__)}/fixtures/fargate/pyspark_source_to_target.py"
          ),
          g.pyspark_source_to_target(g.structure),
          #g.pyspark_source_to_target(g.structure)
          pp(g.structure)
        )
      end
    end
  end
end
