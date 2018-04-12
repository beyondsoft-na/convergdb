require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

# required for testing
require 'minitest'
require 'minitest/autorun'

# imports the ruby file we are testing
require_relative '../lib/generators/generate.rb'
require_relative '../lib/generators/glue/glue.rb'
require_relative 'helpers/dsd_ddd_ir/test_dsd_ddd_irs.rb'

module ConvergDB
  module Generators
    class TestAWSGlue < Minitest::Test
      def test_dir
        File.dirname(File.expand_path(__FILE__))
      end

      def primary_ir_structure
        tmp = JSON.parse(
          File.read(
            "#{test_dir}/fixtures/primary_ir/ir.json"
          ),
          :symbolize_names => true
        )
        h = {}
        tmp.each_key { |k| h[k.to_s] = tmp[k] }
        h
      end

      def glue_friendly_structure
        primary_ir = primary_ir_structure
        return primary_ir["production.ecommerce.inventory.books"]
      end

      def glue_generator(working_path = nil)
        # allow for optional working path for use
        # in artifact creation tests
        test_ir = glue_friendly_structure
        test_ir[:working_path] = working_path if working_path
        AWSGlue.new(
          test_ir,
          ConvergDB::Deployment::TerraformBuilder.new
        )
      end

      def test_generate!
        # no test because it is so much state change.
        # all functions and state changing methods in generate!
        # are tested elsewhere
      end

      def test_glue_etl_job_module_params
        g = glue_generator

        expected = {
          resource_id: "aws_glue_nightly_batch",
          region: "${var.region}",
          job_name: 'nightly_batch',
          local_script: g.etl_job_script_path(g.structure),
          local_pyspark_library: g.pyspark_library_path(g.structure),
          script_bucket: 'demo-utility-us-east-2.beyondsoft.us',
          script_key: g.pyspark_script_key(g.structure),
          pyspark_library_key: g.pyspark_library_key(g.structure),
          schedule: 'cron(0 0 * * ? *)',
          dpu: 2,
          stack_name: g.terraform_builder.to_dash(
            "convergdb-glue-nightly_batch"
          ) + '-${var.deployment_id}',
          service_role: "glueService"
        }

        assert_equal(
          expected,
          g.glue_etl_job_module_params(g.structure)
        )
      end

      def file_contents_match?(file1, file2)
        File.read(file1) == File.read(file2)
      end

      def test_create_static_artifacts!
        test_working_path = '/tmp/convergdb_glue_generator_test/'
        g = glue_generator(test_working_path)

        # FileUtils.mkdir_p(test_working_path)

        g.create_static_artifacts!(g.structure)

        # validate that modules were copied
        [
          'modules/aws_glue_etl_job/main.tf',
          'modules/aws_glue_etl_job/variables.tf'
        ].each do |tf|
          orig = File.expand_path(
            "#{File.dirname(__FILE__)}/../lib/generators/glue/#{tf}"
          )
          assert_equal(
            true,
            file_contents_match?(
              orig,
              "#{test_working_path}/terraform/#{tf}"
            )
          )
        end

        assert(
          file_contents_match?(
            "#{File.dirname(__FILE__)}/../lib/generators/glue/convergdb_pyspark_library.py",
            "#{test_working_path}/terraform/aws_glue/convergdb_pyspark_library.py"
          )
        )
      ensure
        FileUtils.rm_r(test_working_path) rescue nil
      end

      def test_tf_glue_path
        g = glue_generator

        assert_equal(
          "/tmp/terraform/aws_glue",
          g.tf_glue_path(g.structure)
        )
      end

      def test_etl_job_script_path
        g = glue_generator

        assert_equal(
          "#{g.tf_glue_path(g.structure)}/nightly_batch.py",
          g.etl_job_script_path(g.structure)
        )
      end

      def test_pyspark_library_path
        g = glue_generator

        assert_equal(
          "#{g.tf_glue_path(g.structure)}/convergdb_pyspark_library.py",
          g.pyspark_library_path(g.structure)
        )
      end

      def test_create_etl_script_if_not_exists!
        FileUtils.rm(test_path) rescue nil
        g = glue_generator
        test_path = '/tmp/test_etl_script.py'
        g.create_etl_script_if_not_exists!(test_path)

        assert(File.exist?(test_path))

        assert_equal(
          %{import convergdb_pyspark_library\n\n},
          File.read(test_path)
        )
      ensure
        FileUtils.rm(test_path) rescue nil
      end

      def test_append_to_job_script!
        FileUtils.rm(test_path) rescue nil
        g = glue_generator
        test_path = '/tmp/test_etl_script.py'
        g.create_etl_script_if_not_exists!(test_path)
        g.append_to_job_script!(
          test_path,
          'test append'
        )

        assert_equal(
          %{import convergdb_pyspark_library\n\ntest append\n\n},
          File.read(test_path)
        )
      ensure
        FileUtils.rm(test_path) rescue nil
      end

      def test_pyspark_cast_type
        g = glue_generator

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
        g = glue_generator
        
        assert_equal(
          '${var.deployment_id}',
          g.deployment_id
        )
      end
      
      def test_pyspark_s3_key_prefix
        g = glue_generator
        expected = %{#{g.deployment_id}/scripts/aws_glue/nightly_batch}
        
        assert_equal(
          expected,
          g.pyspark_s3_key_prefix(g.structure)
        )
      end
      
      def test_pyspark_library_key
        g = glue_generator
        expected = %{#{g.pyspark_s3_key_prefix(g.structure)}/convergdb_pyspark_library.py}

        assert_equal(
          expected,
          g.pyspark_library_key(g.structure)
        )
      end
      
      def test_pyspark_script_key
        g = glue_generator
        expected = %{#{g.pyspark_s3_key_prefix(g.structure)}/nightly_batch.py}
        
        assert_equal(
          expected,
          g.pyspark_script_key(g.structure)
        )
      end
      
      def test_apply_cast_type!
        g = glue_generator
        s = glue_friendly_structure
        
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
        g = glue_generator
        
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
      
      def test_pyspark_source_lambda_func
        g = glue_generator
        
        # first test is a simple field with no expression
        test1 = [
          {name: 'field1', data_type: 'varchar(10)', expression: nil }
        ]
        
        assert_equal(
          [
            'lambda doc : {',
            '  "field1" : doc.get("field1",None)',
            '}'
          ].join("\n"),
          g.pyspark_source_lambda_func(test1)
        )
        
        # adds a second field and a nested expressions
        test2 = [
          {name: 'field1', data_type: 'varchar(10)', expression: 'field1.nested' },
          {name: 'field2', data_type: 'varchar(10)', expression: nil }
        ]
        
        assert_equal(
          [
            'lambda doc : {',
            '  "field1" : doc.get("field1",{}).get("nested",None),',
            '  "field2" : doc.get("field2",None)',
            '}'
          ].join("\n"),
          g.pyspark_source_lambda_func(test2)
        )
        
        # adds a third field, deeper nesting, and capitalized expression
        test3 = [
          {name: 'field1', data_type: 'varchar(10)', expression: 'field1.nested' },
          {name: 'field2', data_type: 'varchar(10)', expression: nil },
          {name: 'field3', data_type: 'varchar(10)', expression: 'A.B.c' }
        ]
        
        assert_equal(
          [
            'lambda doc : {',
            '  "field1" : doc.get("field1",{}).get("nested",None),',
            '  "field2" : doc.get("field2",None),',
            '  "field3" : doc.get("A",{}).get("B",{}).get("c",None)',
            '}'
          ].join("\n"),
          g.pyspark_source_lambda_func(test3)
        )
      end
      
      def test_pyspark_source_to_target
        g = glue_generator
        
        assert_equal(
          File.read(
            "#{File.dirname(__FILE__)}/fixtures/glue/pyspark_source_to_target.py"
          ),
          g.pyspark_source_to_target(g.structure)
        )
      end
    end
  end
end
