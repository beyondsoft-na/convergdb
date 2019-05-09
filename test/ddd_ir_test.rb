require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

require 'minitest'
require 'minitest/autorun'
require 'pp'

require_relative '../lib/ir/ddd/ddd_ir.rb'

module ConvergDB
  module DDD
    class BaseDDDIRTest < Minitest::Test
      def tree_down_to(deployment_type, level)
        h = {}
        h[:top_level] = DDDTopLevel.new
        return h if level == :top_level

        if deployment_type == :athena
          h[:deployment] = AWSAthena.new(h[:top_level], 'env1')
          h[:top_level].deployment << h[:deployment]
          return h if level == :deployment

          h[:relation] = AWSAthenaRelation.new(h[:deployment])
          h[:deployment].relations << h[:relation]
          return h if level == :relation
        end

        if deployment_type == :s3_source
          h[:deployment] = AWSS3Source.new(h[:top_level], 'env1')
          h[:top_level].deployment << h[:deployment]
          return h if level == :deployment

          h[:relation] = AWSS3SourceRelation.new(h[:deployment])
          h[:deployment].relations << h[:relation]
          return h if level == :relation
        end
      end

      def raises_error?(object, method)
        is_err = false
        begin
          object.public_send(method.to_sym)
        rescue => e
          #puts e.message
          is_err = true
        end
        is_err
      end

      def catch_error?
        begin
          yield
          return false
        rescue => e
          puts "ERROR ERROR ERROR ERROR"
          puts e.message
          return true
        end
      end
    end

    # this class implements a simple form of BaseStructure
    # for use in testing
    class BaseStructureTestable < BaseStructure
      # provide a method with which we can test
      attr_accessor :some_method
    end

    class TestBaseStructure < BaseDDDIRTest
      def test_structure
        t = BaseStructure.new
        assert_equal(
          true,
          raises_error?(t, :structure)
        )
      end

      def test_resolve!
        t = BaseStructure.new
        assert_equal(
          true,
          raises_error?(t, :resolve!)
        )
      end

      def test_validate
        t = BaseStructure.new
        assert_equal(
          true,
          raises_error?(t, :validate)
        )
      end

      def test_override_parent
        t1 = BaseStructureTestable.new
        t2 = BaseStructureTestable.new

        t1.parent = t2
        t2.some_method = 22
        t1.some_method = nil

        # check to see if parent value overrides local nil
        assert_equal(
          22,
          t1.override_parent(:some_method)
        )

        t1.some_method = 23

        assert_equal(
          23,
          t1.override_parent(:some_method)
        )

        t1.some_method = nil
        t2.some_method = nil

        # both nil is acceptable
        assert_nil(
          t1.override_parent(:some_method)
        )
      end
      
      def test_is_convergdb_env_var?
        t1 = BaseStructureTestable.new
        [
          ['CONVERGDB_GOOD_VAR', true],
          ['convergdb_GOOD_VAR', false],
          ['HOSTNAME', false]
        ].each do |t|
          assert t[1] == t1.is_convergdb_env_var?(t[0])
        end
      end
      
      def test_strip_var_ref
        t1 = BaseStructureTestable.new
        assert 'CONVERGDB_ENV_VAR' == t1.strip_var_ref(
          '${env.CONVERGDB_ENV_VAR}'
        )
      end
      
      def test_convergdb_env_var
        # looks up a variable prefixed with CONVERGDB_
        t1 = BaseStructureTestable.new
        ENV['CONVERGDB_env_var_1'] = '1'
        assert '1' == t1.convergdb_env_var('CONVERGDB_env_var_1')
        ENV['CONVERGDB_env_var_1'] = nil
        
        # makes sure that you can't pull non-CONVERGDB_ prefixed env vars
        success = nil
        begin
          tmp = t1.convergdb_env_var('HOSTNAME')
          success = true
        rescue
          success = false
        end
        assert false == success
      end
      
      def test_env_vars_in_this_string
        # test for any string that matches syntax
        t1 = BaseStructureTestable.new
        expected = [
          '${env.CONVERGDB_1}',
          '${env.CONVERGDB_2}',
          '${env.HOSTNAME}'     # only needs to match syntax, not rules
        ]
        assert expected == t1.env_vars_in_this_string(
          'hello_${env.CONVERGDB_1}_${env.CONVERGDB_2}_${env.HOSTNAME}'
        )
        
        # test for uniqueness
        assert ['${env.CONVERGDB_1}'] == t1.env_vars_in_this_string(
          '${env.CONVERGDB_1}${env.CONVERGDB_1}${env.CONVERGDB_1}${env.CONVERGDB_1}'
        )
      end
      
      def test_apply_env_vars
        t1 = BaseStructureTestable.new
        
        expected = "hello world"
        
        ENV['CONVERGDB_A'] = 'hello'
        ENV['CONVERGDB_B'] = 'world'
        
        assert_equal(
          expected,
          t1.apply_env_vars(
            '${env.CONVERGDB_A} ${env.CONVERGDB_B}'
          ),
          puts(t1.apply_env_vars(
            '${env.CONVERGDB_A} ${env.CONVERGDB_B}'
          ))
        )
        
        ENV['CONVERGDB_1'] = nil
        ENV['CONVERGDB_2'] = nil
      end
      
      def test_apply_env_vars_to_attributes!
        t1 = BaseStructureTestable.new
        t1.some_method = '${env.CONVERGDB_A} ${env.CONVERGDB_B}'
        expected = "hello world"
        
        ENV['CONVERGDB_A'] = 'hello'
        ENV['CONVERGDB_B'] = 'world'
        
        t1.apply_env_vars_to_attributes!([:some_method])
        
        assert_equal(
          expected,
          t1.some_method
        )
        
        ENV['CONVERGDB_1'] = nil
        ENV['CONVERGDB_2'] = nil
      end
    end

    class TestDDDTopLevel < BaseDDDIRTest
      def test_initialize
        t = DDDTopLevel.new
        assert_equal(
          [],
          t.structure
        )
      end

      def test_resolve!
        # resolve! will not change top level
        t = tree_down_to(:athena, :top_level)
        t[:top_level].resolve!

        assert_equal(
          [],
          t[:top_level].structure
        )

        # in this case... resolve! will go down to
        # the relation level. region is an attribute
        # which will be passed down from the deployment (parent)
        # to the relation (child).
        t = tree_down_to(:athena, :relation)
        # t[:deployment].region = 'us-west-2'

        # relation dsd must be defined or resolve! will fail
        assert_equal(
          true,
          raises_error?(t[:top_level], :resolve!)
        )

        # fixing the dsd allows resolve! success
        t[:relation].dsd = 'domain.schema.relation'
        assert_equal(
          false,
          raises_error?(t[:top_level], :resolve!)
        )
      end

      def test_structure
        # empty top level returns empty array
        t = DDDTopLevel.new
        assert_equal(
          [],
          t.structure
        )

        # adding a deployment returns more
        t = tree_down_to(:athena, :deployment)
        assert_equal(
          [
            {
              environment: 'env1',
              domain_name: nil,
              schema_name: nil,
              # region: nil,
              service_role: nil,
              script_bucket: nil,
              temp_s3_location: nil,
              storage_format: nil,
              source_relation_prefix: nil,
              etl_job_name: nil,
              etl_job_schedule: nil,
              etl_job_dpu: nil,
              etl_technology: nil,
              etl_docker_image: nil,
              etl_docker_image_digest: nil,
              relations: []
            }
          ],
          t[:top_level].structure
        )

        # dpu should get resolved here
        t[:top_level].resolve!
        assert_equal(
          [
            {
              environment: 'env1',
              domain_name: nil,
              schema_name: nil,
              # region: nil,
              service_role: nil,
              script_bucket: '${var.admin_bucket}',
              temp_s3_location: nil,
              storage_format: nil,
              source_relation_prefix: nil,
              etl_job_name: nil,
              etl_job_schedule: nil,
              etl_job_dpu: 2,
              etl_technology: 'aws_glue',
              etl_docker_image: nil,
              etl_docker_image_digest: nil,
              relations: []
            }
          ],
          t[:top_level].structure
        )
      end

      def test_validate
        # empty top level structure will validate
        # without issue because there are no attributes
        t = DDDTopLevel.new
        assert_equal(
          false,
          raises_error?(t, :validate)
        )

        # in the case below... the deployment is not valid
        # because not all attributes are set... therefore
        # the entire validation fails
        t = tree_down_to(:athena, :deployment)
        assert_equal(
          true,
          raises_error?(t[:top_level], :validate)
        )
      end
    end

    class TestDDDIRAWSAthena < BaseDDDIRTest

      def test_initialize
        t = tree_down_to(:athena, :deployment)

        assert_equal(
          "env1",
          t[:deployment].environment
        )

        assert_equal(
          [],
          t[:deployment].relations
        )
      end

      # THIS TEST IS INCOMPLETE
      # iterates through an array of test cases
      # to determine if the regex is working for the test case.
      def test_validation_regex
        # create athena object
        a = tree_down_to(:athena, :deployment)[:deployment]
        # this array is the test cases
        [
          [:environment, 'valid', true],
          [:environment, 'in valid', false],
          [:environment, '1', false],
          [:environment, 's3://something', false],

          [:domain_name, 'valid', true],
          [:domain_name, 'in valid', false],
          [:domain_name, '1', false],
          [:domain_name, 's3://something', false],

          [:schema_name, 'valid', true],
          [:schema_name, 'in valid', false],
          [:schema_name, '1', false],
          [:schema_name, 's3://something', false],

#           [:region, 'us-west-2', true],
#           [:region, 'us-east-2', true],
#           [:region, 'ap-southeast-2', true],
#           [:region, 'some-other-crap', false],

          [:service_role, 'glueService', true],
          [:service_role, '123abc', false],

          [:script_bucket, 'script-bucket', true],
          [:script_bucket, 's3://bucket', true],

          [:temp_s3_location, 's3://bucket', true],

          [:storage_format, 'parquet', true],
        ].each do |t|
          # if the regex specified by t[0] value of validation_regex hash
          # returns an object the actual value is true... otherwise
          # it is false. t[2] is the expected value. both should
          # match for a successful assertion.
          r = a.validation_regex[t[0]][:regex]
          assert_equal(
            t[2],
            t[1].match(r) ? true : false,
            "#{t} with regex #{r}"
          )
        end
      end

      def test_structure
        t = tree_down_to(:athena, :deployment)
        t[:deployment].schema_name = 'schema'
        t[:deployment].domain_name = 'database'
        # t[:deployment].region = 'us-west-2'
        t[:deployment].service_role = 'glueService'
        t[:deployment].script_bucket = 'script-bucket'
        t[:deployment].temp_s3_location = 's3://temp/s3loc/'
        t[:deployment].storage_format = 'parquet'
        t[:deployment].etl_job_name = 'nightly_batch'
        t[:deployment].etl_job_schedule = 'cron(0 0 * * ? *)'
        t[:deployment].etl_job_dpu = '22'
        t[:deployment].etl_technology = 'aws_glue'
        t[:deployment].etl_docker_image = 'beyondsoftna/convergdb:latest'
        t[:deployment].etl_docker_image_digest = '@sha25612345678'

        assert_equal(
          {
            environment: 'env1',
            schema_name: 'schema',
            domain_name: 'database',
            service_role: 'glueService',
            script_bucket: 'script-bucket',
            temp_s3_location: 's3://temp/s3loc/',
            storage_format: 'parquet',
            source_relation_prefix: nil,
            etl_job_name: 'nightly_batch',
            etl_job_schedule: 'cron(0 0 * * ? *)',
            etl_job_dpu: '22',
            etl_technology: 'aws_glue',
            etl_docker_image: 'beyondsoftna/convergdb:latest',
            etl_docker_image_digest: '@sha25612345678',
            relations: []
          },
          t[:deployment].structure,
          pp(t[:deployment].structure)
        )
      end

      def test_resolve!
        t = tree_down_to(:athena, :relation)
        t[:deployment].region = 'us-west-2'
        t[:relation].dsd = 'domain.schema.relation'

        assert_equal(
          false,
          raises_error?(t[:deployment], :resolve!)
        )

        # demonstrates that recursive resolve! took place
        # down to relation level.
        assert_equal(
          t[:deployment].environment,
          t[:relation].environment
        )
        
        # test that env vars can be substituted
        t = tree_down_to(:athena, :deployment)
        
        ENV['CONVERGDB_SOME_BUCKET'] = 'hello'
        t[:deployment].environment = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].domain_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].schema_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].service_role = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].script_bucket = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].temp_s3_location = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].source_relation_prefix = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].etl_job_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].resolve!

        assert 'hello' == t[:deployment].environment
        assert 'hello' == t[:deployment].domain_name
        assert 'hello' == t[:deployment].schema_name
        assert 'hello' == t[:deployment].service_role
        assert 'hello' == t[:deployment].script_bucket
        assert 'hello' == t[:deployment].temp_s3_location
        assert 'hello' == t[:deployment].source_relation_prefix
        assert 'hello' == t[:deployment].etl_job_name
      
        ENV['CONVERGDB_SOME_BUCKET'] = nil
      end

      def test_validate
        t = tree_down_to(:athena, :deployment)

        # attributes not set will raise failure
        assert_equal(
          true,
          raises_error?(t[:deployment], :validate)
        )

        t[:deployment].service_role = 'glueService'
        t[:deployment].etl_job_name = 'etl_job'
        t[:deployment].etl_job_schedule = 'cron(0 0 * * ? *)'
        t[:deployment].etl_job_dpu = 33
        t[:deployment].etl_technology = 'aws_glue'

        # setting required attributes fixes validation
        assert_equal(
          false,
          raises_error?(t[:deployment], :validate)
        )
      end

      def test_validate_etl
        # validate glue etl
        t = tree_down_to(:athena, :deployment)
        params1 = [
          'aws_glue',
          nil,
          nil,
          2
        ]

        assert_equal(
          false,
          catch_error? do
            t[:deployment].validate_etl(*params1)
          end
        )

      end
    end

    class TestAWSAthenaRelation < BaseDDDIRTest
      def test_comparable
        t = tree_down_to(:athena, :relation)
        t[:relation].dsd = 'domain.schema.relation'
        t[:relation].storage_bucket = 'storage_bucket'
        t[:relation].state_bucket = 'state_bucket'
        t[:relation].storage_format = 'parquet'
        t[:relation].spark_partition_count = 22
        
        # etl_job_name is inherited from the parent deployment
        t[:deployment].etl_job_name = 'nightly_batch'
        
        # make sure everything is ready to go
        t[:relation].resolve!

        assert_equal(
          {
            full_relation_name: 'env1.domain.schema.relation',
            dsd: 'domain.schema.relation',
            storage_bucket: 'storage_bucket',
            state_bucket: 'state_bucket',
            storage_format: 'parquet',
            etl_job_name: 'nightly_batch',
            spark_partition_count: 22
          },
          t[:relation].comparable
        )
      end

      def test_initialize
        t = tree_down_to(:athena, :relation)

        # parent is the only attribute set during initialize
        assert_equal(
          t[:deployment],
          t[:relation].parent
        )
      end

      def test_resolve!
        # test basic resolve actions
        t = tree_down_to(:athena, :relation)
        t[:deployment].service_role = 'glueService'
        t[:relation].dsd = 'domain.schema.relation'

        assert_equal(
          false,
          raises_error?(t[:top_level], :resolve!)
        )

        assert_equal(
          [
            # t[:deployment].region,
            t[:deployment].service_role,
            'env1.domain.schema.relation',
          ],
          [
            # t[:relation].region,
            t[:relation].service_role,
            t[:relation].full_relation_name
          ]
        )

        # resolve actions for inventory
        t = tree_down_to(:athena, :relation)
        t[:deployment].service_role = 'glueService'
        t[:relation].dsd = 'domain.schema.relation'
        t[:top_level].resolve!

        assert_equal(
          'default',
          t[:relation].inventory_source
        )

        # resolve for when both inventory attributes specified
        # inventory_source takes precedence
        t = tree_down_to(:athena, :relation)
        t[:deployment].service_role = 'glueService'
        t[:relation].dsd = 'domain.schema.relation'
        t[:relation].use_inventory = 'true'
        t[:relation].inventory_source = 'streaming'
        t[:top_level].resolve!

        assert_equal(
          'streaming',
          t[:relation].inventory_source
        )

        # resolve for when only use_inventory specified
        t = tree_down_to(:athena, :relation)
        t[:deployment].service_role = 'glueService'
        t[:relation].dsd = 'domain.schema.relation'
        t[:relation].use_inventory = 'true'
        t[:top_level].resolve!

        assert_equal(
          's3',
          t[:relation].inventory_source
        )
        
        # test that env vars can be substituted
        t = tree_down_to(:athena, :relation)
        
        ENV['CONVERGDB_SOME_BUCKET'] = 'hello'
        t[:relation].dsd = 'a.b.c'
        t[:relation].domain_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].schema_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].relation_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].service_role = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].script_bucket = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].temp_s3_location = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].source_relation_prefix = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].resolve!

        assert 'hello' == t[:relation].domain_name
        assert 'hello' == t[:relation].schema_name
        assert 'hello' == t[:relation].relation_name
        assert 'hello' == t[:relation].service_role
        assert 'hello' == t[:relation].script_bucket
        assert 'hello' == t[:relation].temp_s3_location
        assert 'hello' == t[:relation].source_relation_prefix
      
        ENV['CONVERGDB_SOME_BUCKET'] = nil
      end

      def test_structure
        t = tree_down_to(:athena, :relation)

        t[:relation].dsd = 'domain.schema.relation'
        t[:relation].domain_name = 'domain'
        t[:relation].schema_name = 'schema'
        t[:relation].relation_name = 'relation'
        t[:relation].service_role = 'glueService'
        t[:relation].script_bucket = 'script-bucket'
        t[:relation].temp_s3_location = 's3://temp/loc'
        t[:relation].storage_bucket = 'storage-bucket'
        t[:relation].state_bucket = 'state-bucket'
        t[:relation].storage_format = 'parquet'
        t[:relation].source_relation_prefix = 'domain2.schema2.relation2'
        t[:relation].use_inventory = 'true'
        t[:deployment].etl_job_name = 'etl_job'
        t[:deployment].etl_job_schedule = 'cron(0 0 * * ? *)'
        t[:deployment].etl_job_dpu = 22

        t[:relation].resolve!

        assert_equal(
          {
            generators: [
              :athena,
              :glue,
              :fargate,
              :markdown_doc,
              :html_doc,
              :control_table
            ],
            full_relation_name: 'env1.domain.schema.relation',
            dsd: 'domain.schema.relation',
            environment: 'env1',
            domain_name: 'domain',
            schema_name: 'schema',
            relation_name: 'relation',
            service_role: 'glueService',
            script_bucket: 'script-bucket',
            temp_s3_location: 's3://temp/loc',
            storage_bucket: 'storage-bucket',
            state_bucket: 'state-bucket',
            storage_format: 'parquet',
            source_relation_prefix: 'domain2.schema2.relation2',
            use_inventory: 'true',
            inventory_source: 's3',
            etl_job_name: 'etl_job',
            etl_job_schedule: 'cron(0 0 * * ? *)',
            etl_job_dpu: 22,
            etl_technology: nil, # set in resolved parent
            etl_docker_image: nil, # set in resolved parent
            etl_docker_image_digest: nil, # set in resolved parent
            spark_partition_count: nil
          },
          t[:relation].structure
        )
      end

      def test_validate
        t = tree_down_to(:athena, :relation)
        t[:deployment].region = 'us-west-2'

        # t[:relation].dsd = 'domain.schema.relation'
        t[:relation].domain_name = 'domain'
        t[:relation].schema_name = 'schema'
        t[:relation].relation_name = 'relation'
        # t[:relation].service_role = 'glueService'
        t[:relation].script_bucket = 'script-bucket'
        t[:relation].temp_s3_location = 's3://temp/loc'
        t[:relation].storage_bucket = 'storage-bucket'
        t[:relation].state_bucket = 'state-bucket'
        t[:relation].storage_format = 'parquet'
        t[:relation].source_relation_prefix = 'domain2.schema2.relation2'

        assert_equal(
          true,
          raises_error?(t[:relation], :validate)
        )

        # add the attributes...
        t[:relation].service_role = 'glueService'
        t[:relation].dsd = 'domain.schema.relation'

        # ...then resolve the rest
        t[:relation].resolve!
        assert_equal(
          false,
          raises_error?(t[:relation], :validate)
        )
      end

      def test_validation_regex
        # create athena object
        a = tree_down_to(:athena, :relation)[:relation]
        # this array is the test cases
        [
          [:environment, 'valid', true],
          [:environment, 'in valid', false],
          [:environment, '1', false],
          [:environment, 's3://something', false],

          [:domain_name, 'valid', true],
          [:domain_name, 'in valid', false],
          [:domain_name, '1', false],
          [:domain_name, 's3://something', false],

          [:schema_name, 'valid', true],
          [:schema_name, 'in valid', false],
          [:schema_name, '1', false],
          [:schema_name, 's3://something', false],

#          [:region, 'us-west-2', true],
#          [:region, 'us-east-2', true],
#          [:region, 'ap-southeast-2', true],
#          [:region, 'some-other-crap', false],

          [:service_role, 'glueService', true],
          [:service_role, '123abc', false],

          [:script_bucket, 'script-bucket', true],
          [:script_bucket, 's3://bucket', true],

          [:temp_s3_location, 's3://bucket', true],

          [:storage_format, 'parquet', true],

          [:storage_bucket, 'storage-bucket', true],
          [:storage_bucket, 's3://bucket', true],

          [:state_bucket, 'state-bucket', true],
          [:state_bucket, 's3://bucket', true],

          [:source_relation_prefix, 'env1', true],
          [:source_relation_prefix, 'env1.', false],
          [:source_relation_prefix, 'env1.domain', true],
          [:source_relation_prefix, 'env1.domain.', false],
          [:source_relation_prefix, 'env1.domain.schema', true],
          [:source_relation_prefix, 'env1.domain.schema.', false],
          [:source_relation_prefix, 'env1.domain.schema.relation', true],
          [:source_relation_prefix, 'env1.domain.schema.relation.', false],

          [:source_relation_prefix, '1env1', false],
          [:source_relation_prefix, 'env1.1domain', false],
          [:source_relation_prefix, 'env1.domain.1schema', false],
          [:source_relation_prefix, 'env1.domain.schema.1relation', false],
        ].each do |t|
          # if the regex specified by t[0] value of validation_regex hash
          # returns an object the actual value is true... otherwise
          # it is false. t[2] is the expected value. both should
          # match for a successful assertion.
          r = a.validation_regex[t[0]][:regex]
          assert_equal(
            t[2],
            t[1].match(r) ? true : false,
            "#{t} with regex #{r}"
          )
        end
      end

      def test_resolve_full_relation_name
        a = tree_down_to(:athena, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.resolve!
        assert_equal(
          'env1.domain.schema.relation',
          a.resolve_full_relation_name
        )

        a = tree_down_to(:athena, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.domain_name = 'domain2'
        a.resolve!
        assert_equal(
          'env1.domain2.schema.relation',
          a.resolve_full_relation_name
        )

        a = tree_down_to(:athena, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.schema_name = 'schema2'
        a.resolve!
        assert_equal(
          'env1.domain.schema2.relation',
          a.resolve_full_relation_name
        )

        a = tree_down_to(:athena, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.relation_name = 'relation2'
        a.resolve!
        assert_equal(
          'env1.domain.schema.relation2',
          a.resolve_full_relation_name
        )

        a = tree_down_to(:athena, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.domain_name = 'domain2'
        a.schema_name = 'schema2'
        a.relation_name = 'relation2'
        a.resolve!
        assert_equal(
          'env1.domain2.schema2.relation2',
          a.resolve_full_relation_name
        )
      end
    end

    class TestAWSS3Source < BaseDDDIRTest
      def test_initialize
        t = tree_down_to(:s3_source, :deployment)

        assert_equal(
          'env1',
          t[:deployment].environment
        )

        assert_equal(
          [],
          t[:deployment].relations
        )

        assert_equal(
          t[:top_level],
          t[:deployment].parent
        )
      end

      def test_resolve!
        # nothing to resolve at s3 source level
        # so we test to see the recursive resolve!
        # of the relations.
        t = tree_down_to(:s3_source, :relation)
        # t[:deployment].region = 'us-west-2'
        t[:relation].dsd = 'domain.schema.relation'
        t[:relation].storage_bucket = 'bucket-name'
        t[:top_level].resolve!

        assert_equal(
          t[:deployment].environment,
          t[:relation].environment
        )
        
        # test that env vars can be substituted
        t = tree_down_to(:s3_source, :deployment)
        
        ENV['CONVERGDB_SOME_BUCKET'] = 'hello'
        
        t[:deployment].environment = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].domain_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].schema_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].resolve!
        
        assert 'hello' == t[:deployment].environment
        assert 'hello' == t[:deployment].domain_name
        assert 'hello' == t[:deployment].schema_name

        ENV['CONVERGDB_SOME_BUCKET'] = nil
      end

      def test_structure
        t = tree_down_to(:s3_source, :deployment)[:deployment]
        # t.region = 'us-west-2'
        t.domain_name = 'domain'
        t.schema_name = 'schema'
        assert_equal(
          {
            # region: 'us-west-2',
            environment: 'env1',
            domain_name: 'domain',
            schema_name: 'schema',
            relations: []
          },
          t.structure
        )
      end

      def test_validate
        t = tree_down_to(:s3_source, :deployment)
        raised_error = nil
        err = nil
        begin
          t[:deployment].validate
          raised_error = false
        rescue => e
          err = e
          raised_error = true
        end
        
        assert_equal(
          false,
          raised_error,
          (err.message if err)
        )

#        t[:deployment].region = 'us-west-2'
#        assert_equal(
#          false,
#          raises_error?(t[:deployment], :validate)
#        )
      end

      def test_validation_regex
        a = tree_down_to(:s3_source, :deployment)[:deployment]
        # this array is the test cases
        [
          [:environment, 'valid', true],
          [:environment, 'in valid', false],
          [:environment, '1', false],
          [:environment, 's3://something', false],

          [:domain_name, 'valid', true],
          [:domain_name, 'in valid', false],
          [:domain_name, '1', false],
          [:domain_name, 's3://something', false],

          [:schema_name, 'valid', true],
          [:schema_name, 'in valid', false],
          [:schema_name, '1', false],
          [:schema_name, 's3://something', false]
        ].each do |t|
          # if the regex specified by t[0] value of validation_regex hash
          # returns an object the actual value is true... otherwise
          # it is false. t[2] is the expected value. both should
          # match for a successful assertion.
          r = a.validation_regex[t[0]][:regex]
          assert_equal(
            t[2],
            t[1].match(r) ? true : false,
            "#{t} with regex #{r}"
          )
        end
      end
    end

    class TestAWSS3SourceRelation < BaseDDDIRTest
      def test_initialize
        t = tree_down_to(:s3_source, :relation)

        assert_equal(
          t[:deployment],
          t[:relation].parent
        )
      end

      def resolve_structure_base
        t = tree_down_to(:s3_source, :relation)
        t[:relation].dsd = 'domain.schema.relation'
        t[:deployment].domain_name = 'domain2'
        t[:deployment].schema_name = 'schema2'
        t[:relation].storage_bucket = 'bucket-name'
        t
      end

      def test_resolve!
        t = resolve_structure_base
        t[:relation].resolve!

        # sourced from parent deployment
        assert_equal(
          t[:deployment].environment,
          t[:relation].environment
        )

        # set by parent
        assert_equal(
          'domain2',
          t[:relation].domain_name
        )

        # set by parent
        assert_equal(
          'schema2',
          t[:relation].schema_name
        )

        # full_relation_name has overrides for each namespace
        # except environment.
        assert_equal(
          'env1.domain2.schema2.relation',
          t[:relation].full_relation_name
        )

        assert_equal(
          '',
          t[:relation].inventory_table
        )

        assert_equal(
          'false',
          t[:relation].streaming_inventory
        )

        assert_equal(
          nil,
          t[:relation].streaming_inventory_output_bucket
        )

        assert_equal(
          nil,
          t[:relation].streaming_inventory_table
        )

        # deeper testing for inventory handling
        t = resolve_structure_base
        t[:relation].streaming_inventory = 'true'
        t[:relation].resolve!

        assert_equal(
          '${var.admin_bucket}/${var.deployment_id}/streaming_inventory/bucket-name/',
          t[:relation].streaming_inventory_output_bucket
        )

        assert_equal(
          'convergdb_inventory_${var.deployment_id}.bucket__name',
          t[:relation].streaming_inventory_table
        )

        # streaming inventory attributes specified
        t = resolve_structure_base
        t[:relation].storage_bucket = 'bucket-name/some/prefix'
        t[:relation].streaming_inventory = 'true'
        t[:relation].streaming_inventory_output_bucket = 'bucket/prefix'
        t[:relation].streaming_inventory_table = 'this_database.this_table'
        t[:relation].resolve!

        assert_equal(
          'bucket/prefix',
          t[:relation].streaming_inventory_output_bucket
        )

        assert_equal(
          'this_database.this_table',
          t[:relation].streaming_inventory_table
        )

        # test that env vars can be substituted
        t = resolve_structure_base
        
        ENV['CONVERGDB_SOME_BUCKET'] = 'hello'
        
        t[:relation].domain_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].schema_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].relation_name = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].storage_bucket = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].inventory_table = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].streaming_inventory_output_bucket = "${env.CONVERGDB_SOME_BUCKET}"
        t[:relation].streaming_inventory_table = "${env.CONVERGDB_SOME_BUCKET}"
        t[:deployment].resolve!

        assert_equal(
          'hello',
          t[:relation].domain_name,
          t[:relation].domain_name
        )
        assert 'hello' == t[:relation].schema_name
        assert 'hello' == t[:relation].relation_name
        assert 'hello' == t[:relation].storage_bucket
        assert 'hello' == t[:relation].inventory_table
        assert 'hello' == t[:relation].streaming_inventory_output_bucket
        assert 'hello' == t[:relation].streaming_inventory_table
        
        ENV['CONVERGDB_SOME_BUCKET'] = nil
      end

      def test_structure
        t = tree_down_to(:s3_source, :relation)
        # t[:deployment].region = 'us-west-2'
        t[:relation].dsd = 'domain.schema.relation'
        t[:relation].domain_name = 'domain2'
        t[:relation].schema_name = 'schema2'
        t[:relation].relation_name = 'relation2'
        t[:relation].storage_bucket = 'some-bucket'
        t[:relation].storage_format = 'json'

        t[:relation].resolve!

        assert_equal(
          {
            generators: [
              :streaming_inventory,
              :s3_source,
              :markdown_doc,
              :html_doc
            ],
            dsd: 'domain.schema.relation',
            full_relation_name: 'env1.domain2.schema2.relation2',
            environment: 'env1',
            domain_name: 'domain2',
            schema_name: 'schema2',
            relation_name: 'relation2',
            storage_bucket: 'some-bucket',
            storage_format: 'json',
            inventory_table: '',
            streaming_inventory: "false",
            streaming_inventory_output_bucket: nil,
            streaming_inventory_table: nil,
            csv_header: nil,
            csv_separator: nil,
            csv_quote: nil,
            csv_null: nil,
            csv_escape: nil,
            csv_trim: nil
          },
          t[:relation].structure
        )
      end

      def test_validate
        t = tree_down_to(:s3_source, :relation)
        # t[:deployment].region = 'us-west-2'

        assert_equal(
          true,
          raises_error?(t[:relation], :validate)
        )
      end

      def test_validation_regex
        a = tree_down_to(:s3_source, :relation)[:relation]
        # this array is the test cases
        [
          [:environment, 'valid', true],
          [:environment, 'in valid', false],
          [:environment, '1', false],
          [:environment, 's3://something', false],

          [:domain_name, 'valid', true],
          [:domain_name, 'in valid', false],
          [:domain_name, '1', false],
          [:domain_name, 's3://something', false],

          [:schema_name, 'valid', true],
          [:schema_name, 'in valid', false],
          [:schema_name, '1', false],
          [:schema_name, 's3://something', false],

          [:relation_name, 'valid', true],
          [:relation_name, 'in valid', false],
          [:relation_name, '1', false],
          [:relation_name, 's3://something', false],

#          [:region, 'us-west-2', true],
#          [:region, 'us-east-2', true],
#          [:region, 'ap-southeast-2', true],
#          [:region, 'some-other-crap', false],

          [:storage_format, 'json', true],

          [:storage_bucket, 'storage-bucket', true],
          [:storage_bucket, 's3://bucket', true],

          [:streaming_inventory, 'true', true],
          [:streaming_inventory, 'false', true],
          [:streaming_inventory, 'True', true],
          [:streaming_inventory, 'josemadre', false],

        ].each do |t|
          # if the regex specified by t[0] value of validation_regex hash
          # returns an object the actual value is true... otherwise
          # it is false. t[2] is the expected value. both should
          # match for a successful assertion.
          r = a.validation_regex[t[0]][:regex]
          assert_equal(
            t[2],
            t[1].match(r) ? true : false,
            "#{t} with regex #{r}"
          )
        end
      end

      def test_resolve_full_relation_name
        a = tree_down_to(:s3_source, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.storage_bucket = 'bucket-name'
        a.resolve!
        assert_equal(
          'env1.domain.schema.relation',
          a.resolve_full_relation_name
        )

        a = tree_down_to(:s3_source, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.storage_bucket = 'bucket-name'
        a.domain_name = 'domain2'
        a.resolve!
        assert_equal(
          'env1.domain2.schema.relation',
          a.resolve_full_relation_name
        )

        a = tree_down_to(:s3_source, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.storage_bucket = 'bucket-name'
        a.schema_name = 'schema2'
        a.resolve!
        assert_equal(
          'env1.domain.schema2.relation',
          a.resolve_full_relation_name
        )

        a = tree_down_to(:s3_source, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.storage_bucket = 'bucket-name'
        a.relation_name = 'relation2'
        a.resolve!
        assert_equal(
          'env1.domain.schema.relation2',
          a.resolve_full_relation_name
        )

        a = tree_down_to(:s3_source, :relation)[:relation]
        a.dsd = 'domain.schema.relation'
        a.storage_bucket = 'bucket-name'
        a.domain_name = 'domain2'
        a.schema_name = 'schema2'
        a.relation_name = 'relation2'
        a.resolve!
        assert_equal(
          'env1.domain2.schema2.relation2',
          a.resolve_full_relation_name
        )
      end
    end

    class TestDDDIRBuilder < BaseDDDIRTest
      def raises_error?(&block)
        ret = false
        begin
          block.call
        rescue => e
          ret = true
        end
        ret
      end

      def factory_down_to(deployment_type, depth)
        f = DDDIRBuilder.new
        return f if depth == :top_level

        f.deployment(deployment_type, 'env1')
        return f if depth == :deployment

        f.relation
        return f if depth == :relation
      end

      def test_ddd_ir_factory_initialize
        f = DDDIRBuilder.new

        assert_equal(
          DDDTopLevel,
          f.top_level.class
        )
      end

      def test_attribute
        # test deployment level attribute
        f = factory_down_to(:athena, :deployment)
        f.attribute(:deployment, 'region', 'us-west-2')

        assert_equal(
          'us-west-2',
          f.current_deployment.region
        )

        # test relation level attribute
        f = factory_down_to(:athena, :relation)
        f.attribute(:relation, 'storage_format', 'parquet')

        assert_equal(
          'parquet',
          f.current_relation.storage_format
        )

        # test raise error on attribute already set
        f = factory_down_to(:athena, :relation)
        f.attribute(:relation, 'storage_format', 'parquet')

        assert_equal(
          true,
          raises_error? { f.attribute(:relation, 'storage_format', 'parquet') }
        )
      end

      def test_clear_state_below
        f = factory_down_to(:athena, :relation)
        assert_equal(
          false,
          f.current_relation.nil?
        )

        f.clear_state_below(DDDIRBuilder::States::DEPLOYMENT)
        assert_equal(
          true,
          f.current_relation.nil?
        )
      end

      def test_current_state_depth
        # there are only two state objects so testing is minimal
        f = factory_down_to(:athena, :relation)
        assert_equal(
          DDDIRBuilder::States::RELATION,
          f.current_state_depth
        )

        # test after clearing below deployment
        f.clear_state_below(DDDIRBuilder::States::DEPLOYMENT)
        assert_equal(
          DDDIRBuilder::States::DEPLOYMENT,
          f.current_state_depth
        )
      end

      def test_deployment
        f = factory_down_to(:athena, :relation)

        # create an additional deployment
        f.deployment(:s3_source, 'env1')

        # check if 2 now exist
        assert_equal(
          2,
          f.top_level.deployment.length
        )

        assert_equal(
          AWSS3Source,
          f.top_level.deployment[1].class
        )
      end

      def test_relation
        f = factory_down_to(:s3_source, :deployment)
        f.relation

        assert_equal(
          AWSS3SourceRelation,
          f.top_level.deployment[0].relations[0].class
        )
      end

      def test_state_depth_must_be
        f = factory_down_to(:s3_source, :deployment)

        assert_equal(
          false,
          raises_error? { f.state_depth_must_be(DDDIRBuilder::States::DEPLOYMENT) }
        )

        assert_equal(
          true,
          raises_error? { f.state_depth_must_be(DDDIRBuilder::States::RELATION) }
        )

        f.relation

        assert_equal(
          false,
          raises_error? { f.state_depth_must_be(DDDIRBuilder::States::RELATION) }
        )
      end

      def test_states
        f = factory_down_to(:s3_source, :relation)

        assert_equal(
          [
            AWSS3Source,
            AWSS3SourceRelation
          ],
          [
            f.current_deployment.class,
            f.current_relation.class
          ]
        )
      end
    end
  end
end
