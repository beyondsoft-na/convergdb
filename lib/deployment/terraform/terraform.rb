# ConvergDB - DevOps for Data
# Copyright (C) 2018 Beyondsoft Consulting, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
require 'erb'
require 'json'
require 'digest'
require 'fileutils'

module ConvergDB
  # generators to create deployable artifacts
  module Deployment
    # used to build a terraform deployment
    class BaseTerraform
      attr_accessor :resource_id
      attr_reader :resource_type
      attr_reader :validation_regex

      # resolves all subobjects to a single structure recursively
      def resolve!
        raise "resolve! must be implemented for class #{self.class}"
      end

      # performs validation... should be recursive for objects with children.
      # note that this will raise errors.
      def validate
        validate_string_attributes
      end

      # returns the structure, recursively resolved for any child objects.
      # every structure needs to have a :structure key containing the
      # definition for the terraform resource to be created. the :structure
      # should match tf_json.
      # @return [Hash]
      def structure
        raise "structure must be implemented for class #{self.class}"
      end

      # removes . and - from strings and replaces with __
      # @param [String] s input string
      # @return [String]
      def to_underscore(s)
        s.gsub(/\.|\-/, '__')
      end

      # removes . and _ from strings and replaces with --
      # @param [String] s input string
      # @return [String]
      def to_dash(s)
        s.gsub(/\.|\_/, '--')
      end

      # removes . and _ and - from string
      # @param [String] s input string
      # @return [String]
      def to_nothing(s)
        s.gsub(/\.|\_|\-/, '')
      end

      # outputs the tf_json format for the structure associated with
      # this object. this is what will actually be called at runtime.
      # @return [String]
      def tf_json
        to_tf_json(structure).join('')
      end

      # generates a decent looking text block that can be used in a terraform
      # json file (tf_json). this logic is separate from tf_json in order to
      # facilitate unit testing. the reason for this method existing is that
      # terraform allows for non-compliant json structures with duplicate
      # keys. for example... the module key can be repeated multiple times
      # within the document. it may be possible to create the output with a
      # properly merged json structure.
      # @param [Hash] structure
      # @return [String]
      def to_tf_json(structure)
        # structure_top_level_key = structure[:structure].keys.first
        k = structure[:structure].keys.first
        h = structure[:structure][k]
        ret = []
        ret << 34.chr + k.to_s + 34.chr + ' : { '
        ret << %("#{h.keys.first}" : #{JSON.pretty_generate(h[h.keys.first])})
        ret << '},'
      end

      # used for validating string parameters. pass in the
      # value to be tested along with the regex expression.
      # nil is an acceptable value if mandatory is not set to true.
      # note that this will return true if there is any match so
      # be specific with your pattern definition.
      # @param [String] str string to be tested
      # @param [Regexp] pattern regex pattern to match
      # @param [true, false] mandatory defaults to false
      # @return [true, false] indicates whether match was successful
      def valid_string_match?(str, pattern, mandatory = false)
        return true if str.nil? && (mandatory == false)
        # if a value is provided it must be a string
        return false unless str.class == String
        # if value is a string it must match
        return false unless str =~ pattern
        true
      end

      # validates all attributes defined by validation_regex
      def validate_string_attributes
        validation_regex.each_key do |m|
          t = valid_string_match?(
            send(m).to_s,
            validation_regex[m][:regex],
            validation_regex[m][:mandatory]
          )
          raise "#{self.class} error #{m} : #{send(m)}" unless t == true
        end
      end
    end

    # handles the creation of glue tables by way of a cf stack.
    class AWSGlueTablesModule < BaseTerraform
      attr_accessor :region
      attr_accessor :source
      attr_accessor :stack
      attr_accessor :stack_name
      attr_accessor :local_stack_file_path

      # @param [Hash] params
      def initialize(params)
        @resource_id = to_underscore(params[:resource_id])
        @region = params[:region]
        @source = './modules/aws_athena_relations'
        @local_stack_file_path =
          "#{params[:working_path]}/terraform/cloudformation/#{params[:resource_id]}.json"
        @stack = initialize_stack
        @stack_name = params[:resource_id]
      end

      # values with which to initialize the cf stack
      # @return [Hash]
      def initialize_stack
        {
          'AWSTemplateFormatVersion' => '2010-09-09',
          'Description' => 'Create ConvergDB tables in Glue catalog',
          'Resources' => {}
        }
      end

      # appends cf hash structure to the stack. this is performed once
      # for each table that is managed by the stack.
      # @param [Hash] structure
      def append_to_stack!(structure)
        @stack['Resources'].merge!(structure)
      end

      # @return [Hash]
      def validation_regex
        {
          resource_id: { regex: /.*/, mandatory: true }
        }
      end

      # @param [String] path
      # @param [Hash] stack cf stack for output as json
      def stack_to_file!(path, stack)
        # make sure the directory exists
        FileUtils.mkdir_p(File.dirname(path))

        File.open(path, 'w') do |f|
          f.puts(
            stack.to_json
          )
        end
      end

      # @return [Hash]
      def structure
        stack_to_file!(
          @local_stack_file_path,
          @stack
        )

        {
          resource_id: @resource_id,
          resource_type: :aws_glue_tables_module,
          structure: {
            module: {
              @resource_id => {
                source: @source,
                region: @region,
                stack_name: @stack_name,
                deployment_id: %(${var.deployment_id}),
                local_stack_file_path: @local_stack_file_path,
                s3_stack_key: %{${var.deployment_id}/cloudformation/#{@stack_name}_${var.deployment_id}.json},
                admin_bucket: %{${var.admin_bucket}}, # to pass downstream into the module
                data_bucket: %{${var.data_bucket}},    # to pass downstream into the module
                aws_account_id: '${data.aws_caller_identity.current.account_id}',
                database_stack_id: "${module.convergdb_athena_databases_stack.database_stack_id}"
              }
            }
          }
        }
      end
    end

    # manages the creation of glue databases which must be in place
    # before creating the tables.
    class AWSGlueDatabaseModule < BaseTerraform
      attr_accessor :region
      attr_accessor :source
      attr_accessor :stack

      # @param [Hash] params
      def initialize(params)
        @resource_id = to_underscore(params[:resource_id])
        @region = params[:region]
        @source = './modules/aws_athena_database'
        @stack = initialize_stack
      end

      # initializes the stack object
      def initialize_stack
        {
          'AWSTemplateFormatVersion' => '2010-09-09',
          'Description' => 'Create ConvergDB databases in Glue catalog',
          'Resources' => {}
        }
      end

      # appends cf hash structure to the stack
      # @param [Hash] structure
      def append_to_stack!(structure)
        @stack['Resources'].merge!(structure)
      end

      # @return [Hash]
      def validation_regex
        {
          resource_id: { regex: /.*/, mandatory: true }
        }
      end

      # @return [Hash]
      def structure
        {
          resource_id: @resource_id,
          resource_type: :aws_glue_database_module,
          structure: {
            module: {
              @resource_id => {
                source: @source,
                region: '${var.region}',
                stack: @stack.to_json,
                deployment_id: %(${var.deployment_id})
              }
            }
          }
        }
      end
    end

    # creates a glue etl job
    class AWSGlueETLJobModule < BaseTerraform
      attr_accessor :region
      attr_accessor :source
      attr_accessor :job_name
      attr_accessor :local_script
      attr_accessor :local_pyspark_library
      attr_accessor :script_bucket
      attr_accessor :script_key
      attr_accessor :pyspark_library_key
      attr_accessor :schedule
      attr_accessor :stack_name
      attr_accessor :service_role
      attr_accessor :dpu

      # @param [Hash] params
      def initialize(params)
        @resource_id = params[:resource_id]
        @region = params[:region]
        @source = './modules/aws_glue_etl_job'
        @job_name = params[:job_name]
        @local_script = params[:local_script]
        @local_pyspark_library = params[:local_pyspark_library]
        @script_bucket = params[:script_bucket]
        @script_key = params[:script_key]
        @pyspark_library_key = params[:pyspark_library_key]
        @schedule = params[:schedule]
        @stack_name = params[:stack_name]
        @service_role = params[:service_role] || ''
        @dpu = params[:dpu]
      end

      # @return [Hash]
      def validation_regex
        {
          resource_id: { regex: /.*/, mandatory: true }
        }
      end

      # @return [Hash]
      def structure
        {
          resource_id: @resource_id,
          resource_type: :aws_glue_etl_job_module,
          structure: {
            module: {
              @resource_id => {
                source: @source,
                stack_name: @stack_name,
                region: @region,
                job_name: @job_name,
                local_script: @local_script,
                local_pyspark_library: @local_pyspark_library,
                script_bucket: "${var.admin_bucket}",
                script_key: @script_key,
                pyspark_library_key: @pyspark_library_key,
                schedule: @schedule,
                service_role: @service_role,
                deployment_id: %(${var.deployment_id}),
                admin_bucket: "${var.admin_bucket}",
                data_bucket: "${var.data_bucket}",
                dpu: @dpu,
                cloudwatch_namespace: "convergdb/${var.deployment_id}",
                sns_topic: "${aws_sns_topic.convergdb-notifications.arn}"
              }
            }
          }
        }
      end
    end

    # builder object for a terraform deployment.
    class TerraformBuilder < BaseTerraform
      # @return [Array<BaseTerraform>]
      attr_reader :resources

      def initialize
        @resources = []
      end

      # confirms whether or not a resource_id exists.
      # @param [String] resource_id
      # @return [Boolean]
      def resource_id_exists?(resource_id)
        t = @resources.select do |r|
          r.resource_id == resource_id
        end
        return false if t == []
        true
      end

      # lookup to find the object associated with resource_id provided.
      # @param [String] resource_id
      # @return [BaseTerraform]
      def resource_by_id(resource_id)
        @resources.each do |r|
          return r if r.resource_id == resource_id
        end
        nil
      end

      # idempotently appends a AWSGlueTablesModule to the resources array.
      # after creation... the table is appended to the stack.
      # @param [Hash] params
      def aws_glue_table_module!(params)
        resource_id = to_underscore(params[:resource_id])
        unless resource_id_exists?(resource_id)
          c = AWSGlueTablesModule.new(params)
          @resources << c
        end

        resource_by_id(resource_id).append_to_stack!(
          params[:structure]
        )
      end

      # idempotently appends a AWSGlueDatabaseModule to the resources array.
      # after creation... the database is appended to the stack.
      # @param [Hash] params
      def aws_glue_database_module!(params)
        resource_id = params[:resource_id]
        unless resource_id_exists?(resource_id)
          c = AWSGlueDatabaseModule.new(params)
          @resources << c
        end

        resource_by_id(resource_id).append_to_stack!(
          params[:structure]
        )
      end

      # appends a AWSGlueETLJobModule to the resources array.
      # will not append an object if the resource_id already exists
      # @param [Hash] params
      def aws_glue_etl_job_module!(params)
        unless resource_id_exists?(to_underscore(params[:resource_id]))
          g = AWSGlueETLJobModule.new(params)
          @resources << g
        end
      end
    end
  end
end
