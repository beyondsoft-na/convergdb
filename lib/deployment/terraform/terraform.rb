# Copyright © 2020 Beyondsoft Consulting, Inc.

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the “Software”), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
# BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require 'erb'
require 'json'
require 'digest'
require 'fileutils'

require_relative './../../version.rb'

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
      
      def database_module_name(database_name)
        "database_#{Digest::MD5.new.hexdigest(database_name)}"
      end

      def tf_hcl
        module_structure_to_hcl(structure)
      end
      
      def module_structure_to_hcl(s)
        ret = []
        ret << %{module "#{s[:structure][:module].keys.first.to_s}" \{}
        s[:structure][:module][s[:structure][:module].keys.first.to_s].keys.each do |k|
          ret << "  #{k.to_s} = #{value_to_hcl(s[:structure][:module][s[:structure][:module].keys.first.to_s][k])}"
        end
        ret << '}'
        ret.join("\n")
      end
      
      def value_to_hcl(v)
        case 
          when v.class == Hash then return hash_to_hcl(v)
          when v.class == Array then return array_to_hcl(v)
          when v.class == String then return "#{34.chr}#{v}#{34.chr}"
          else return "#{34.chr}#{v.to_s}#{34.chr}"
        end
      end
      
      def hash_to_hcl(h)
        ret = []
        h.keys.each do |k|
          ret << "#{k.to_s} = #{value_to_hcl(h[k])}" if value_to_hcl(h[k]).to_s != ''
        end
        return "{#{ret.join(',')}}"
      end
      
      def array_to_hcl(a)
        %{[#{a.map { |i| value_to_hcl(i).to_s }.join(',')}]}
      end
    end

    # handles the creation of glue tables by way of a cf stack.
    class AWSGlueTableModule < BaseTerraform
      attr_accessor :region
      attr_accessor :source

      attr_accessor :database_name
      attr_accessor :table_name
      attr_accessor :columns
      attr_accessor :location
      attr_accessor :input_format
      attr_accessor :output_format
      attr_accessor :compressed
      attr_accessor :number_of_buckets
      attr_accessor :ser_de_info_name
      attr_accessor :ser_de_info_serialization_library
      attr_accessor :bucket_columns
      attr_accessor :sort_columns
      attr_accessor :skewed_column_names
      attr_accessor :skewed_column_value_location_maps
      attr_accessor :skewed_column_values
      attr_accessor :stored_as_sub_directories
      attr_accessor :partition_keys
      attr_accessor :classification
      attr_accessor :convergdb_full_relation_name
      attr_accessor :convergdb_dsd
      attr_accessor :convergdb_storage_bucket
      attr_accessor :convergdb_state_bucket
      attr_accessor :convergdb_storage_format
      attr_accessor :convergdb_etl_job_name
      attr_accessor :convergdb_deployment_id
      
      # @param [Hash] params
      def initialize(params)
        @resource_id = to_underscore(params[:resource_id])
        @source = ConvergDB::TERRAFORM_MODULES[:aws_glue_table]
        @region = params[:region]
        # there are many so let's automate this...
        params[:structure].keys.each do |k|
          self.send("#{k}=", params[:structure][k])
        end
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
          resource_type: :aws_glue_table_module,
          structure: {
            module: {
              @resource_id => {
                source: @source,
                # region: @region,
                # deployment_id: %(${var.deployment_id}),

                database_name: @database_name,
                table_name: @table_name,
                columns: @columns,
                location: @location,
                input_format: @input_format,
                output_format: @output_format,
                compressed: @compressed,
                number_of_buckets: @number_of_buckets,
                ser_de_info_name: @ser_de_info_name,
                ser_de_info_serialization_library: @ser_de_info_serialization_library,
                bucket_columns: @bucket_columns,
                sort_columns: @sort_columns,
                skewed_column_names: @skewed_column_names,
                skewed_column_value_location_maps: @skewed_column_value_location_maps,
                skewed_column_values: @skewed_column_values,
                stored_as_sub_directories: @stored_as_sub_directories,
                partition_keys: @partition_keys,
                classification: @classification,
                convergdb_full_relation_name: @convergdb_full_relation_name,
                convergdb_dsd: @convergdb_dsd,
                convergdb_storage_bucket: @convergdb_storage_bucket,
                convergdb_state_bucket: @convergdb_state_bucket,
                convergdb_storage_format: @convergdb_storage_format ,
                convergdb_etl_job_name: @convergdb_etl_job_name,
                convergdb_deployment_id: @convergdb_deployment_id
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
      attr_accessor :database_name

      # @param [Hash] params
      def initialize(params)
        @resource_id = to_underscore(params[:resource_id])
        @region = params[:region]
        @source = ConvergDB::TERRAFORM_MODULES[:aws_glue_database]
        @database_name = params[:structure][:database_name]
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
                # region: '${var.region}',
                database_name: @database_name,
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
        @source = ConvergDB::TERRAFORM_MODULES[:aws_glue_etl_job]
        @job_name = params[:job_name]
        @local_script = params[:local_script]
        @local_pyspark_library = params[:local_pyspark_library]
        @script_bucket = params[:script_bucket]
        @script_key = params[:script_key]
        @pyspark_library_key = params[:pyspark_library_key]
        @schedule = params[:schedule]
        @stack_name = params[:stack_name]
        @service_role = params[:service_role]
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
                sns_topic: "${aws_sns_topic.convergdb-notifications.arn}",
                etl_lock_table: "${var.etl_lock_table}"
              }
            }
          }
        }
      end
    end

    # creates a fargate based etl job
    class AWSFargateETLJobModule < BaseTerraform
      attr_accessor :region
      attr_accessor :etl_job_name
      attr_accessor :etl_job_schedule
      attr_accessor :local_script
      attr_accessor :local_pyspark_library
      attr_accessor :script_bucket
      attr_accessor :script_key
      attr_accessor :pyspark_library_key
      attr_accessor :lambda_trigger_key
      attr_accessor :docker_image
      attr_accessor :docker_image_digest

      # @param [Hash] params
      def initialize(params)
        @resource_id = params[:resource_id]
        @region = params[:region]
        @etl_job_name = params[:etl_job_name]
        @etl_job_schedule = params[:etl_job_schedule]
        @local_script = params[:local_script]
        @local_pyspark_library = params[:local_pyspark_library]
        @script_bucket = params[:script_bucket]
        @script_key = params[:script_key]
        @pyspark_library_key = params[:pyspark_library_key]
        @lambda_trigger_key = params[:lambda_trigger_key]
        @docker_image = params[:docker_image]
        @docker_image_digest = params[:docker_image_digest]
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
          resource_type: :aws_fargate_etl_job_module,
          structure: {
            module: {
              @resource_id => {
                source: ConvergDB::TERRAFORM_MODULES[:aws_fargate_etl_job],
                region: @region,
                deployment_id: '${var.deployment_id}',
                etl_job_name: @etl_job_name,
                etl_job_schedule: @etl_job_schedule,
                local_script: @local_script,
                local_pyspark_library: @local_pyspark_library,
                script_bucket: @script_bucket,
                script_key: @script_key,
                pyspark_library_key: @pyspark_library_key,
                lambda_trigger_key: @lambda_trigger_key,
                admin_bucket: "${var.admin_bucket}",
                data_bucket: "${var.data_bucket}",
                cloudwatch_namespace: 'convergdb/${var.deployment_id}',
                sns_topic: '${aws_sns_topic.convergdb-notifications.arn}',
                ecs_subnet: '${var.fargate_subnet}',
                ecs_cluster: '${var.fargate_cluster}', 
                ecs_log_group: '${var.ecs_log_group}',
                docker_image: "#{@docker_image}@#{@docker_image_digest}",
                execution_task_role: '${var.ecs_execution_role}',
                etl_lock_table: "${var.etl_lock_table}"
              }
            }
          }
        }
      end
    end
    
    # creates a streaming inventory system for source s3 buckets
    class StreamingInventoryModule < BaseTerraform
      attr_accessor :region
      attr_accessor :source
      attr_accessor :source_bucket
      attr_accessor :destination_bucket
      attr_accessor :destination_prefix
      attr_accessor :firehose_stream_name
      attr_accessor :lambda_name

      # @param [Hash] params
      def initialize(params)
        @resource_id = params[:resource_id]
        @region = params[:region] ? params[:region] : "${var.region}"
        @source = ConvergDB::TERRAFORM_MODULES[:aws_s3_streaming_inventory]
        @source_bucket = params[:storage_bucket].split('/')[0]
        @firehose_stream_name = inventory_stream_name(@source_bucket)
        @destination_bucket = params[
          :streaming_inventory_output_bucket
        ].split('/')[0]
        @destination_prefix = params[
          :streaming_inventory_output_bucket
        ].split('/', 2)[1]
        @lambda_name = lambda_function_name(@source_bucket)
      end
      
      # creates a name for the inventory stream
      # @param [String] storage_bucket
      # @return [String]
      def inventory_stream_name(storage_bucket)
        b = Digest::MD5.new.hexdigest(
          storage_bucket.split('/')[0]
        ) # rubocop
        "convergdb-${var.deployment_id}-#{b}"
      end

      # @return [Hash]
      def validation_regex
        {
          resource_id: { regex: /.*/, mandatory: true }
        }
      end

      # @param [String] source_bucket
      # @return [String] unique name for lambda
      def lambda_function_name(source_bucket)
        bucket_hex = Digest::MD5.new.hexdigest(source_bucket)
        "convergdb-${var.deployment_id}-#{bucket_hex}"
      end

      # @return [Hash]
      def structure
        {
          resource_id: @resource_id,
          resource_type: :streaming_inventory_module,
          structure: {
            module: {
              @resource_id => {
                source: @source,
                region: @region,
                firehose_stream_name: @firehose_stream_name,
                source_bucket: @source_bucket,
                destination_bucket: @destination_bucket,
                destination_prefix: @destination_prefix,
                lambda_name: @lambda_name
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

      # idempotently appends a AWSGlueTableModule to the resources array.
      # after creation... the table is appended to the stack.
      # @param [Hash] params
      def aws_glue_table_module!(params)
        unless resource_id_exists?(params[:resource_id])
          c = AWSGlueTableModule.new(params)
          @resources << c
        end
      end

      # idempotently appends a AWSGlueDatabaseModule to the resources array.
      # after creation... the database is appended to the stack.
      # @param [Hash] params
      def aws_glue_database_module!(params)
        unless resource_id_exists?(params[:resource_id])
          c = AWSGlueDatabaseModule.new(params)
          @resources << c
        end
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

      # appends a AWSFargateETLJobModule to the resources array.
      # will not append an object if the resource_id already exists
      # @param [Hash] params
      def aws_fargate_etl_job_module!(params)
        unless resource_id_exists?(to_underscore(params[:resource_id]))
          g = AWSFargateETLJobModule.new(params)
          @resources << g
        end
      end
      
      # Idempotently appends a StreamingInventoryModule to the resources array.
      # @param [Hash] params
      def streaming_inventory_module!(params)
        unless resource_id_exists?(params[:resource_id])
          g = StreamingInventoryModule.new(params)
          @resources << g
        end
      end
    end
  end
end
