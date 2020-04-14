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
require 'fileutils'
require_relative '../generate.rb'

module ConvergDB
  # generators to create deployable artifacts
  module Generators
    # used to generate SQL files for athena deployment
    class AWSFargate < BaseGenerator
      include ConvergDB::ErrorHandling
      
      # post initialization tasks
      def post_initialize
        if @structure[:etl_technology] == 'aws_fargate'
          apply_cast_type!(@structure)
          @structure[:deployment_id] = "${deployment_id}"
          @structure[:region] = "${region}"
          @structure[:sns_topic] = "${sns_topic}"
          @structure[:cloudwatch_namespace] = "${cloudwatch_namespace}"
          unless @aws_clients.nil?
            diff = diff_with_aws(
              @aws_clients,
              @structure
            )
            output_diff(
              "AWS Fargate ETL job #{@structure[:etl_job_name]}",
              diff.map {|i| diff_item_coloring(i) }
            )
          end
        end
      end

      # provides a mask for the structure that is provided to the template_file
      # used for the ETL script.
      # @params[Hash] structure
      # @return [Hash] structure with ephemeral overrides
      def script_structure(structure)
        t = structure.clone
        if t[:source_structure][:streaming_inventory_output_bucket]
          t[:source_structure][:streaming_inventory_output_bucket] = var_substitution(
            t[:source_structure][:streaming_inventory_output_bucket]
          )
        end

        if t[:script_bucket]
          t[:script_bucket] = var_substitution(
            t[:script_bucket]
          )
        end
        t
      end

      def var_substitution(val)
        val.gsub(
          'var.admin_bucket',
          'admin_bucket'
        ).gsub(
          'var.deployment_id',
          'deployment_id'
        )
      end

      # generates the artifacts but only if glue is required
      def generate!
        if @structure[:etl_technology] == 'aws_fargate'
          create_static_artifacts!(@structure)

          create_etl_script_if_not_exists!(
            etl_job_script_path(@structure)
          )

          append_to_job_script!(
            etl_job_script_path(@structure),
             pyspark_source_to_target(@structure)
          )

          @terraform_builder.aws_fargate_etl_job_module!(
            fargate_etl_job_module_params(@structure)
          )
        end
      end
      
      #! DIFF METHODS
      # @param [Hash] structure
      # @return [Hash] structure
      def comparable(structure)
        {
          etl_job_schedule: structure[:etl_job_schedule]
        }
      end
      
      # @param [Aws::CloudWatchEvents::Client] client
      # @return [Hash]
      def cloudwatch_event_rules(client)
        ignore_error do
          client.list_rules.to_h
        end
      end
      
      # @param [Hash] rules from list_rules API call
      # @param [String] etl_job_name for this ETL job
      # @return [Hash]
      def etl_schedule_for_this_job(rules, etl_job_name)
        reg_job_name = etl_job_name.gsub(/\_/, '\_').gsub(/\-/, '\-')
        regex = /^convergdb\-\w{16}\-#{reg_job_name}\-trigger$/
        ret = rules[:rules].select { |r| r[:name].match(regex) }[0]
        if ret.nil?
          {}
        else
          {
            etl_job_schedule: ret[:schedule_expression]
          }
        end
      end
      
      # @param [Hash] aws_clients
      # @param [Hash] structure
      # @return [Array]
      def diff_with_aws(aws_clients, structure)
        HashDiff.diff(
          etl_schedule_for_this_job(
            cloudwatch_event_rules(
              aws_clients[:aws_cloudwatch_events]
            ),
            structure[:etl_job_name]
          ),
          comparable(structure)
        )
      end
      
      # creates a hash to be used as input to the aws_glue_etl_job_module!
      # method of the terraform builder.
      # @param [Hash] structure
      # @return [Hash]
      def fargate_etl_job_module_params(structure)
        {
          resource_id: "aws_fargate_#{structure[:etl_job_name]}",
          region: '${var.region}',
          etl_job_name: structure[:etl_job_name],
          etl_job_schedule: structure[:etl_job_schedule],
          local_script: etl_job_script_relative_path(structure),
          local_pyspark_library: pyspark_library_relative_path(structure),
          script_bucket: structure[:script_bucket],
          script_key: pyspark_script_key(structure),
          pyspark_library_key: pyspark_library_key(structure),
          lambda_trigger_key: pyspark_lambda_trigger_key(structure),
          docker_image: structure[:etl_docker_image],
          docker_image_digest: structure[:etl_docker_image_digest]
        }
      end

      # creates the modules, directories, and pyspark library
      # necessary for the terraform deployment. this method should only
      # be run once.
      # @param [Hash] structure
      def create_static_artifacts!(structure)
        FileUtils.mkdir_p("#{structure[:working_path]}/terraform/aws_fargate")

        lib_path = File.expand_path(
          "#{File.expand_path(File.dirname(__FILE__))}/../"
        )

        FileUtils.cp(
          "#{lib_path}/convergdb.zip",
          pyspark_library_path(structure),
        )
      end

      # @param [Hash] structure
      # @return [String] path used to store aws glue pyspark scripts
      def tf_fargate_path(structure)
        "#{structure[:working_path]}/terraform/aws_fargate"
      end

      # @return [String] path used in terraform configuration
      def tf_fargate_relative_path
        './aws_fargate'
      end
      
      # @param [Hash] structure
      # @return [String] path for this etl job pyspark script
      def etl_job_script_path(structure)
        "#{tf_fargate_path(structure)}/#{structure[:etl_job_name]}.py"
      end

      # @param [Hash] structure
      # @return [String] path for this etl job pyspark script
      def etl_job_script_relative_path(structure)
        "#{tf_fargate_relative_path}/#{structure[:etl_job_name]}.py"
      end
      
      # @param [Hash] structure
      # @return [String] path to pyspark library
      def pyspark_library_path(structure)
        "#{tf_fargate_path(structure)}/convergdb.zip"
      end

      # @param [Hash] structure
      # @return [String] path to pyspark library
      def pyspark_library_relative_path(structure)
        "#{tf_fargate_relative_path}/convergdb.zip"
      end
      
      # script uses local header and assumes /tmp as working dir
      # inside of the container.
      # @param [String] script_path
      def create_etl_script_if_not_exists!(script_path)
        unless File.exist?(script_path)
          File.open(script_path, 'w') do |f|
            f.puts("import sys")
            f.puts("sys.path.insert(0, '/tmp/convergdb.zip')")
            f.puts('import convergdb')
            f.puts('from convergdb.local_header import *')
            f.puts
          end
        end
      end

      # @param [String] script_path
      # @param [String] job_text
      def append_to_job_script!(script_path, job_text)
        File.open(script_path, 'a') do |f|
          f.puts(job_text)
          f.puts
        end
      end

      # generates invocation of athena_from_s3() function inside pyspark.
      # this can be used multiple times within the same document.
      # @param [Hash] structure dsd_ddd_ir
      def pyspark_source_to_target(structure)
        ret = []
        ret << 'convergdb.source_to_target('
        ret << '  sql_context(),'
        ret << '"""'
        ret << JSON.pretty_generate(script_structure(structure)).gsub('${var.','${')
        ret << '"""'
        ret << ')'
        ret.join("\n")
      end

      # converts the provided sql_type to a type usable
      # for casting inside a pyspark script.
      # @param [String] sql_type
      # @return [String]
      def pyspark_cast_type(sql_type)
        if sql_type =~ /char.*\(\d+\)/i then 'string'
        elsif sql_type =~ /tinyint/i then 'byte'
        elsif sql_type =~ /smallint/i then 'short'
        elsif sql_type =~ /^int|^integer/i then 'integer'
        elsif sql_type =~ /bigint/i then 'long'
        elsif sql_type =~ /boolean/i then 'boolean'
        elsif sql_type =~ /float/i then 'float'
        elsif sql_type =~ /double/i then 'double'
        elsif sql_type =~ /datetime|timestamp/i then 'timestamp'
        elsif sql_type =~ /date/i then 'date'
        elsif sql_type =~ /(numeric|decimal)\(\d+,\d+\)/i
          then sql_type.gsub(/numeric|decimal/i, 'decimal')
        else raise 'data type error'
        end
      end

      # adds :cast_type to the structure :attributes based upon
      # pyspark_cast_type method. both the source and target relation
      # attributes are mutated.
      # @param [Hash] structure
      def apply_cast_type!(structure)
        # target attributes
        structure[:attributes].each do |attribute|
          attribute[:cast_type] = pyspark_cast_type(attribute[:data_type])
        end

        # source attributes
        structure[:source_structure][:attributes].each do |attribute|
          attribute[:cast_type] = pyspark_cast_type(attribute[:data_type])
        end
      end

      # refers to the tf deployment id. this value is resolved in tf deployment
      # @return [String]
      def deployment_id
        '${var.deployment_id}'
      end

      # s3 "folder" location of the pyspark script for this relation
      # @param [Hash] structure
      # @return [String]
      def pyspark_s3_key_prefix(structure)
        %{#{deployment_id}/scripts/aws_fargate/#{structure[:etl_job_name]}}
      end

      # s3 "folder" location of the pyspark library
      # @param [Hash] structure
      # @return [String]
      def pyspark_library_key(structure)
        %{#{pyspark_s3_key_prefix(structure)}/#{File.basename(pyspark_library_path(structure))}}
      end

      # s3 key for the pyspark library
      # @param [Hash] structure
      # @return [String]
      def pyspark_script_key(structure)
        %{#{pyspark_s3_key_prefix(structure)}/#{structure[:etl_job_name]}.py}
      end

      # s3 key for the python lambda used to trigger the job
      # @param [Hash] structure
      # @return [String]
      def pyspark_lambda_trigger_key(structure)
        %{#{pyspark_s3_key_prefix(structure)}/#{structure[:etl_job_name]}_trigger_lambda.py}
      end
    end
  end
end
