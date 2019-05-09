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
require 'fileutils'
require_relative '../generate.rb'
require_relative '../../exceptions.rb'

require 'pp'

module ConvergDB
  # generators to create deployable artifacts
  module Generators
    # used to generate SQL files for athena deployment
    class AWSGlue < BaseGenerator
      include ConvergDB::ErrorHandling
      
      # post initialization tasks
      def post_initialize
        if @structure[:etl_technology] == 'aws_glue'
          apply_cast_type!(@structure)
          @structure[:deployment_id] = "${deployment_id}"
          @structure[:region] = "${region}"
          @structure[:sns_topic] = "${sns_topic}"
          @structure[:cloudwatch_namespace] = "${cloudwatch_namespace}"
          
          # only create a diff if clients are passed to the object
          unless @aws_clients.nil?
            diff = diff_with_aws(
              @aws_clients,
              @structure
            )
            output_diff(
              "AWS Glue ETL job #{@structure[:etl_job_name]}",
              diff.map { |i| diff_item_coloring(i) }
            )
          end
        end
      end

      #! COMPARABLE
      # @param [Aws::Glue::Client] client
      # @param [String] job_name
      # @return [Hash]
      def glue_job_by_name(client, job_name)
        ignore_error do
          client.get_job(
            {
              job_name: job_name
            }
          ).to_h
        end
      end

      # @param [Aws::Glue::Client] client
      # @param [String] job_name
      # @return [Hash]
      def glue_trigger_by_job_name(client, job_name)
        ignore_error do
          client.get_trigger(
            {
              name: "convergdb-#{job_name}"
            }
          ).to_h
        end
      end
      
      # combines both of the hashes returned form AWS into a single hash.
      # @param [Hash] get_job_response
      # @param [Hash] get_trigger_response
      # @return [Hash]
      def comparable_glue_structure(get_job_response, get_trigger_response)
        if get_job_response || get_trigger_response
          {
            dpu: get_job_response[:job][:allocated_capacity],
            etl_job_schedule: get_trigger_response[:trigger][:schedule]
          }
        else
          {}
        end
      end
      
      # @param [Hash] structure
      # @return [Hash]
      def comparable(structure)
        {
          dpu: structure[:etl_job_dpu],
          etl_job_schedule: structure[:etl_job_schedule]
        }
      end

      # @param [Hash] aws_clients
      # @param [Hash] structure
      # @return [Array]
      def diff_with_aws(aws_clients, structure)
        HashDiff.diff(
          comparable_glue_structure(
            glue_job_by_name(
              aws_clients[:aws_glue],
              structure[:etl_job_name]
            ),
            glue_trigger_by_job_name(
              aws_clients[:aws_glue],
              structure[:etl_job_name]
            )
          ),
          comparable(structure)
        )
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
        if @structure[:etl_technology] == 'aws_glue'
          create_static_artifacts!(@structure)

          create_etl_script_if_not_exists!(
            etl_job_script_path(@structure)
          )

          append_to_job_script!(
            etl_job_script_path(@structure),
             pyspark_source_to_target(@structure)
          )

          @terraform_builder.aws_glue_etl_job_module!(
            glue_etl_job_module_params(@structure)
          )
        end
      end

      # creates a hash to be used as input to the aws_glue_etl_job_module!
      # method of the terraform builder.
      # @param [Hash] structure
      # @return [Hash]
      def glue_etl_job_module_params(structure)
        {
          resource_id: "aws_glue_#{structure[:etl_job_name]}",
          region: '${var.region}',
          job_name: structure[:etl_job_name],
          local_script: etl_job_script_relative_path(structure),
          local_pyspark_library: pyspark_library_relative_path(structure),
          script_bucket: structure[:script_bucket],
          script_key: pyspark_script_key(structure),
          pyspark_library_key: pyspark_library_key(structure),
          schedule: structure[:etl_job_schedule],
          dpu: structure[:etl_job_dpu],
          stack_name: @terraform_builder.to_dash(
            "convergdb-glue-#{structure[:etl_job_name]}"
          ) + '-${var.deployment_id}',
          service_role: structure[:service_role]
        }
      end

      # creates the modules, directories, and pyspark library
      # necessary for the terraform deployment. this method should only
      # be run once.
      # @param [Hash] structure
      def create_static_artifacts!(structure)
        FileUtils.mkdir_p("#{structure[:working_path]}/terraform")

#        FileUtils.cp_r(
#          "#{File.dirname(__FILE__)}/modules/",
#          "#{structure[:working_path]}/terraform/",
#        )

        FileUtils.mkdir_p("#{structure[:working_path]}/terraform/aws_glue")

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
      def tf_glue_path(structure)
        "#{structure[:working_path]}/terraform/aws_glue"
      end

      # @return [String] path used inside terraform configuration
      def tf_glue_relative_path
        "./aws_glue"
      end

      # @param [Hash] structure
      # @return [String] path for this etl job pyspark script
      def etl_job_script_path(structure)
        "#{tf_glue_path(structure)}/#{structure[:etl_job_name]}.py"
      end

      # @param [Hash] structure
      # @return [String] path for this etl job pyspark script
      def etl_job_script_relative_path(structure)
        "#{tf_glue_relative_path}/#{structure[:etl_job_name]}.py"
      end

      # @param [Hash] structure
      # @return [String] path to pyspark library
      def pyspark_library_path(structure)
        "#{tf_glue_path(structure)}/convergdb.zip"
      end

      # @param [Hash] structure
      # @return [String] path to pyspark library
      def pyspark_library_relative_path(structure)
        "#{tf_glue_relative_path}/convergdb.zip"
      end

      # @param [String] script_path
      def create_etl_script_if_not_exists!(script_path)
        unless File.exist?(script_path)
          File.open(script_path, 'w') do |f|
            f.puts("import os")
            f.puts("import sys")
            f.puts("from awsglue.utils import getResolvedOptions")
            f.puts("args = getResolvedOptions(sys.argv, ['JOB_NAME', 'convergdb_lock_table','aws_region'])")
            f.puts("os.environ['AWS_GLUE_REGION'] = args['aws_region']")
            f.puts("os.environ['LOCK_TABLE'] = args['convergdb_lock_table']")
            f.puts("os.environ['LOCK_ID']    = args['JOB_NAME']")
            f.puts('import convergdb')
            f.puts('from convergdb.glue_header import *')
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
        %{#{deployment_id}/scripts/aws_glue/#{structure[:etl_job_name]}}
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
    end
  end
end
