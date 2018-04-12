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

module ConvergDB
  # generators to create deployable artifacts
  module Generators
    # used to generate SQL files for athena deployment
    class AWSGlue < BaseGenerator
      def post_initialize
        apply_cast_type!(@structure)
        @structure[:deployment_id] = "${deployment_id}"
        @structure[:region] = "${region}"
        @structure[:sns_topic] = "${sns_topic}"
        @structure[:cloudwatch_namespace] = "${cloudwatch_namespace}"
      end

      # generates the artifacts
      def generate!
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

      # creates a hash to be used as input to the aws_glue_etl_job_module!
      # method of the terraform builder.
      # @param [Hash] structure
      # @return [Hash]
      def glue_etl_job_module_params(structure)
        {
          resource_id: "aws_glue_#{structure[:etl_job_name]}",
          region: '${var.region}',
          job_name: structure[:etl_job_name],
          local_script: etl_job_script_path(structure),
          local_pyspark_library: pyspark_library_path(structure),
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

        FileUtils.cp_r(
          "#{File.dirname(__FILE__)}/modules/",
          "#{structure[:working_path]}/terraform/",
        )

        FileUtils.mkdir_p("#{structure[:working_path]}/terraform/aws_glue")

        FileUtils.cp(
          "#{File.dirname(__FILE__)}/convergdb_pyspark_library.py",
          pyspark_library_path(structure),
        )
      end

      # @param [Hash] structure
      # @return [String] path used to store aws glue pyspark scripts
      def tf_glue_path(structure)
        "#{structure[:working_path]}/terraform/aws_glue"
      end

      # @param [Hash] structure
      # @return [String] path for this etl job pyspark script
      def etl_job_script_path(structure)
        "#{tf_glue_path(structure)}/#{structure[:etl_job_name]}.py"
      end

      # @param [Hash] structure
      # @return [String] path to pyspark library
      def pyspark_library_path(structure)
        "#{tf_glue_path(structure)}/convergdb_pyspark_library.py"
      end

      # @param [String] script_path
      def create_etl_script_if_not_exists!(script_path)
        unless File.exist?(script_path)
          File.open(script_path, 'w') do |f|
            f.puts('import convergdb_pyspark_library')
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
        ret << 'convergdb_pyspark_library.source_to_target('
        ret << '"""'
        ret << JSON.pretty_generate(structure)
        ret << '""",'
        ret << pyspark_source_lambda_func(
          structure[:source_structure][:attributes]
        )
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

      # takes in a list of attributes from the structure, and returns a python
      # lambda function used to map the JSON attributes after they are parsed
      # to a python dictionary. the lambda makes use of chained .get() calls
      # to traverse the dict structure without raising errors for keys which
      # are not found.
      # @param [Array<Hash>] attributes
      # @return [String]
      def pyspark_source_lambda_func(attributes)
        # r represents the lines of the function body
        r = []
        # function header
        r << 'lambda doc : {'
        # add a mapping from doc parameter to each
        attributes.each_with_index do |a, i|
          # first... default the expression to the name if no expression
          # was provided.
          e = a[:expression] ? a[:expression] : a[:name]
          # array is used to store the .get chain
          ref = []
          # expression is dot notated json path (arrays not yet supported)
          indices = e.split('.')
          indices.each_with_index do |s, i|
            # default is None if last .get in chain.
            # otherwise {} is used as default in order to prevent errors
            default = (i == indices.length - 1) ? 'None' : '{}'
            # append this .get to the chain
            ref << %{.get("#{s}",#{default})}
          end
          # unless this is the last .get in chain add a comma
          comma = i == (attributes.length - 1) ? '' : ','
          # append the mapping for this field to the function body
          r << %{  "#{a[:name]}" : doc#{ref.join('')}#{comma}}
        end
        # closing brace
        r << '}'
        # return as multiline text
        r.join("\n")
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
