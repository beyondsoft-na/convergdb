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
require 'hashdiff'
require 'rainbow'
require 'pp'
require 'digest'

require_relative '../generate.rb'

module ConvergDB
  # generators to create deployable artifacts
  module Generators
    # used to generate SQL files for athena deployment
    class AWSAthena < ConvergDB::Generators::BaseGenerator
      # set by the master generator. this represents the current state
      # of this relation based upon what is currently in AWS.
      attr_accessor :current_state

      # outputs a friendly message about what is happening with this relation.
      # details of the message are inline with the code.
      # @param [Hash] structure
      # @param [Hash] current_state
      # @param [HashDiff] diff
      # @return [Array] combination of string and Rainbow objects
      def message(structure, current_state, diff)
        # we are going to return an array of objects all of which are
        # printable strings. the rainbow object adds color codes for terminal
        # output.
        m = []

        # relation name in bright green
        m << Rainbow(
          "athena: #{structure[:full_relation_name]}"
        ).bright.green + "\n"

        # if there is no current_state we are treating this
        # as a new relation... and notifying us about it here
        m << Rainbow('  new relation').green + "\n" if current_state == {}

        # creates a diff between the current state and current configuration.
        # the diff is in an Array<Array> with a form + key new_value.
        # + - ~ all indicate the details.
        hd = diff

        # check to see if the diff is empty therefore no changes to
        # make in the deployment. empty diff only indicates no change
        # if the current state is not empty.
        if hd.length.zero?
          m << '  no change' + "\n" if current_state != {}
        end

        # iterate through each diff record.
        # append a colorized version to the message array.
        hd.each do |d|
          colored = diff_item_coloring(d)
          m << colored if colored
        end

        # final newline
        m << "\n"
      rescue => e
        puts 'message error'
        puts e.message
        e.backtrace { |b| puts b }
        raise e
      end

      # proceses a single element from a HashDiff output and returns
      # a rainbow colored string if it is a + or -... otherwise returns
      # nothing
      # @param [Array] diff
      # @return [Rainbow]
      def diff_item_coloring(diff)
        case diff[0]
        when '-' then
          Rainbow("  #{diff[0]} #{diff[1]} = #{diff[2]}").red + "\n"
        when '+' then
          Rainbow("  #{diff[0]} #{diff[1]} = #{diff[2]}").green + "\n"
        when '~' then
          "  #{diff[0]} #{diff[1]} from '#{diff[2]}' to '#{diff[3]}'" + "\n"
        end
      end

      # generates the artifacts necessary to deploy tables in glue
      # catalog... for use in athena.
      def generate!
        # outputs a text summary based upon a diff between the current state
        # and the current configuration.
        puts(
          message(
            @structure,
            @current_state,
            HashDiff.diff(@current_state, comparable(@structure))
          ).join('')
        )

        # insure that the module is in place
        create_static_artifacts!(@structure[:working_path])

        @terraform_builder.aws_glue_database_module!(
          aws_glue_database_module_params(@structure)
        )

        # idempotent create and append relation to config
        @terraform_builder.aws_glue_table_module!(
          aws_glue_table_module_params(
            @structure,
            @terraform_builder
          )
        )
      end

      # parameters to be passed to the aws_glue_database_module
      # method of a terraform builder.
      # @param [Hash] structure
      # @return [Hash]
      def aws_glue_database_module_params(structure)
        {
          resource_id: 'convergdb_athena_databases_stack',
          region: '${var.region}',
          athena_database_tf_module_name: athena_database_tf_module_name,
          structure: cfn_database_resource(structure)
        }
      end

      # @param [String] input string to use in md5
      # @param [Fixnum] base mask for bits to use
      # @return [String]
      def aws_glue_table_module_resource_id_bucket(input, base = 127)
        md5 = Digest::MD5.new
        # mask the least sig bytes based upon the base.. which
        # should be something like 127
        return (md5.hexdigest(input)[0..1].hex & base).to_s(16)
      end

      # returns a cf stack name that is appended with a 2 digit hex value
      # derived from the md5 of the input string. this keeps the table
      # in the same cf stack, preventing unwanted rebuilds. this functionality
      # will go away once terraform supports glue table resources directly.
      # @param [Hash] input
      # @return [String]
      def aws_glue_table_module_resource_id(input)
        return "relations-#{aws_glue_table_module_resource_id_bucket(input)}"
      end

      # parameters to be passed to the aws_glue_table_module
      # method of a terraform builder.
      # @param [Hash] structure
      # @param [TerraformBuilder] terraform_builder
      # @return [Hash]
      def aws_glue_table_module_params(structure, terraform_builder)
        {
          resource_id: aws_glue_table_module_resource_id(
            structure[:full_relation_name]
          ),
          region: '${var.region}',
          athena_relation_module_name: terraform_builder.to_underscore(
            structure[:full_relation_name]
          ),
          structure: cfn_table_resource(structure),
          working_path: structure[:working_path]
        }
      end

      # @return [String]
      def athena_database_tf_module_name
        'convergdb_athena_databases_stack'
      end

      # creates necessary files and folders for use with terraform
      # @param [String] working_path working path for this run
      def create_static_artifacts!(working_path)
        unless Dir.exist?("#{working_path}/terraform/modules")
          FileUtils.mkdir_p("#{working_path}/terraform/modules")
        end

        unless Dir.exist?("#{working_path}/terraform/cloudformation")
          FileUtils.mkdir_p("#{working_path}/terraform/cloudformation")
        end

        FileUtils.cp_r(
          "#{File.dirname(__FILE__)}/modules/",
          "#{working_path}/terraform/"
        )
      end

      # extracts "table name" from a qualified relation name
      # @param [String] relation_name
      # @return [String]
      def table_name(relation_name)
        relation_name.split('.').reverse[0]
      end

      # s3 url formattted storage location for use in table definition
      # @param [Hash] structure
      # @return [String]
      def s3_storage_location(structure)
        "s3://#{structure[:storage_bucket]}/"
      end

      # converts to athena supported data types
      # @param [String] data_type original data type
      # @return [String] athena data type
      def athena_data_type(data_type)
        if data_type =~ /char.*\(\d+\)/i then 'string'
        elsif data_type =~ /tinyint/i then 'tinyint'
        elsif data_type =~ /smallint/i then 'smallint'
        elsif data_type =~ /^int|^integer/i then 'int'
        elsif data_type =~ /bigint/i then 'bigint'
        elsif data_type =~ /boolean/i then 'boolean'
        elsif data_type =~ /float|double/i then 'double'
        elsif data_type =~ /datetime|timestamp/i then 'timestamp'
        elsif data_type =~ /date/i then 'date'
        elsif data_type =~ /(numeric|decimal)\(\d+,\d+\)/i
          then data_type.gsub(/(numeric|decimal)/i, 'decimal')
          # then 'double'
        else raise 'data type error'
        end
      end

      # @param [Hash] structure
      # @return [Hash] representation of tblproperties
      def tblproperties(structure)
        {
          # required by glue
          classification: structure[:storage_format],

          # required by athena
          EXTERNAL: 'TRUE',

          # required by convergdb
          convergdb_full_relation_name: structure[:full_relation_name],
          convergdb_dsd: structure[:dsd],
          convergdb_storage_bucket: structure[:storage_bucket],
          convergdb_state_bucket: structure[:state_bucket] || '',
          convergdb_storage_format: structure[:storage_format],
          convergdb_etl_job_name: structure[:etl_job_name] || '',
          convergdb_deployment_id: %(${deployment_id}),
          convergdb_database_cf_id:
            %(${database_stack_id})
        }
      end

      # returns a name to be used as db/schema/etc. period is replaced by a
      # double underscore so as to allow normal underscore naming convention.
      # @param [Hash] structure
      # @return [String]
      def athena_database_name(structure)
        structure[:full_relation_name].split('.')[0..2].join('__')
      end

      # extracts important elements from the structure in order to create a
      # hash that can be compared to what exists in AWS (if anything).
      # @param [Hash] structure
      # @return [Hash]
      def comparable(structure)
        {
          full_relation_name: structure[:full_relation_name] || '',
          dsd: structure[:dsd] || '',
          storage_bucket: structure[:storage_bucket] || '',
          state_bucket: structure[:state_bucket] || '',
          storage_format: structure[:storage_format] || '',
          etl_job_name: structure[:etl_job_name] || '',
          attributes: structure[:attributes].select{ |x| !structure[:partitions].include?(x[:name]) }.map do |a|
            {
              name: a[:name],
              data_type: athena_data_type(a[:data_type]),
              expression: Digest::MD5.new.hexdigest(a[:expression].to_s) || ''
            }
          end
        }
      end

      # hadoop output format for use in table definition.
      # @param [String] type either json or parquet
      # @return [String] output class appropriate for storage type
      def output_format(type)
        case type
        when 'json' then
          'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        when 'parquet' then
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        end
      end

      # hadoop input format for use in table definition.
      # @param [String] type either json or parquet
      # @return [String] input class appropriate for storage type
      def input_format(type)
        case type
        when 'json' then
          'org.apache.hadoop.mapred.TextInputFormat'
        when 'parquet' then
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        end
      end

      # hadoop serialization library for use in table definition.
      # @param [String] type either json or parquet
      # @return [String] serialization class appropriate for storage type
      def serialization_library(type)
        case type
        when 'json' then
          'org.apache.hive.hcatalog.data.JsonSerDe'
        when 'parquet' then
          'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        end
      end

      # extracts all table attributes which are used in partitioning.
      # @param [Hash] structure
      # @return [Array<Hash>] partition attributes
      def partition_attributes(structure)
        structure[:attributes].select do |a|
          structure[:partitions].include?(a[:name])
        end
      end

      # extracts all table attributes which are not used in partitioning.
      # @param [Hash] structure
      # @return [Array<Hash>] non-partition attributes
      def non_partition_attributes(structure)
        structure[:attributes].select do |a|
          !structure[:partitions].include?(a[:name])
        end
      end

      # this bad boy is appended to the Resources section of the Cloudformation
      # stack used to deploy all of the tables in the athena/glue catalog. See
      # the AWS documentation for the Glue API for more information on the
      # structure and meaning of these parameters.
      # @param [Hash] structure
      # @return [Hash]
      def cfn_table_resource(structure)
        {
          # hashed from the :full_relation_name to avoid conflicts
          %(convergdbTable#{
            Digest::SHA256.hexdigest(
              structure[:full_relation_name]
            )}) => {
            'Type' => 'AWS::Glue::Table',
            'Properties' => {
              # terraform will populate this for you based upon the aws account
              'CatalogId' => '${aws_account_id}',
              'DatabaseName' => athena_database_name(structure),
              'TableInput' => {
                'StorageDescriptor' => {
                  'OutputFormat' => output_format(structure[:storage_format]),
                  'SortColumns' => [],
                  'InputFormat' => input_format(structure[:storage_format]),
                  'SerdeInfo' => {
                    'SerializationLibrary' => serialization_library(
                      structure[:storage_format]
                    ),
                    'Parameters' => {
                      'serialization.format' => '1'
                    }
                  },
                  'BucketColumns' => [],
                  'Parameters' => {},
                  'SkewedInfo' => {
                    'SkewedColumnNames' => [],
                    'SkewedColumnValueLocationMaps' => {},
                    'SkewedColumnValues' => []
                  },
                  'Location' => s3_storage_location(structure),
                  'NumberOfBuckets' => -1,
                  'StoredAsSubDirectories' => false,
                  'Columns' => non_partition_attributes(structure).map do |a|
                    {
                      'Name' => a[:name],
                      'Type' => athena_data_type(a[:data_type]),
                      'Comment' => Digest::MD5.new.hexdigest(a[:expression]) || ''
                    }
                  end,
                  'Compressed' => false
                },
                'PartitionKeys' => partition_attributes(structure).map do |a|
                    {
                      'Name' => a[:name],
                      'Type' => athena_data_type(a[:data_type]),
                      'Comment' => Digest::MD5.new.hexdigest(a[:expression].to_s) || ''
                    }
                  end,
                'Name' => table_name(structure[:full_relation_name]),
                'Parameters' => tblproperties(structure),
                'TableType' => 'EXTERNAL_TABLE',
                'Owner' => 'hadoop',
                'Retention' => 0
              }
            }
          }
        }
      end

      # creates a database resource for use inside a cloudformation template.
      # see the AWS documentation for the Glue API for more info.
      # @param [Hash] structure
      # @return [Hash]
      def cfn_database_resource(structure)
        {
          # hashed from the :full_relation_name to avoid conflicts
          %(convergdbDatabase#{
            Digest::SHA256.hexdigest(
              athena_database_name(structure)
            )}) =>
          {
            'Type' => 'AWS::Glue::Database',
            'Properties' => {
              # terraform will populate this for you based upon the aws account
              'CatalogId' => '${data.aws_caller_identity.current.account_id}',
              'DatabaseInput' => {
                'Name' => athena_database_name(structure),
                'Parameters' => {
                  'convergdb_deployment_id' =>
                    '${var.deployment_id}'
                }
              }
            }
          }
        }
      end
    end
  end
end
