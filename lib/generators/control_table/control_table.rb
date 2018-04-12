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

require_relative '../athena/athena.rb'

module ConvergDB
  # generators to create deployable artifacts
  module Generators
    # used to generate SQL files for athena deployment
    class AWSAthenaControlTableGenerator < ConvergDB::Generators::AWSAthena
      # generates the artifacts necessary to deploy tables in glue
      # catalog... for use in athena.
      def generate!
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

      def storage_format
        'json'
      end

      # returns a cf stack name that is appended with a 2 digit hex value
      # derived from the md5 of the input string. this keeps the table
      # in the same cf stack, preventing unwanted rebuilds. this functionality
      # will go away once terraform supports glue table resources directly.
      # @param [Hash] input
      # @return [String]
      def aws_glue_table_module_resource_id(input)
        i = "#{input}_control" # append to input string for better bucketing
        "relations-#{aws_glue_table_module_resource_id_bucket(i)}"
      end

      def control_table_attributes
        [
          {
            name: 'convergdb_batch_id',
            data_type: 'varchar(64)',
            expression: ''
          },
          {
            name: 'batch_start_time',
            data_type: 'timestamp',
            expression: ''
          },
          {
            name: 'batch_end_time',
            data_type: 'timestamp',
            expression: ''
          },
          {
            name: 'source_type',
            data_type: 'varchar(64)',
            expression: ''
          },
          {
            name: 'source_format',
            data_type: 'varchar(64)',
            expression: ''
          },
          {
            name: 'source_relation',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'source_bucket',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'source_key',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'load_type',
            data_type: 'varchar(64)',
            expression: ''
          },
          {
            name: 'status',
            data_type: 'varchar(64)',
            expression: ''
          }
        ]
      end

      # @return [String]
      def athena_database_tf_module_name
        'convergdb_athena_databases_stack'
      end

      # extracts "table name" from a qualified relation name
      # @param [String] relation_name
      # @return [String]
      def table_name(relation_name)
        @terraform_builder.to_underscore(relation_name)
      end

      # s3 url formattted storage location for use in table definition
      # @param [Hash] structure
      # @return [String]
      def s3_storage_location(structure)
        "s3://#{structure[:state_bucket]}/${deployment_id}/state/#{structure[:full_relation_name]}/control"
      end

      # @param [Hash] structure
      # @return [Hash] representation of tblproperties
      def tblproperties(structure)
        {
          # required by glue
          classification: storage_format,

          # required by athena
          EXTERNAL: 'TRUE',

          # required by convergdb
          convergdb_full_relation_name: structure[:full_relation_name],
          convergdb_dsd: structure[:dsd],
          convergdb_storage_bucket: structure[:state_bucket],
          convergdb_storage_format: structure[:storage_format],
          convergdb_etl_job_name: structure[:etl_job_name] || '',
          convergdb_deployment_id: %(${deployment_id}),
          convergdb_database_cf_id:
            %(${database_stack_id})
        }
      end

      # returns a name to be used as db/schema/etc. this version resolves the
      # deployment_id in the tf template file for cf stack.
      # @return [String]
      def athena_database_name
        'convergdb_control_${deployment_id}'
      end

      # returns a name to be used as db/schema/etc. this version resolves the
      # deployment_id in the deployment.tf.json file.
      # @return [String]
      def database_name
        'convergdb_control_${var.deployment_id}'
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
          %(convergdbControlTable#{
            Digest::SHA256.hexdigest(structure[:full_relation_name])
          }) => {
            'Type' => 'AWS::Glue::Table',
            'Properties' => {
              # terraform will populate this for you based upon the aws account
              'CatalogId' => '${aws_account_id}',
              'DatabaseName' => athena_database_name,
              'TableInput' => {
                'StorageDescriptor' => {
                  'OutputFormat' => output_format(storage_format),
                  'SortColumns' => [],
                  'InputFormat' => input_format(storage_format),
                  'SerdeInfo' => {
                    'SerializationLibrary' => serialization_library(
                      storage_format
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
                  'Columns' => control_table_attributes.map do |a|
                    {
                      'Name' => a[:name],
                      'Type' => athena_data_type(a[:data_type]),
                      'Comment' => a[:expression] || ''
                    }
                  end,
                  'Compressed' => false
                },
                'PartitionKeys' => [],
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
      def cfn_database_resource(*)
        {
          # hashed from the :full_relation_name to avoid conflicts
          %(convergdbDatabase#{
            Digest::SHA256.hexdigest(athena_database_name)}) =>
          {
            'Type' => 'AWS::Glue::Database',
            'Properties' => {
              # terraform will populate this for you based upon the aws account
              'CatalogId' => '${data.aws_caller_identity.current.account_id}',
              'DatabaseInput' => {
                'Name' => database_name,
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
