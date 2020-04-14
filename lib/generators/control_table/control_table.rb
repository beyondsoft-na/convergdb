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

      # parameters to be passed to the aws_glue_table_module
      # method of a terraform builder.
      # @param [Hash] structure
      # @param [TerraformBuilder] terraform_builder
      # @return [Hash]
      def aws_glue_table_module_params(structure, terraform_builder)
        {
          resource_id: "#{table_name(structure[:full_relation_name])}_control",
          region: '${var.region}',
          athena_relation_module_name: terraform_builder.to_underscore(
            structure[:full_relation_name]
          ),
          structure: table_parameters(structure),
          working_path: structure[:working_path]
        }
      end
      
      def storage_format
        'json'
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
        "s3://#{structure[:state_bucket]}/${var.deployment_id}/state/#{structure[:full_relation_name]}/control"
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
          convergdb_deployment_id: %(${var.deployment_id})
        }
      end

      # returns a name to be used as db/schema/etc. 
      # for use in tf.json file
      # @return [String]
      def athena_database_name(ignored)
        'convergdb_control_${var.deployment_id}'
      end

      def table_parameters(structure)
        {
          # database name uses module output to force
          database_name: "${module.#{@terraform_builder.database_module_name(athena_database_name(structure))}.database_name}",
          table_name: table_name(structure[:full_relation_name]),
          columns: control_table_attributes.map  { |a| terraform_column_attributes(a) },
          location: s3_storage_location(structure),
          input_format: input_format(storage_format),
          output_format: output_format(storage_format),
          compressed: false,
          number_of_buckets: -1,
          ser_de_info_name: storage_format,
          ser_de_info_serialization_library: serialization_library(
            storage_format
          ),
          bucket_columns: [],
          sort_columns: [],
          skewed_column_names: [],
          skewed_column_value_location_maps: {},
          skewed_column_values: [],
          stored_as_sub_directories: false,
          partition_keys: [],
          classification: tblproperties(structure)[:classification],
          convergdb_full_relation_name: tblproperties(structure)[:convergdb_full_relation_name],
          convergdb_dsd: tblproperties(structure)[:convergdb_dsd],
          convergdb_storage_bucket: tblproperties(structure)[:convergdb_storage_bucket],
          convergdb_state_bucket: tblproperties(structure)[:convergdb_state_bucket],
          convergdb_storage_format: tblproperties(structure)[:convergdb_storage_format],
          convergdb_etl_job_name: tblproperties(structure)[:convergdb_etl_job_name],
          convergdb_deployment_id: tblproperties(structure)[:convergdb_deployment_id]
        }
      end
    end
  end
end
