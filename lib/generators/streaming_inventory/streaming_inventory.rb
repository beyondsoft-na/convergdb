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
    # used to generate the a streaming inventory pipeline for the bucket
    # associated with an s3_source relation. streaming inventories are
    # only created at the bucket level, so the same table is shared across
    # multiple
    class StreamingInventoryTableGenerator < ConvergDB::Generators::AWSAthena
      # generates the artifacts necessary to deploy a streaming inventory
      # pipeline to be used as an optimization in the ETL jobs. though this
      # generator may be run many times for the same bucket, the idempotent
      # handling in the terraform builder results in only a single database,
      # single table, and single pipeline per bucket. ETL jobs need to
      # filter on the key field in order to filter the files relevant to them.
      def generate!
        if @structure[:streaming_inventory].match(/^true$/i)
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

          @terraform_builder.streaming_inventory_module!(
            streaming_inventory_module_params(
              @structure,
              @terraform_builder
            )
          )
        end
      end

      # creates necessary files and folders for use with terraform
      # @param [String] working_path working path for this run
      def create_static_artifacts!(working_path)
        super(working_path)
      end
      
      # @return [String]
      def storage_format
        'json'
      end

      # these are the field definitions for a streaming inventory table.
      # @return [Array<Hash>]
      def streaming_inventory_attributes
        [
          {
            name: 'last_modified_timestamp',
            data_type: 'timestamp',
            expression: ''
          },
          {
            name: 'bucket',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'key',
            data_type: 'varchar(1024)',
            expression: ''
          },
          {
            name: 'size',
            data_type: 'bigint',
            expression: ''
          },
          {
            name: 'e_tag',
            data_type: 'varchar(32)',
            expression: ''
          },
          {
            name: 'sequencer',
            data_type: 'varchar(16)',
            expression: ''
          }
        ]
      end

      # provides parameters to pass to terraform builder.
      # @param [Hash] structure
      # @param [ConvergDB::Deployment::Terraform::TerraformBuilder] terraform_builder
      # @return [Hash]
      def streaming_inventory_module_params(structure, terraform_builder)
        {
          :resource_id => streaming_inventory_resource_id(structure),
          :source => './modules/streaming_inventory',
          :storage_bucket => structure[:storage_bucket],
          :streaming_inventory_output_bucket => structure[
            :streaming_inventory_output_bucket
          ]
        }
      end

      # creates a resource id to be used as a key for idempotent handling
      # in the terraform builder.
      # @param [Hash] structure
      # @return [String]
      def streaming_inventory_resource_id(structure)
        inv_name = @terraform_builder.to_underscore(
          structure[:storage_bucket].split('/')[0]
        )
        "streaming_inventory_#{inv_name}"
      end

      # extracts "table name" from the source storage_bucket
      # @param [String] storage_bucket
      # @return [String]
      def table_name(storage_bucket)
        @terraform_builder.to_underscore(storage_bucket)
      end

      # @param [String] source_bucket bucket being inventoried
      # @return [String] bucket path without prefix
      def inventory_s3_bucket(source_bucket)
        source_bucket.split('/')[0]
      end

      # s3 url formattted storage location for use in table definition
      # needs some refactoring after switching to new terraform handling
      # of glue tables.
      # @param [Hash] structure
      # @return [String]
      def s3_storage_location(structure)
        a = structure[:streaming_inventory_output_bucket].gsub(
          'var.admin_bucket',
          'var.admin_bucket'
        ).gsub(
          'var.deployment_id',
          'var.deployment_id'
        )
        "s3://#{a}"
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
          convergdb_storage_format: structure[:storage_format],
          convergdb_etl_job_name: structure[:etl_job_name] || '',
          convergdb_deployment_id: %(${var.deployment_id})
        }
      end

#      # returns a name to be used as db/schema/etc. this version resolves the
#      # deployment_id in the tf template file for cf stack.
#      # @return [String]
#      def athena_database_name
#        'convergdb_inventory_${deployment_id}'
#      end

      # returns a name to be used as db/schema/etc. this version resolves the
      # deployment_id in the deployment.tf.json file.
      # @return [String]
      def athena_database_name(ignored)
        'convergdb_inventory_${var.deployment_id}'
      end
      
      def table_parameters(structure)
        {
          # database name uses module output to force
          database_name: "${module.#{@terraform_builder.database_module_name(athena_database_name(nil))}.database_name}",
          table_name: table_name(structure[:storage_bucket].split('/')[0]),
          columns: streaming_inventory_attributes.map  { |a| terraform_column_attributes(a) },
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
