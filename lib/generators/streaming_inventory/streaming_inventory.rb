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

        unless Dir.exist?("#{working_path}/terraform/modules")
          FileUtils.mkdir_p("#{working_path}/terraform/modules")
        end

        FileUtils.cp_r(
          "#{File.dirname(__FILE__)}/modules/",
          "#{working_path}/terraform/"
        )
      end
      
      # @return [String]
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

      # @return [String]
      def athena_database_tf_module_name
        'convergdb_athena_databases_stack'
      end

      # extracts "table name" from the source storage_bucket
      # @param [String] relation_name
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
      # @param [Hash] structure
      # @return [String]
      def s3_storage_location(structure)
        a = structure[:streaming_inventory_output_bucket].gsub(
          'var.admin_bucket',
          'admin_bucket'
        ).gsub(
          'var.deployment_id',
          'deployment_id'
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
          convergdb_deployment_id: %(${deployment_id}),
          convergdb_database_cf_id:
            %(${database_stack_id})
        }
      end

      # returns a name to be used as db/schema/etc. this version resolves the
      # deployment_id in the tf template file for cf stack.
      # @return [String]
      def athena_database_name
        'convergdb_inventory_${deployment_id}'
      end

      # returns a name to be used as db/schema/etc. this version resolves the
      # deployment_id in the deployment.tf.json file.
      # @return [String]
      def database_name
        'convergdb_inventory_${var.deployment_id}'
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
          %(convergdbInventoryTable#{
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
                  'Columns' => streaming_inventory_attributes.map do |a|
                    {
                      'Name' => a[:name],
                      'Type' => athena_data_type(a[:data_type]),
                      'Comment' => a[:expression] || ''
                    }
                  end,
                  'Compressed' => false
                },
                'PartitionKeys' => [],
                'Name' => table_name(structure[:storage_bucket].split('/')[0]),
                # 'Parameters' => tblproperties(structure),
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
          %(convergdbInventoryDatabase${var.deployment_id}) =>
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
