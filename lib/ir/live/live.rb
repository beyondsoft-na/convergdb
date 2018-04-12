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

require 'aws-sdk-glue'
require 'aws-sdk-s3'
require_relative '../base_ir.rb'
require_relative '../../exceptions.rb'
require 'json'
require 'pp'
require 'digest'

module ConvergDB
  # live state of objects inside AWS managed by convergdb.
  module LiveState
    # creates a representation of the live state of the athena tables in aws
    class IR
      include ConvergDB::ErrorHandling

      # reads a file from S3 then returns the contents as a string.
      # @param [String] bucket
      # @param [String] key
      # @param [String] warning
      def s3_object_to_string(bucket, key, warning = 's3 get object error')
        log_warning(warning) do
          s3 = s3_client
          resp = s3.get_object(
            bucket: bucket,
            key: key
          )
          return resp.body.read rescue nil
        end
      end

      # returns deployment_id from the tfstate file if it exists
      # @return [String]
      def get_deployment_id(
          local_state_path = tfstate_path,
          backend_path = tfjson_path
        )
        # this will be our return value.
        # nil means that no deployment_id was found
        deployment_id = nil
        ignore_error do
          # first check to see if a local tfstate file exists
          if File.exist?(local_state_path)
            # if it exists... we will attempt to extract the deployment_id
            deployment_id = deployment_id_from_tfstate_json(
              File.read(local_state_path)
            )

          # if no local file... let's check for tf backend configuration
          elsif File.exist?(backend_path)
            # if we have a backend config... extract the s3 location info
            # in order to pull down the state file
            s3_attributes = s3_attributes_from_tf_backend(
              File.read(backend_path)
            )

            # attempt to pull down state file as a string
            resp = s3_object_to_string(
              s3_attributes[:bucket],
              s3_attributes[:key],
              'tf state backend not found'
            )

            # if we have some text... try to extract the deployment_id
            unless resp.nil?
              deployment_id = deployment_id_from_tfstate_json(resp)
            end
          end
        end
        # return whatever we did or did not find
        deployment_id
      end

      # extracts deployment_id from a json string
      # @param [String] tfstate_json
      def deployment_id_from_tfstate_json(tfstate_json)
        # first turn the json into a hash
        r = {}
        ignore_error do
          r = JSON.parse(tfstate_json)
        end
        # if no modules present or other bs
        # then there is nothing to see here
        return nil unless r.key?('modules')
        # iterate the modules looking for the structure we want
        r['modules'].each do |m|
          begin
            if m['path'] == ['root']
              # return this as the deployment id...
              return m['resources']['random_id.convergdb_deployment_id']['primary']['attributes']['dec']
            end
          rescue => e
            # ...but trap the error in case the key doesn't exist
            nil
          end
        end
        # nil is default which means no deployment_id found
        nil
      end

      # parses out bucket, key, and region from backend config file created
      # by convergdb bootstrap.
      # @param [String] backend_json json text of backend config
      # @return [Hash]
      def s3_attributes_from_tf_backend(backend_json)
        j = {}
        ignore_error do
          j = JSON.parse(backend_json)
        end
        return nil unless j.key?('terraform')
        {
          bucket: j['terraform']['backend']['s3']['bucket'],
          key: j['terraform']['backend']['s3']['key'],
          region: j['terraform']['backend']['s3']['region']
        }
      end

      # path to the local terraform state file
      def tfstate_path
        './terraform/terraform.tfstate'
      end

      # path to the terraform backend configuration file created by
      # convergdb bootstrap process
      def tfjson_path
        './terraform/terraform.tf.json'
      end

      # creates a glue client. will infer credentials using the normal
      # precedence of the aws sdk.
      # @return [Aws::Glue::Client]
      def glue_client
        @glue_client ||= Aws::Glue::Client.new
      end

      # creates a s3 client. will infer credentials using the normal
      # precedence of the aws sdk.
      # @return [Aws::S3::Client]
      def s3_client
        @s3_client ||= Aws::S3::Client.new
      end

      # get a list of all database names available in this aws region
      # @return [Array<String>]
      def database_names
        @database_names ||= glue_client.get_databases.database_list.map(&:name)
      end

      # makes a single call to get metadata for tables associated with the
      # given database.
      # @param [String] database
      # @param [String] next_token defaults to nil if not provided
      # @return [Struct] database table metadata
      def tables_for_database(database, next_token = nil)
        m = glue_client.get_tables(
          {
            database_name: database,
            next_token: next_token
          }
        )
      end

      # gathers all of the metadata about the tables in a database.
      # this function handles the continuation token nature of the api calls.
      # note that the results are filtered for tables created by convergdb.
      # @param [String] database
      # @return [Array<Struct>]
      def all_tables_for_database(database)
        t = []
        # next token is outside of the loop
        next_token = nil
        loop do
          # first call will be for first result set because next_token is nil.
          resp = tables_for_database(
            database,
            next_token
          )
          # append all of the tables to the array
          resp.table_list.each do |tbl|
            t << tbl.to_h
          end
          # set next_token if there is one
          next_token = resp.next_token
          # otherwise exit the loop
          break if next_token.nil?
        end
        t
      end

      # filter for only convergdb managed tables and return
      # @param [Array<Hash>] tables
      # @return [Array<Hash>]
      def convergdb_relations_only(tables)
        tables.select do |f|
          f[:parameters].has_key?('convergdb_full_relation_name')
        end
      end

      # extracts a comparable table structure from the API response structure
      # @param [Hash] tbl table object from glue API
      # @param [String] deployment_id
      def comparable_table(tbl, deployment_id)
        # info is extracted from the parameters associated with the table.
        # these p
        # use show TBLPROPERTIES tablename to see this info via athena.
        t = tbl[:parameters]
        {
          full_relation_name: t['convergdb_full_relation_name'],
          dsd: t['convergdb_dsd'],
          storage_bucket: convergdb_bucket_reference(
            t['convergdb_storage_bucket'],
            deployment_id
          ),
          state_bucket: convergdb_bucket_reference(
            t['convergdb_state_bucket'],
            deployment_id
          ) || '',
          storage_format: t['convergdb_storage_format'],
          etl_job_name: t['convergdb_etl_job_name'] || '',
          attributes: tbl[:storage_descriptor][:columns].map do |c|
            {
              name: c[:name],
              data_type: c[:type],
              expression: c[:comment].to_s
            }
          end
        }
      end

      # creates a hash which can be compared to elements in the primary IR.
      # info is extracted from the parameters associated with the table
      # at create time.
      # use show TBLPROPERTIES tablename to see this info via athena.
      # @return [Hash]
      def comparable_athena_relations
        deployment_id = get_deployment_id
        athena_relations = {}
        if deployment_id
          database_names.each do |db|
            convergdb_relations_only(
              all_tables_for_database(db)
            ).each.map do |r|
              # info is extracted from the parameters associated with the table.
              # use show TBLPROPERTIES tablename to see this info via athena.
              t = r[:parameters]
              if t['convergdb_deployment_id'] == deployment_id
                athena_relations[
                  t['convergdb_full_relation_name']
                ] = comparable_table(r, deployment_id)
              end
            end
          end
        end
        # puts JSON.pretty_generate(athena_relations)
        athena_relations
      end

      # replaces convergdb bootstrap bucket references with
      # variable name to allow comparison to primary_ir.
      # @param [String] input
      # @return [String]
      def convergdb_bucket_reference(input, deployment_id)
        return input unless input.class == String
        admin_bucket = /^convergdb\-admin\-[0-9a-f]{16}/
        data_bucket  = /^convergdb\-data\-[0-9a-f]{16}/
        input.gsub(
          admin_bucket,
          '${admin_bucket}'
        ).gsub(
          data_bucket,
          '${data_bucket}'
        ).gsub(
          deployment_id.to_s,
          '${deployment_id}'
        )
      end

      # @return [Hash] glue get_trigger response as hash
      def glue_triggers
        return @glue_triggers if defined?(@glue_triggers)
        @glue_triggers = glue_client.get_triggers.to_h
      end

      # @param [Hash] trigger_response
      # @return [Array<Hash>]
      def convergdb_glue_triggers(trigger_response)
        trigger_response[:triggers].select { |tr| tr[:name] =~ /^convergdb\-*/ }
      end

      # returns a simple hash with ETL job triggers keyed by job name
      # @param [Array<Hash>] triggers
      # @return [Hash]
      def schedules_by_job_name(triggers)
        ret = {}
        triggers.each do |t|
          ret[t[:actions][0][:job_name]] = t[:schedule]
        end
        ret
      end

      # @return [Hash]
      def glue_etl_jobs
        return @glue_etl_jobs if defined?(@glue_etl_jobs)
        @glue_etl_jobs = glue_client.get_jobs.jobs.to_a.map(&:to_h)
      end

      # @param [Array<Hash>] etl_jobs
      # @return [Array<Hash>]
      def convergdb_etl_jobs(etl_jobs)
        etl_jobs.select do |j|
          j[:default_arguments].key?('--convergdb_deployment_id')
        end
      end

      # @param [Array<Hash>] convergdb_jobs
      # @param [String]  deployment_id
      # @param [Array<Hash>]
      def convergdb_this_deployment_etl_jobs(convergdb_jobs, deployment_id)
        convergdb_jobs.select do |j|
          j[:default_arguments]['--convergdb_deployment_id'] == deployment_id
        end
      end

      # creates an array which can be compared to elements in the primary IR.
      # @param [Array<Hash>] etl_jobs in hash format returned by AWS SDK
      # @return [Array]
      def comparable_glue_etl_jobs(
          etl_jobs = glue_etl_jobs,
          schedule = schedules_by_job_name(
            convergdb_glue_triggers(
              glue_triggers
            )
          ),
          deployment_id = get_deployment_id
        )

        job_list = []
        if deployment_id
          job_list = convergdb_this_deployment_etl_jobs(
            convergdb_etl_jobs(
              etl_jobs
            ),
            deployment_id
          ).map { |j| j[:name] }
        end
        job_list.map do |j|
          {
            name: j,
            schedule: schedule[j]
          }
        end
      end
    end
  end
end
