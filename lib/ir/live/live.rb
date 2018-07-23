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
require 'aws-sdk-cloudwatchevents'
require 'aws-sdk-s3'
require_relative '../../exceptions.rb'

module ConvergDB
  # live state of objects inside AWS managed by convergdb.
  module LiveState
    # creates a representation of the live state of objects in aws
    class IR
      include ConvergDB::ErrorHandling

      # returns deployment_id from the tfstate file if it exists
      # @return [String]
      def get_deployment_id
        # nil means that no deployment_id was found
        deployment_id = nil
        ignore_error do
          deployment_id = variables_file_text.match(
            /variable\s\"deployment_id\"\s+\{(.|\w)*?}/m
          )[0].split('"')[3]
        end
        # return whatever we did or did not find
        deployment_id
      end
      
      # text of the file containing variables for this deployment
      # @return [String]
      def variables_file_text
        ignore_error do
          File.read(local_variables_path)
        end
      end
      
      # @return [String]
      def local_variables_path
        './terraform/variables.tf'
      end

      # creates a glue client. will infer credentials using the normal
      # precedence of the aws sdk.
      # @return [Aws::Glue::Client]
      def glue_client
        log_warning('unable to connect to AWS glue') do
          @glue_client ||= Aws::Glue::Client.new
        end
      end

      # creates a cw events client. will infer credentials using the normal
      # precedence of the aws sdk.
      # @return [Aws::CloudWatchEvents::Client]
      def cloudwatchevents_client
        log_warning('error connecting to AWS events') do
          @cloudwatchevents_client ||= Aws::CloudWatchEvents::Client.new
        end
      end
      
      # trims all the fluff from the cloudwatch rule name
      # @param [String] rule
      # @return [String]
      def rule_name_to_etl_job(rule)
        rule.gsub(/^convergdb\-(\d|[abcdef]){16}\-/, '').gsub(/\-trigger$/, '')
      end
      
      # calls AWS to get the event rules. rules are tagged to indicate
      # whether or not they are a part of the specified deployment.
      # @param [Aws::CloudWatchEvents::Client] client
      # @param [String] deployment_id
      # @return [Array<Hash>]
      def event_rules_for_comparison(client, deployment_id)
        rules = []
        client.list_rules.each do |resp|
          resp.rules.each do |rule|
            if deployment_id
              if rule.name.match(/^convergdb\-#{deployment_id}/)
                rules << {
                  name: rule_name_to_etl_job(rule.name),
                  technology: 'aws_fargate',
                  this_deployment: true
                }
              elsif rule.name.match(/^convergdb/)
                rules << {
                  name: rule_name_to_etl_job(rule.name),
                  technology: 'aws_fargate',
                  this_deployment: false
                }
              end
            else
              if rule.name.match(/^convergdb/)
                rules << {
                  name: rule_name_to_etl_job(rule.name),
                  technology: 'aws_fargate',
                  this_deployment: false
                }
              end
            end
          end
        end
        rules
      end

      # creates a s3 client. will infer credentials using the normal
      # precedence of the aws sdk.
      # @return [Aws::S3::Client]
      def s3_client
        log_warning('unable to connect to s3') do
          @s3_client ||= Aws::S3::Client.new
        end
      end
      
      # creates a list of fargate ETL jobs currently deployed in AWS
      # @return [Array<Hash>]
      def fargate_etl_jobs
        log_warning('error identifying Fargate ETL jobs in AWS.. please check credentials') do
          event_rules_for_comparison(
            cloudwatchevents_client,
            get_deployment_id
          )
        end
      end

      # calls AWS to get the Glue ETL jobs which are tagged to indicate
      # whether or not they are a part of the specified deployment.
      # @param [Aws::Glue::Client] client
      # @param [String] deployment_id
      # @return [Array<Hash>]
      def glue_etl_jobs_for_comparison(client, deployment_id)
        jobs = []
        if client
          client.get_jobs.each do |resp|
            resp.jobs.each do |job|
              if deployment_id
                if job.default_arguments.key?('--convergdb_deployment_id')
                  if job.default_arguments['--convergdb_deployment_id'] == deployment_id
                    jobs << {
                      name: job.name,
                      technology: 'aws_glue',
                      this_deployment: true
                    }
                  else
                    jobs << {
                      name: job.name,
                      technology: 'aws_glue',
                      this_deployment: false
                    }
                  end
                end
              else
                jobs << {
                  name: job.name,
                  technology: 'aws_glue',
                  this_deployment: false
                }
              end
            end
          end
        end
        return jobs
      end

      # creates a list of glue ETL jobs currently deployed in AWS
      # @return [Array<Hash>]
      def glue_etl_jobs
        log_warning('error identifying Glue ETL jobs in AWS.. please check credentials') do
          glue_etl_jobs_for_comparison(
            glue_client,
            get_deployment_id
          )
        end
      end

      # creates a list of ETL jobs currently deployed in AWS
      # @return [Array<Hash>]
      def all_etl_jobs
        fargate_etl_jobs + glue_etl_jobs
      end
      
      # creates a list of all databases in AWS glue catalog.
      # @param [Aws::Glue::Client] client
      # @return [Array<String>]
      def glue_databases(client)
        databases = []
        client.get_databases.each do |resp|
          resp.database_list.each do |db|
            databases << db.name
          end
        end
        databases
      end
      
      # creates a list of glue tables that exist in the current AWS environment.
      # @param [Aws::Glue::Client] client
      # @param [Array<String>] databases a list of databases (see glue_database method)
      # @param [String] deployment_id
      # @return [Array<String>]
      def glue_tables(client, databases, deployment_id)
        tables = []
        databases.each do |db|
          client.get_tables(database_name: db).each do |resp|
            resp.table_list.each do |table|
              if table.parameters.key?('convergdb_deployment_id')
                if table.parameters['convergdb_deployment_id'] == deployment_id
                  tables << {
                    name: "#{db}.#{table.name}",
                    this_deployment: true
                  }
                else
                  tables << {
                    name: "#{db}.#{table.name}",
                    this_deployment: false
                  }
                end
              else
                tables << {
                  name: "#{db}.#{table.name}",
                  this_deployment: false
                }
              end
            end
          end
        end
        tables
      end
      
      # a list of tables in the current AWS deployment
      # @return [Array<Hash>]
      def all_tables
        log_warning('error identifying tables in AWS.. please check credentials') do
          glue_tables(
            glue_client,
            glue_databases(glue_client),
            get_deployment_id
          )
        end
      end
    end
  end
end

