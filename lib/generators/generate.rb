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
require 'securerandom'

require 'aws-sdk-cloudwatchevents'
require 'aws-sdk-ecs'
require 'aws-sdk-glue'

require_relative '../deployment/terraform/terraform.rb'
require_relative '../exceptions.rb'

module ConvergDB
  module Generators
    # base class used to create generators. generator classes implement a
    # resolve! validate structure pattern.
    # @abstract
    class BaseGenerator
      # this is the hash passed from the primary IR
      attr_accessor :structure

      # passed from the master generator
      # @return [ConvergDB::Deployment::TerraformBuilder]
      attr_accessor :terraform_builder

      # @return [Hash]
      attr_accessor :aws_clients

      # @param [Hash] structure primary_ir structure
      # @param [ConvergDB::Deployment::TerraformBuilder] terraform_builder
      # @param [Hash] aws_clients aws connections for use in generator
      def initialize(structure, terraform_builder, aws_clients)
        @structure = structure
        @terraform_builder = terraform_builder
        @aws_clients = aws_clients
        post_initialize
      end

      # override if tasks need to take place post
      # object initialization.
      def post_initialize
        nil
      end

      # must be implemented. will be called
      # by the master.. and expects the object
      # to generate the concrete artifacts
      def generate!
        raise "must override! generate method in class #{self.class}"
      end

      # creates the directories before opening.
      # @param [String] path
      # @param [String] mode
      # @return [File]
      def create_dirs_and_open_file(path, mode)
        FileUtils.mkdir_p(
          File.dirname(
            path
          )
        )

        File.open(
          path,
          mode
        )
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
          Rainbow("  #{diff[0]} #{diff[1]} from '#{diff[2]}' to '#{diff[3]}'").yellow + "\n"
        end
      end
      
      # outputs diff information.
      # @param [String] header
      # @param [Array<String>] diff
      def output_diff(header, diff)
        puts(
          Rainbow(
            header
          ).bright
        )
        if diff.length == 0
          puts("  no change")
        else
          diff.each do |d|
            puts(d)
          end
        end
        puts('')
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
    end

    # maps symbol to generator class
    # @return [Hash]
    def self.generator_class_map
      {
        athena: AWSAthena,
        glue: AWSGlue,
        s3_source: AWSS3Source,
        markdown_doc: MarkdownDoc,
        html_doc: HtmlDoc,
        control_table: AWSAthenaControlTableGenerator,
        streaming_inventory: StreamingInventoryTableGenerator,
        fargate: AWSFargate
      }
    end

    # uses fully resolved IR to generate
    # artifacts using the various generators.
    class MasterGenerator
      include ConvergDB::ErrorHandling

      # fully resolved dsd_ddd_ir
      attr_accessor :structure

      # @return [Array<BaseGenerator>]
      attr_accessor :generators

      # this is the builder to be passed to all generators
      # @return [ConvergDB::Deployment::TerraformBuilder]
      attr_accessor :terraform_builder
      
      attr_accessor :aws_clients
      
      def initialize(structure)
        @structure = structure
        @generators = []
        @terraform_builder = ConvergDB::Deployment::TerraformBuilder.new
        log_warning("unable to create AWS client connections") do
          @aws_clients = aws_clients
        end
      end

      # @return [Hash]
      def aws_clients
        {
          aws_glue: Aws::Glue::Client.new,
          aws_ecs: Aws::ECS::Client.new,
          aws_cloudwatch_events: Aws::CloudWatchEvents::Client.new
        }
      end

      # returns an array of generator objects by iterating through all the
      # :generators attached to the relation structure. this is a one to many
      # relationship where a single relation will create several generators.
      # @param [Hash] structure primary IR with a hash for each relation
      # @return [Array<BaseGenerator>]
      def create_generators(structure)
        g = []
        structure.keys.each do |k|
          structure[k][:generators].each do |i|
            # lookup the class based upon the symbol name
            generator_class = ConvergDB::Generators.generator_class_map[i.to_sym]
            raise "generator not defined #{i}" if generator_class == nil
            # creating new instance of generator_class... initializing
            # it with the hash from the primary IR as well as the current
            # terraform builder
            g << generator_class.new(
              structure[k],
              @terraform_builder,
              @aws_clients
            )
          end
        end
        g
      end

      # iterates through all of the generator objects and calls
      # the generate method. this will create the concrete
      # implementations. this method kicks off the generation.
      def generate!
        # basic terraform file setup
        create_static_artifacts!(working_path(@structure))

        # creates the generator array from the primary IR
        @generators = create_generators(@structure)
        
        output_diff_messages(@generators) unless @aws_clients.nil?

        # call the generate method of every generator
        @generators.each do |g|
          g.generate!
        end

        # dump all of the terraform resources created with the builder
        # into a tf.json file.
        f = File.open("#{terraform_directory_path(working_path(@structure))}/deployment.tf", 'w')
        @terraform_builder.resources.each do |r|
          f.puts(r.tf_hcl)
        end
        f.close
      end

      # outputs high level diff messages for tables and ETL jobs
      # created by convergdb.
      # @param [Array<BaseGenerator>] generators
      def output_diff_messages(generators)
        # output information about glue etl job diffs
        output_glue_etl_job_diff_message(generators)

        # pulls the current state of the relations from AWS and attaches
        # it to the relation of the same name if present. also outputs
        # a list of all relations in AWS.. in this config... and which
        # relations will be deleted.
        output_athena_relation_diff_message(generators)
      end

      # extracts the working_path from a provided primary_ir structure
      # @param [Hash] structure primary_ir structure
      # @return [String]
      def working_path(structure)
        working_path = structure[structure.keys.first][:working_path]
      end

      # @param [String] working_path
      # @return [String]
      def terraform_directory_path(working_path)
        "#{working_path}/terraform"
      end

      # makes sure the terraform and docs directories exist.
      # docs are deleted every time before they are recreated. same goes
      # for glue etl job pyspark scripts.
      # @param [String] working_path
      def create_static_artifacts!(working_path)
        # terraform directory
        FileUtils.mkdir_p(
          terraform_directory_path(working_path)
        )

        # aws provider config for terraform
        FileUtils.cp(
          "#{File.dirname(__FILE__)}/aws.tf",
          "#{working_path}/terraform/aws.tf"
        )

        create_bootstrap_artifacts!(working_path)

        # etl job files are rebuilt each time so old ones are deleted
        ['aws_glue', 'aws_fargate'].each do |etl|
          if Dir.exist?("#{terraform_directory_path(working_path)}/#{etl}")
            glue_py = "#{terraform_directory_path(working_path)}/#{etl}/*.py"
            Dir.glob(glue_py).each do |f|
              File.delete(f)
            end
          end
        end

        # delete'em if you got 'em
        if Dir.exist?("#{working_path}/docs")
          Dir.glob("#{working_path}/docs/*.md").each do |f|
            File.delete(f)
          end

          Dir.glob("#{working_path}/docs/*.html").each do |f|
            File.delete(f)
          end
        end

        # otherwise clear the way...
        FileUtils.mkdir_p("#{working_path}/docs")
      end

      # creates the bootstrap folder and the bootstrap.tf file. this is used
      # to create the s3 bucket for convergdb admin (and tf backend) purposes,
      # as well as the dynamodb lock table.
      # @param [String] working_path
      def create_bootstrap_artifacts!(working_path)
        if !Dir.exist?("#{working_path}/bootstrap")
          # generate random suffix for admin S3 bucket
          suffix = SecureRandom.hex(8)
          admin_bucket = "convergdb-admin-#{suffix}"
          data_bucket = "convergdb-data-#{suffix}"
          lock_table = "convergdb-lock-#{suffix}"
          etl_lock_table = "convergdb-etl-lock-#{suffix}"
          deployment_id = "#{suffix}"

          # Write terraform backend bootstrap deployment to separate directory
          FileUtils.mkdir_p("#{working_path}/bootstrap")

          bootstrap_output = ERB.new(
            IO.read(
              "#{File.dirname(__FILE__)}/bootstrap.tf.erb"
            )
          ).result(binding).to_s

          File.open("#{working_path}/bootstrap/bootstrap.tf", 'w') do |f|
            f.puts(bootstrap_output)
          end
        end
      end

      # takes in the array of generators... checks to see if the
      # relation exists in AWS at this time. if so... it attaches
      # the relation as current_state... if not it attaches an empty hash.
      # then it calls the method to trigger a nice terminal output
      # to summarize the relation diff between AWS and the current config.
      # @param [Array<BaseGenerator>] generators
      def output_athena_relation_diff_message(generators)
        log_warning('unable to create diff analysis for tables') do
          live = ConvergDB::LiveState::IR.new.all_tables
          this_deployment = []
          live.each do |t|
            if (t[:this_deployment] == true)
              if (!t[:name].match(/^convergdb\_control/))
                this_deployment << t[:name]
              end
            end
          end
          local = current_deployment_athena_relations(generators)

          removed_tables = (this_deployment - local)
          if removed_tables.length > 0
            puts Rainbow("Tables being removed:").bright.red
            removed_tables.each do |removed|
              puts(Rainbow("  #{removed}").red)
            end
            puts
          end

          added_tables = local - live.map { |t| t[:name] }
          if added_tables.length > 0
            puts Rainbow("Tables being added:").bright.green
            added_tables.each do |added|
              puts(Rainbow("  #{added}").green)
            end
            puts
          end
        end
      end

      # returns an array of generator objects with current_state attached.
      # @param [Array<BaseGenerator>] generators
      # @return [Array<String>]
      def current_deployment_athena_relations(generators)
        a = generators.select do |g|
          g.class == ConvergDB::Generators::AWSAthena
        end
        a.map do |g|
          splitted = g.structure[:full_relation_name].split('.')
          "#{splitted[0..2].join('__')}.#{splitted[3]}"
        end
      end

     # @param [Array<BaseGenerator>] generators
     # @return [Array<String>]
      def current_deployment_etl_jobs(generators)
        etl_jobs = []
        generators.each do |g|
          case
            when [
              ConvergDB::Generators::AWSGlue,
              ConvergDB::Generators::AWSFargate
            ].include?(g.class)
            then etl_jobs << {
              name: g.structure[:etl_job_name],
              technology: g.structure[:etl_technology]
            }
          end
        end
        etl_jobs.uniq
      end

      # outputs a message indicating the difference between the current state
      # and glue jobs in the current configuration.
      # @param [Array<BaseGenerator>] generators
      def output_glue_etl_job_diff_message(generators)
        live = ConvergDB::LiveState::IR.new.all_etl_jobs
        local = current_deployment_etl_jobs(generators)
        local_names = local.map { |l| l[:name] }

        live_this_deployment = []
        live.each do |job|
          live_this_deployment << job[:name] if job[:this_deployment]
        end

        # check for duplicate ETL job technologies

        # ETL jobs added
        added_jobs = local_names - live_this_deployment
        if added_jobs.length > 0
          puts Rainbow('ETL Jobs Added').bright.green
          added_jobs.each do |j|
            puts Rainbow("  #{j}").green
          end
          puts
        end

        # ETL jobs removed
        removed_jobs = live_this_deployment - local_names
        if removed_jobs.length > 0
          puts Rainbow('ETL Jobs Removed').bright.red
          removed_jobs.each do |j|
            puts Rainbow("  #{j}").red
          end
          puts
        end

        # ETL jobs technology changed
      end
    end
  end
end

# having these requires after the base class definition
# smooths out the class dependency resolution.
require_relative 'athena/athena.rb'
require_relative 'glue/glue.rb'
require_relative 's3_source/s3_source.rb'
require_relative 'markdown_doc/markdown_doc.rb'
require_relative 'html_doc/html_doc.rb'
require_relative 'control_table/control_table.rb'
require_relative 'streaming_inventory/streaming_inventory.rb'
require_relative 'fargate/fargate.rb'
