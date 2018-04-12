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

require_relative '../deployment/terraform/terraform.rb'

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

      # @param [Hash] structure primary_ir structure
      # @param [ConvergDB::Deployment::TerraformBuilder] terraform_builder
      def initialize(structure, terraform_builder)
        @structure = structure
        @terraform_builder = terraform_builder
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
        control_table: AWSAthenaControlTableGenerator
      }
    end

    # uses fully resolved IR to generate
    # artifacts using the various generators.
    class MasterGenerator
      # fully resolved dsd_ddd_ir
      attr_accessor :structure

      # @return [Array<BaseGenerator>]
      attr_accessor :generators

      # this is the builder to be passed to all generators
      # @return [ConvergDB::Deployment::TerraformBuilder]
      attr_accessor :terraform_builder

      def initialize(structure)
        @structure = structure
        @generators = []
        @terraform_builder = ConvergDB::Deployment::TerraformBuilder.new
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
            d = structure[k]
            g << generator_class.new(
              d,
              @terraform_builder
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

        apply_existing_athena_relations!(
          @generators,
          ConvergDB::LiveState::IR.new.comparable_athena_relations
        )

        output_diff_messages(@generators)

        # call the generate method of every generator
        @generators.each do |g|
          g.generate!
        end

        # dump all of the terraform resources created with the builder
        # into a tf.json file.
        f = File.open("#{terraform_directory_path(working_path(@structure))}/deployment.tf.json", 'w')
        f.puts "{"
        @terraform_builder.resources.each do |r|
          f.puts(r.tf_json)
        end
        f.puts "}"
        f.close
      end

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
        if Dir.exist?("#{terraform_directory_path(working_path)}/aws_glue")
          glue_py = "#{terraform_directory_path(working_path)}/aws_glue/*.py"
          Dir.glob(glue_py).each do |f|
            File.delete(f)
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
        c = ConvergDB::LiveState::IR.new

        print(
          relation_diff_message(
            c.comparable_athena_relations.keys,
            current_deployment_athena_relations(
              generators
            )
          ).join("\n")
        )
      end

      # returns an array of generator objects with current_state attached.
      # @param [Array<BaseGenerator>] generators
      # @return [Array<String>]
      def current_deployment_athena_relations(generators)
        a = generators.select { |g| g.class == ConvergDB::Generators::AWSAthena }
        a.map { |g| g.structure[:full_relation_name] }
      end

      # returns an array of generator objects with current_state attached.
      # @param [Array<BaseGenerator>] generators
      # @param [Hash] comparable_relations
      def apply_existing_athena_relations!(generators, comparable_relations)
        generators.each do |g|
          if g.class == ConvergDB::Generators::AWSAthena
            if comparable_relations[g.structure[:full_relation_name]]
              g.current_state = comparable_relations[
                g.structure[:full_relation_name]
              ]
            else
              g.current_state = {}
            end
          end
        end
      end

      # diffs two arrays of :full_relation_name.
      # this method returns an array of strings and Rainbow objects.
      # @param [Array<String>] athena
      # @param [Array<String>] current
      # @return [Array] stringlike objects for multiline output
      def relation_diff_message(athena, current)
        ret = []
        ret << Rainbow("ConvergDB relations in Athena:").bright
        athena.sort.each do |k|
          ret << "  #{k}"
        end
        ret << ''
        ret << Rainbow("Relations in current configuration:").bright
        current.sort.each do |k|
          ret << "  #{k}"
        end
        ret << ''
        removed = athena - current
        unless removed == []
          ret << Rainbow("Relations being removed:").bright.red
          removed.sort.each do |r|
            ret << Rainbow("  #{r}").red
          end
          ret << ''
        end
        ret << ''
        ret
      end

      # outputs a message indicating the difference between the current state
      # and glue jobs in the current configuration.
      # @param [Array<BaseGenerator>] generators
      def output_glue_etl_job_diff_message(generators)
        print(
          glue_etl_job_diff_message(
            generators,
            ConvergDB::LiveState::IR.new.comparable_glue_etl_jobs.map { |j| j[:name] }
          ).join("\n")
        )
      end

      # diffs two arrays of :full_relation_name.
      # this method is purely informational.
      # @param [Array<ConvergDB::Generators::BaseGenerator>] generators
      # @param [Array] comparable_jobs
      # @return [Array] stringlike objects for multiline output
      def glue_etl_job_diff_message(generators, comparable_jobs)
        current = []
        ret = []
        generators.each do |ir|
          if ir.class == ConvergDB::Generators::AWSGlue
            unless current.include?(ir.structure[:etl_job_name])
              current << ir.structure[:etl_job_name]
            end
          end
        end

        ret << Rainbow("ConvergDB ETL jobs in Glue:").bright
        comparable_jobs.sort.each do |k|
          ret << "  #{k}"
        end
        ret << ''

        ret << Rainbow("ETL jobs in current configuration:").bright
        current.sort.each do |k|
          ret << "  #{k}"
        end
        ret << ''

        removed = comparable_jobs - current
        unless removed == []
          ret << Rainbow("ETL jobs being removed:").bright.red
          removed.sort.each do |r|
            ret << Rainbow("  #{r}").red
          end
          ret << ''
        end
        ret << ''
        ret
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
