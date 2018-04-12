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

require_relative '../base_ir.rb'

module ConvergDB
  # used for DDD intermediate representations
  module DDD
    # represents a top level object for holding deployments.
    class DDDTopLevel < ConvergDB::BaseStructure
      # array of deployments
      attr_accessor :deployment

      # sets :deployment to an empty array
      def initialize
        @deployment = []
      end

      # @return [Array<Hash>]
      def structure
        @deployment.map(&:structure)
      end

      # resolves all items in :deployment array.
      # nothing to resolve inside top level object.
      def resolve!
        @deployment.each(&:resolve!)
      end

      # validates all items in :deployment array.
      # nothing to validate inside top level object.
      def validate
        @deployment.each(&:validate)
      end
    end

    # represents base relations with s3 "inbox"
    class AWSS3Source < ConvergDB::BaseStructure
      # AWS region of this deployment
      attr_accessor :region

      # environment for all relations in this deployment.
      # required.
      attr_accessor :environment

      # optional override for domain_name affecting
      # all relations in this deployment.
      attr_accessor :domain_name

      # optional override for schema_name affecting
      # all relations in this deployment.
      attr_accessor :schema_name

      # array of relations
      attr_accessor :relations

      # initialize with reference to parent and current environment.
      # :relations is set to empty array.
      # @param [DDDTopLevel] parent
      # @param [String] environment
      def initialize(parent, environment)
        @parent = parent
        @environment = environment
        @relations = []
      end

      # local attributes and relations attributes
      # @return [Hash]
      def structure
        {
          # region: @region,
          environment: @environment,
          domain_name: @domain_name,
          schema_name: @schema_name,
          relations: @relations.map(&:structure)
        }
      end

      # resolves relations
      def resolve!
        @relations.each(&:resolve!)
      end

      # hash containing symbol names mapped to regex patterns.
      # the symbol names match the methods that they are used
      # to validate. for example :environment key in this hash
      # correlates to the @environment attribute of this object.
      # @return [Hash]
      def validation_regex
        {
          environment: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: true
          },
          schema_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
          domain_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
        }
      end

      # performs validation on this object... then
      # validates all of the :relation items.
      def validate
        validate_string_attributes
        @relations.each(&:validate)
      end
    end

    # represents base relation as s3 "inbox"
    class AWSS3SourceRelation < ConvergDB::BaseStructure
      # dsd name db.schema.relation
      attr_accessor :dsd

      attr_accessor :full_relation_name

      # AWS region of this deployment
      # attr_reader :region

      # environment for all relations in this deployment.
      # required.
      attr_accessor :environment

      # optional override for domain_name affecting
      # all relations in this deployment.
      attr_accessor :domain_name

      # optional override for schema_name affecting
      # all relations in this deployment.
      attr_accessor :schema_name

      attr_accessor :relation_name

      # bucket to store the data. required.
      attr_accessor :storage_bucket

      # data format (json)
      attr_accessor :storage_format

      attr_accessor :inventory_table

      # @param [Object] parent
      def initialize(parent)
        @parent = parent
      end

      # @return [Hash]
      def structure
        {
          generators: [:s3_source, :markdown_doc, :html_doc],
          dsd: @dsd,
          full_relation_name: @full_relation_name,
          environment: @environment,
          domain_name: @domain_name,
          schema_name: @schema_name,
          relation_name: @relation_name,
          storage_bucket: @storage_bucket,
          storage_format: @storage_format,
          inventory_table: @inventory_table
        }
      end

      # resolves all of the attributes of this object
      def resolve!
        # @region = @parent.region
        @environment = @parent.environment
        @domain_name = override_parent(:domain_name)
        @schema_name = override_parent(:schema_name)
        # no resolve for @relation_name
        @full_relation_name = resolve_full_relation_name
        @inventory_table ||= ''
        # buckets will default if not specified
        unless @storage_bucket
          @storage_bucket = "${data_bucket}/${deployment_id}/#{@full_relation_name}"
        end
      end

      # full_relation_name is created by overriding the attributes
      # of the dsd with locally specified attributes.
      def resolve_full_relation_name
        f = []
        f[0] = @environment
        f[1] = @domain_name || @dsd.split('.')[0]
        f[2] = @schema_name || @dsd.split('.')[1]
        f[3] = @relation_name || dsd.split('.')[2]
        f.join('.')
      end

      # hash containing symbol names mapped to regex patterns.
      # the symbol names match the methods that they are used
      # to validate. for example :environment key in this hash
      # correlates to the @environment attribute of this object.
      # @return [Hash]
      def validation_regex
        {
          dsd: {
            regex: ConvergDB::ValidationRegex::DSD_RELATION_NAME,
            mandatory: true
          },
          environment: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: true
          },
          schema_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
          domain_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
          relation_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
          full_relation_name: {
            regex: /^[a-zA-Z]+\w*\.[a-zA-Z]+\w*\.[a-zA-Z]+\w*\.[a-zA-Z]+\w*$/,
            mandatory: true
          },
          inventory_table: {
            regex: ConvergDB::ValidationRegex::ATHENA_TABLE,
            mandatory: false
          },
          storage_bucket: { regex: /.*/, mandatory: false },
          storage_format: { regex: /json/, mandatory: false }
        }
      end

      # performs validation on this object.
      def validate
        validate_string_attributes
      end
    end

    # represents an AWS athena deployment. :relations array
    # contains objects for each relation defined in the deployment.
    class AWSAthena < ConvergDB::BaseStructure
      # holds all relations in this deployment
      attr_accessor :relations

      # environment for all relations attached to this object
      attr_accessor :environment

      # optional override to be applied to :dsd database name.
      # will apply to all relations
      attr_accessor :domain_name

      # optional override to be applied to :dsd schema name.
      # will apply to all relations
      attr_accessor :schema_name

      # aws region for this athena deployment.
      # can be overridden.
      attr_accessor :region

      # aws service role.
      # can be overridden.
      attr_accessor :service_role

      # bucket to store athena glue scripts.
      # can be overridden.
      attr_accessor :script_bucket

      # temp location for DDL execution.
      # can be overridden.
      attr_accessor :temp_s3_location

      attr_accessor :storage_format

      attr_accessor :source_relation_prefix

      attr_accessor :etl_job_name

      attr_accessor :etl_job_schedule

      attr_accessor :etl_job_dpu

      # initialize with reference to parent and current environment.
      # :relations is set to empty array.
      # @param [DDDTopLevel] parent
      # @param [String] environment
      def initialize(parent, environment)
        @parent = parent
        @environment = environment
        @relations = []
      end

      # returns all of the attribute values for this object.
      # structure for each relation is called in order to
      # populate all of the ancestral detail. resolve and
      # validate before calling structure.
      # @return [Hash]
      def structure
        {
          environment: @environment,
          domain_name: @domain_name,
          schema_name: @schema_name,
          service_role: @service_role,
          script_bucket: @script_bucket,
          temp_s3_location: @temp_s3_location,
          storage_format: @storage_format,
          source_relation_prefix: @source_relation_prefix,
          etl_job_name: @etl_job_name,
          etl_job_schedule: @etl_job_schedule,
          etl_job_dpu: @etl_job_dpu,
          relations: @relations.map(&:structure)
        }
      end

      # resolves all of the attribute values in the downstream
      # relations.
      def resolve!
        # dpu default
        @etl_job_dpu = @etl_job_dpu ? @etl_job_dpu.to_i : 2
        @relations.map(&:resolve!)
      end

      # hash containing symbol names mapped to regex patterns.
      # the symbol names match the methods that they are used
      # to validate. for example :environment key in this hash
      # correlates to the @environment attribute of this object.
      # @return [Hash]
      def validation_regex
        {
          environment: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: true
          },
          schema_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
          domain_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
          service_role: {
            regex: ConvergDB::ValidationRegex::AWS_GLUE_SERVICE_ROLE,
            mandatory: false
          },
          script_bucket: { regex: /.*/, mandatory: false },
          temp_s3_location: { regex: /.*/, mandatory: false },
          storage_format: { regex: /parquet/, mandatory: false },
          source_relation_prefix: {
            regex: ConvergDB::ValidationRegex::SOURCE_RELATION_PREFIX,
            mandatory: false
          },
          etl_job_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: true
          },
          etl_job_schedule: {
            regex: /.*/,
            mandatory: true
          }
        }
      end

      # performs validation on this object... then
      # validates all of the :relation items.
      def validate
        raise "DPU out of range" unless @etl_job_dpu.between?(2,100)
        validate_string_attributes
        @relations.each(&:validate)
      end
    end

    # represents a relation attached to an AWA athena deployment.
    class AWSAthenaRelation < ConvergDB::BaseStructure
      # sourced from DSD
      # attr_accessor :partitions
      # attr_accessor :attributes

      # dsd name db.schema.relation
      attr_accessor :dsd

      # hash of symbols indicating generator types to use [:athena, :glue]
      # attr_accessor :implementation

      # full relation name with all overrides applied
      # attr_accessor :full_relation_name

      # sourced from parent
      attr_accessor :environment

      # overrides to be applied to :dsd name
      attr_accessor :domain_name
      attr_accessor :schema_name
      attr_accessor :relation_name

      # derived by this object
      attr_accessor :full_relation_name

      # sourced from parent
      attr_accessor :region

      # overrides from parent otherwise sourced
      attr_accessor :service_role
      attr_accessor :script_bucket
      attr_accessor :temp_s3_location

      # must be specified
      attr_accessor :storage_bucket
      attr_accessor :state_bucket
      attr_accessor :storage_format
      attr_accessor :source_relation_prefix
      attr_accessor :use_inventory

      attr_accessor :etl_job_name
      attr_accessor :etl_job_schedule
      attr_accessor :etl_job_dpu


      # @param [Object] parent
      def initialize(parent)
        @parent = parent
      end

      # all important attributes of this object returned as hash
      # @return [Hash]
      def structure
        {
          generators: [:athena, :glue, :markdown_doc, :html_doc, :control_table],
          full_relation_name: @full_relation_name,
          dsd: @dsd,
          environment: @environment,
          domain_name: @domain_name,
          schema_name: @schema_name,
          relation_name: @relation_name,
          service_role: @service_role,
          script_bucket: @script_bucket,
          temp_s3_location: @temp_s3_location,
          storage_bucket: @storage_bucket,
          state_bucket: @state_bucket,
          storage_format: @storage_format,
          source_relation_prefix: @source_relation_prefix,
          use_inventory: @use_inventory,
          etl_job_name: @etl_job_name,
          etl_job_schedule: @etl_job_schedule,
          etl_job_dpu: @etl_job_dpu
        }
      end

      # provides a structure to be used for comparison to live AWS
      # infrastructure.
      # @return [Hash]
      def comparable
        {
          full_relation_name: @full_relation_name,
          dsd: @dsd,
          storage_bucket: @storage_bucket,
          state_bucket: @state_bucket,
          storage_format: @storage_format,
          etl_job_name: @etl_job_name
        }
      end

      # resolves all of the attributes of this object
      def resolve!
        @environment = @parent.environment
        @domain_name = override_parent(:domain_name)
        @schema_name = override_parent(:schema_name)
        # no resolve for @relation_name
        @full_relation_name = resolve_full_relation_name
        # @region = override_parent(:region)
        @service_role = override_parent(:service_role) || ''
        @script_bucket = override_parent(:script_bucket)
        @temp_s3_location = override_parent(:temp_s3_location)
        @storage_format = override_parent(:storage_format)
        @source_relation_prefix = override_parent(:source_relation_prefix)
        @use_inventory ||= 'false'
        # buckets will default if not specified
        # used in module definition
        unless @script_bucket
          @script_bucket = "${admin_bucket}"
        end

        # used inside a template_file
        unless @state_bucket
          @state_bucket = "${admin_bucket}"
        end

        # used inside a template_file
        unless @storage_bucket
          @storage_bucket = "${data_bucket}/${deployment_id}/#{@full_relation_name}"
        end

        # only ever pulls from parent
        @etl_job_name = @parent.etl_job_name
        @etl_job_schedule = @parent.etl_job_schedule
        @etl_job_dpu = @parent.etl_job_dpu
      end

      # full_relation_name is created by overriding the attributes
      # of the dsd with locally specified attributes.
      def resolve_full_relation_name
        raise 'dsd must be defined' unless @dsd
        f = []
        f[0] = @environment
        f[1] = @domain_name || @dsd.split('.')[0]
        f[2] = @schema_name || @dsd.split('.')[1]
        f[3] = @relation_name || @dsd.split('.')[2]
        f.join('.').downcase
      end

      # hash containing symbol names mapped to regex patterns.
      # the symbol names match the methods that they are used
      # to validate. for example :environment key in this hash
      # correlates to the @environment attribute of this object.
      # @return [Hash]
      def validation_regex
        {
          dsd: {
            regex: ConvergDB::ValidationRegex::DSD_RELATION_NAME,
            mandatory: true
          },
          environment: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: true
          },
          domain_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
          schema_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
          relation_name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: false
          },
          full_relation_name: {
            regex: ConvergDB::ValidationRegex::FULL_RELATION_NAME,
            mandatory: true
          },
          service_role: {
            regex: ConvergDB::ValidationRegex::AWS_GLUE_SERVICE_ROLE,
            mandatory: false
          },

          script_bucket: { regex: /^.*$/, mandatory: false },
          temp_s3_location: { regex: /^.*$/, mandatory: false },
          storage_bucket: { regex: /^.*$/, mandatory: false },
          state_bucket: { regex: /^.*$/, mandatory: false },

          storage_format: { regex: /^[a-zA-Z]+\w*$/, mandatory: true },
          source_relation_prefix: {
            regex: ConvergDB::ValidationRegex::SOURCE_RELATION_PREFIX,
            mandatory: false
          },
          use_inventory: {
            regex: ConvergDB::ValidationRegex::BOOLEAN_VALUE,
            mandatory: false
          }
        }
      end

      # performs the validation for this object.
      # each attribute name (as a symbol) is iterated through
      # the valid_string_match? function to determine if
      # the value is acceptable. for this object... all
      # of the tested attributes are mandatory.
      def validate
        validate_string_attributes
      end
    end

    # used to create intermediate representation of the DSD.
    # designed to be utilized by the output of the parser ast.
    class DDDIRBuilder
      # top level Domains object
      attr_accessor :top_level

      # @return [Domain] currently scoped domain object
      attr_accessor :current_deployment

      # @return [Schema] currently scoped schema object
      attr_accessor :current_relation

      # set top_level to new Domains object
      def initialize
        @top_level = DDDTopLevel.new
      end

      # constants to represent state levels during factory operation
      module States
        # within a deployment
        DEPLOYMENT = 0

        # inside a relation definition
        RELATION = 1
      end

      # array of all state keeping objects
      # their positions in the array
      # @return [Array] objects for various depths of the state
      def states
        [
          @current_deployment,
          @current_relation
        ]
      end

      # clears all state below specified level.
      # for example: level == DEPLOYMENT will clear
      # current_relation.
      # @param [Fixnum] level state level to clear below
      def clear_state_below(level)
        case level
        when States::DEPLOYMENT then @current_relation = nil
        end
      end

      # insures that all state down to specified level
      # has a valid reference.
      # @param[States] level as indicated by constants in state module
      def state_depth_must_be(level)
        states[0..level].each do |a|
          raise 'factory state error' if a.nil?
        end
      end

      # return [Fixnum] current index of deepest non-nil state
      def current_state_depth
        states.each_with_index do |s, i|
          return i - 1 if s == nil
        end
        states.length - 1
      end

      # creates a new deployment.
      # clears all state below deployment.
      # insures that the specified deployment is in scope.
      # @param [String] type
      # @param [String] environment
      def deployment(type, environment)
        n = case type.to_sym
        when :athena then AWSAthena.new(@top_level, environment)
        when :s3_source then AWSS3Source.new(@top_level, environment)
        end
        top_level.deployment << n
        @current_deployment = n
        clear_state_below(States::DEPLOYMENT)
      end

      # creates relation if not exists.
      # clears all state below deployment.
      # insures that the new relation is in scope.
      def relation
        state_depth_must_be(States::DEPLOYMENT)
        n = case
        when @current_deployment.class == AWSAthena
          then n = AWSAthenaRelation.new(@current_deployment)
        when @current_deployment.class == AWSS3Source
          then n = AWSS3SourceRelation.new(@current_deployment)
        end
        @current_deployment.relations << n
        @current_relation = n
      end

      # sets an attribute for the current scoped object
      # @param [String] attr_name name of the attribute to set
      # @param [Object] attr_val value to assign to this attribute
      def attribute(state, attr_name, attr_val)
        clear_state_below(States::DEPLOYMENT) if state.to_sym == :deployment
        o = states[current_state_depth]
        if !o.send(attr_name.to_sym).nil?
          raise "attribute #{attr_name} already defined"
        end
        a = "#{attr_name}="
        o.public_send(a.to_sym, attr_val)
      end
    end
  end
end
