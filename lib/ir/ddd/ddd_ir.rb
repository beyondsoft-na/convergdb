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
        apply_env_vars_to_attributes!(
          [
            :environment,
            :domain_name,
            :schema_name
          ]
        )
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

      # logical link to alreadye existing s3 inventory table
      attr_accessor :inventory_table

      # whether or not to create a streaming inventory table
      attr_accessor :streaming_inventory

      # bucket location for storing inventory files
      # defaults to admin_bucket
      attr_accessor :streaming_inventory_output_bucket

      attr_accessor :streaming_inventory_table
      
      # csv parameters
      attr_accessor :csv_header
      attr_accessor :csv_separator
      attr_accessor :csv_quote
      attr_accessor :csv_null 
      attr_accessor :csv_escape
      attr_accessor :csv_trim
    
      # @param [Object] parent
      def initialize(parent)
        @parent = parent
      end

      # @return [Hash]
      def structure
        {
          generators: [
            :streaming_inventory,
            :s3_source,
            :markdown_doc,
            :html_doc],
          dsd: @dsd,
          full_relation_name: @full_relation_name,
          environment: @environment,
          domain_name: @domain_name,
          schema_name: @schema_name,
          relation_name: @relation_name,
          storage_bucket: @storage_bucket,
          storage_format: @storage_format,
          inventory_table: @inventory_table,
          streaming_inventory: @streaming_inventory,
          streaming_inventory_output_bucket: @streaming_inventory_output_bucket,
          streaming_inventory_table: @streaming_inventory_table,
          csv_header: @csv_header,
          csv_separator: @csv_separator,
          csv_quote: @csv_quote,
          csv_null: @csv_null,
          csv_escape: @csv_escape,
          csv_trim: @csv_trim
        }
      end

      # resolves all of the attributes of this object
      def resolve!
        apply_env_vars_to_attributes!(
          [
            :domain_name,
            :schema_name,
            :relation_name,
            :storage_bucket,
            :inventory_table,
            :streaming_inventory_output_bucket,
            :streaming_inventory_table
          ]
        )
        # @region = @parent.region
        @environment = @parent.environment
        @domain_name = override_parent(:domain_name)
        @schema_name = override_parent(:schema_name)
        # no resolve for @relation_name
        @full_relation_name = resolve_full_relation_name
        @inventory_table ||= ''
        @streaming_inventory ||= 'false'
        
        unless @streaming_inventory_output_bucket
          if @streaming_inventory != 'false'
            if @storage_bucket.class == String
              bucket = '${var.admin_bucket}'
              prefix = '${var.deployment_id}/streaming_inventory'
              sb = @storage_bucket.split('/')[0]
              siob = "#{bucket}/#{prefix}/#{sb}/"
              @streaming_inventory_output_bucket = siob
            else
              raise "must define a storage_bucket for #{full_relation_name}"
            end
          end
        end
        
        unless @streaming_inventory_table
          if @streaming_inventory == 'true'
            if @storage_bucket
              database = "convergdb_inventory_${deployment_id}"
              a = @storage_bucket.split('/')[0].gsub(/(\.|\-)/, '__')
              @streaming_inventory_table = "#{database}.#{a}"
            else
              raise "issue"
            end
          end
        end
        
        resolve_csv_parameters!
      end
      
      def csv_params
        [
          @csv_header,
          @csv_separator,
          @csv_quote,
          @csv_null,
          @csv_escape,
          @csv_trim
        ]
      end
      
      def resolve_csv_parameters!
        if @storage_format == 'csv'
          puts("resolving csv parameters")
          @csv_header ||= 'false'
          @csv_trim ||= 'false'
          @csv_separator = @csv_separator ? csv_param(@csv_separator) : ','.ord
          @csv_quote = @csv_quote ? csv_param(@csv_quote) : '"'.ord
          @csv_null = @csv_null ? csv_param(@csv_null) : 0
          @csv_escape = @csv_escape ? csv_param(@csv_escape) : csv_param(':backslash')
        else
          if csv_params.select { |a| a != nil }.length > 0
            raise "csv parameters can only be applied for storage_format = csv"
          end
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
          storage_bucket: { regex: /.*/, mandatory: true },
          storage_format: { regex: /(json|csv)/, mandatory: false },
          streaming_inventory: { regex: /^(true|false)$/i, mandatory: false }
#          csv_header: { regex: /^(true|false)$/i, mandatory: false },
#          csv_separator: { regex: /^(\d{1,3}|u\d{4})$/i, mandatory: false },
#          csv_quote: { regex: /^(\d{1,3}|u\d{4})$/i, mandatory: false },
#          csv_null: { regex: /^(\d{1,3}|u\d{4})$/i, mandatory: false },
#          csv_escape: { regex: /^(\d{1,3}|u\d{4})$/i, mandatory: false },
#          csv_trim: { regex: /^(true|false)$/i, mandatory: false }
        }
      end

      # performs validation on this object.
      def validate
        validate_string_attributes
      end
      
      # @param [String] value
      # @return [String|Integer]
      def csv_param(value)
        if value.match(/^.{1}$/)
          # single character ascii value
          return value.ord
        elsif value.match(/^\:/)
          case
            when value.strip.downcase == ':colon'
            then return 58
            when value.match(/^:tab$/)
            then return 11
            when value.match(/^:backslash$/)
            then return 92
            when value.match(/^:newline$/)
            then return 10
            when value.match(/^:quote$/)
            then return 34
            # ascii
            when value.match(/^:\d{1,3}$/)
            then 
              ret = value.match(/\d+/)[0].to_i
              raise "ascii value must be less than 255" if ret > 255
              return ret
            # unicode
            when value.match(/^:u([abcdef]|\d){4}$/i)
            then return "u#{value.match(/([abcdef]|\d){4}/i)[0]}"
            # otherwise it's invalid
            else raise "invalid csv parameter #{value}"
          end
        else
          raise "invalid csv parameter #{value}"
        end
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

      # fargate container handling
      attr_accessor :etl_technology
      attr_accessor :etl_docker_image
      attr_accessor :etl_docker_image_digest

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
          etl_technology: @etl_technology,
          etl_docker_image: @etl_docker_image,
          etl_docker_image_digest: @etl_docker_image_digest,
          relations: @relations.map(&:structure)
        }
      end

      # resolves all of the attribute values in the downstream
      # relations.
      def resolve!
        apply_env_vars_to_attributes!(
          [
            :environment,
            :domain_name,
            :schema_name,
            :service_role,
            :script_bucket,
            :temp_s3_location,
            :source_relation_prefix,
            :etl_job_name
          ]
        )
        @script_bucket ||= '${var.admin_bucket}'
        # dpu and image defaults
        @etl_technology ||= 'aws_glue'
        if @etl_technology == 'aws_fargate'
          # if no image is specified... use the dockerhub image
          unless @etl_docker_image
            @etl_docker_image = ConvergDB::DOCKERHUB_IMAGE_NAME
            @etl_docker_image_digest ||= ConvergDB::DOCKERHUB_IMAGE_SHA256
          end
        elsif @etl_technology == 'aws_glue'
          @etl_job_dpu = @etl_job_dpu ? @etl_job_dpu.to_i : 2
        end
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
          },
          etl_technology: {
            regex: /(aws\_glue|aws\_fargate)/,
            mandatory: true
          }
        }
      end

      # performs validation on this object... then
      # validates all of the :relation items.
      def validate
        validate_string_attributes
        validate_etl(
          @etl_technology,
          @etl_docker_image,
          @etl_docker_image_digest,
          @etl_job_dpu
        )
        @relations.each(&:validate)
      end

      # validates the etl job related attributes for this object.
      # @param [String] technology
      # @param [String] docker_image
      # @param [String] docker_image_digest
      # @param [String] job_dpu
      def validate_etl(technology, docker_image, docker_image_digest, job_dpu)
        if technology == 'aws_fargate'
          unless docker_image_digest
            raise "you must specify an SHA256 digest for the docker image"
          end
          raise "DPU does not apply to AWS Fargate based ETL jobs" if job_dpu
        elsif technology == 'aws_glue'
          if docker_image
            raise "image doesn't apply to Glue based ETL jobs"
          end
          if docker_image_digest
            raise "image digest doesn't apply to Glue based ETL jobs"
          end
          raise "DPU out of range" unless job_dpu.between?(2,100)
        end
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
      # deprecated
      attr_accessor :use_inventory
      # api(default), streaming, s3
      attr_accessor :inventory_source

      attr_accessor :etl_job_name
      attr_accessor :etl_job_schedule
      attr_accessor :etl_job_dpu

      # fargate container handling
      attr_accessor :etl_technology
      attr_accessor :etl_docker_image
      attr_accessor :etl_docker_image_digest

      # @param [Object] parent
      def initialize(parent)
        @parent = parent
      end

      # all important attributes of this object returned as hash
      # @return [Hash]
      def structure
        {
          generators: [:athena, :glue, :fargate, :markdown_doc, :html_doc, :control_table],
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
          inventory_source: @inventory_source,
          use_inventory: @use_inventory,
          etl_job_name: @etl_job_name,
          etl_job_schedule: @etl_job_schedule,
          etl_job_dpu: @etl_job_dpu,
          etl_technology: @etl_technology,
          etl_docker_image: @etl_docker_image,
          etl_docker_image_digest: @etl_docker_image_digest
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
        apply_env_vars_to_attributes!(
          [
            :domain_name,
            :schema_name,
            :relation_name,
            :service_role,
            :script_bucket,
            :temp_s3_location,
            :source_relation_prefix,
            :etl_job_name
          ]
        )
        @environment = @parent.environment
        @domain_name = override_parent(:domain_name)
        @schema_name = override_parent(:schema_name)
        # no resolve for @relation_name
        @full_relation_name = resolve_full_relation_name
        # @region = override_parent(:region)
        @service_role = override_parent(:service_role)
        @script_bucket = override_parent(:script_bucket)
        @temp_s3_location = override_parent(:temp_s3_location)
        @storage_format = override_parent(:storage_format)
        @source_relation_prefix = override_parent(:source_relation_prefix)

        if @use_inventory
          puts('athena stanza use_inventory attribute is deprecated... please use inventory_source = "s3"')
        end
        @use_inventory ||= 'false'

        unless @inventory_source
          if @use_inventory == 'true'
            @inventory_source = 's3'
          end
        end

        # this is the attribute used downstream
        @inventory_source ||= 'default'

        # buckets will default if not specified
        # used in module definition
        @script_bucket ||= "${admin_bucket}"

        # used inside a template_file
        @state_bucket ||= "${admin_bucket}"

        # used inside a template_file
        @storage_bucket ||= "${data_bucket}/${deployment_id}/#{@full_relation_name}"

        # only ever pulls from parent
        @etl_job_name = @parent.etl_job_name
        @etl_job_schedule = @parent.etl_job_schedule
        @etl_job_dpu = @parent.etl_job_dpu

        @etl_technology = @parent.etl_technology
        @etl_docker_image = @parent.etl_docker_image
        @etl_docker_image_digest = @parent.etl_docker_image_digest
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
          },
          inventory_source: {
            regex: /(s3|streaming|api|default)/i,
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
