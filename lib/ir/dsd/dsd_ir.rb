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

# top level namespace
module ConvergDB
  # used for DSD intermediate representations
  module DSD
    # used to store all domains in DSD
    class Domains < BaseStructure
      # @return [Array<Domain>]
      attr_accessor :domains

      # creates domains object as array
      def initialize
        @domains = []
      end

      # @return [Array<BaseStructure>] array of domain structures
      def structure
        @domains.map(&:structure)
      end

      # resolves all items in :domains
      def resolve!
        @domains.map(&:resolve!)
      end

      # validates domain classes then validates domains
      def validate
        @domains.each do |d|
          raise 'domain definition error' unless d.class == Domain
        end
        @domains.map(&:validate)
      end
    end

    # holds a DSD domain with array for child
    # schema references.
    class Domain < BaseStructure
      # @return [String]
      attr_accessor :name

      # @return [Array<Schema>]
      attr_accessor :schemas

      # @return [Domain] reference to parent object
      attr_accessor :parent

      # @param [BaseStructure] parent optional parent reference
      # @param [String] name domain name
      def initialize(parent, name)
        @parent = parent
        @name = name
        @schemas = []
      end

      # structure methods on schemas will be called.
      # @return [Hash] domain name and downstream schema structures
      def structure
        {
          domain_name: @name,
          schemas: @schemas.map(&:structure)
        }
      end

      # resolves all items in :schemas
      def resolve!
        @schemas.map(&:resolve!)
      end

      # hash containing symbol names mapped to regex patterns.
      # the symbol names match the methods that they are used
      # to validate. for example :namespace key in this hash
      # correlates to the @namespace attribute of this object.
      # @return [Hash]
      def validation_regex
        {
          name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: true
          }
        }
      end

      # validates domain all items in :schemas
      def validate
        validate_string_attributes
        @schemas.map(&:validate)
      end
    end

    # holds a DSD schema
    class Schema < BaseStructure
      # @return [String]
      attr_accessor :name

      # @return [Array<Relation>]
      attr_accessor :relations

      # # @return [Domain] reference to parent object
      attr_accessor :parent

      # @param [BaseStructure] parent optional parent reference
      # @param [String] name schema name
      def initialize(parent, name)
        @parent = parent
        @name = name
        @relations = []
      end

      # @return [Hash] schema name and downstream relation structures
      def structure
        {
          schema_name: @name,
          relations: @relations.map(&:structure)
        }
      end

      # resolves all items in :relations
      def resolve!
        @relations.map(&:resolve!)
      end

      # hash containing symbol names mapped to regex patterns.
      # the symbol names match the methods that they are used
      # to validate. for example :namespace key in this hash
      # correlates to the @namespace attribute of this object.
      # @return [Hash]
      def validation_regex
        {
          name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: true
          }
        }
      end

      # resolves all items in :relations
      def validate
        validate_string_attributes
        @relations.map(&:validate)
      end
    end

    # all references to relation types should be made
    # through these constants.
    module RelationTypes
      # implies a base relation (no dependencies)
      BASE = 0

      # implies a derived relation (dependencies on other relations)
      DERIVED = 1
    end

    # IR of a relation (table) definition.
    class Relation < BaseStructure
      # @return [String]
      attr_accessor :name

      # @return [Fixnum] is one of DSD::RelationTypes
      attr_accessor :relation_type

      # can be relation schema.relation or domain.schema.relation
      # @return [String] dsd name/prefix
      attr_accessor :relation_source

      # array of attribute names to indicate partitions.
      # left to right ordering indicates precedence
      # @return [Array<String>]
      attr_accessor :partitions

      # @return [Array<RelationAttribute>] array of attributes (fields/columns)
      attr_accessor :attributes

      # reference to parent object (domain)
      # @return [Domain]
      attr_accessor :parent

      # @param [BaseStructure] parent optional parent reference
      # @param [String] name relation name
      def initialize(parent, name)
        @parent = parent
        @name = name
        @attributes = []
        @partitions = []
      end

      # @return [Hash] relation and attribute properties
      def structure
        {
          dsd_name: dsd_name,
          source_dsd_name: source_dsd_name(dsd_name),
          relation_name: @name,
          relation_type: @relation_type,
          partitions: @partitions,
          attributes: @attributes.map(&:structure)
        }
      end

      # evaluates and validates the structure of this object.
      # insures all of the properties have resolved values
      def resolve!
        # default to base relation if not specified
        @relation_type ||= RelationTypes::BASE
        @attributes.map(&:resolve!)
      end

      # @return [String] dsd name with domain and schema resolved
      def dsd_name
        "#{@parent.parent.name}.#{@parent.name}.#{@name}".downcase
      end

      # @param [String] dsd_name
      # @return [String] with domain and schema resolved
      def source_dsd_name(dsd_name)
        if @relation_type == RelationTypes::DERIVED
          d = @relation_source.split('.').reverse
          r = []
          r[0] = d[2] || @parent.parent.name
          r[1] = d[1] || @parent.name
          r[2] = d[0]
          return r.join('.').downcase
        else
          return nil
        end
      end

      # hash containing symbol names mapped to regex patterns.
      # the symbol names match the methods that they are used
      # to validate. for example :namespace key in this hash
      # correlates to the @namespace attribute of this object.
      # @return [Hash]
      def validation_regex
        {
          name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: true
          },
          relation_source:
          {
            regex: ConvergDB::ValidationRegex::DSD_NAME_SCOPED,
            mandatory: false
          }
        }
      end

      # performs validation for this object and descendants
      def validate
        raise 'attributes not defined' unless attributes_are_defined?
        raise 'attribute names invalid' unless attribute_names_valid?
        validate_string_attributes
        @attributes.each(&:validate)
        validate_partition_fields(
          @partitions,
          attribute_names
        )
      end

      # validates that the partition fields specified actually
      # exist as attribute names. the exception is convergdb_batch_id
      # which is available as an exposed partition in any convergdb relation.
      # @param [Array<String>] partition_fields 
      # @param [Array<String>] attribute_names
      def validate_partition_fields(partition_fields, attribute_names)
        partition_fields.select do |partition_field|
          unless attribute_names.insert(-1, 'convergdb_batch_id').include?(partition_field)
            raise "partition field #{partition_field} undefined in relation #{@name}"
          end
        end
        nil
      end

      # return [Array<String>] list of attribute names in order
      def attribute_names
        @attributes.map(&:name)
      end

      # return [Boolean] 0 attributes is unacceptable
      def attributes_are_defined?
        # attributes are defined
        return false if @attributes == []
        @attributes.each do |a|
          return false unless a.class == RelationAttribute
        end
        true
      end

      # attribute names must be unique across the relation
      # return [Boolean]
      def attribute_names_valid?
        if attribute_names.detect { |a| attribute_names.count(a) > 1 }
          return false
        end
        true
      end
    end

    # dictates behaviors of relation attribute
    module RelationAttributeFieldTypes
      # indicates a measure field
      MEASURE = 0

      # indicates a dimension field
      DIMENSION = 1
    end

    # attribute... otherwise known as a column or field
    class RelationAttribute < BaseStructure
      # @return [String]
      attr_accessor :name

      # required... yes if true... nullable if false
      # @return [Boolean]
      attr_accessor :required

      # SQL style expression syntax.
      # @return [String]
      attr_accessor :expression

      # @return [String]
      attr_accessor :data_type

      # one of RelationAttributeFieldTypes
      # @return [RelationAttributeFieldTypes]
      attr_accessor :field_type

      # reference to parent object (relation)
      # @return [Relation]
      attr_accessor :parent

      # relation properties
      # @return [Hash]
      attr_accessor :properties

      # @param [BaseStructure] parent optional parent reference
      # @param [String] name attribute name
      def initialize(parent, name)
        @parent = parent
        @name = name
        @properties = {}
      end

      # evaluates all local values
      # then returns hash of attributes
      # @return [Hash] all downstream fields evaluated as Hash/Array
      def structure
        # create a hash and return it
        h = {
          name: @name,
          required: @required,
          expression: @expression,
          data_type: @data_type,
          field_type: @field_type
        }
        h[:properties] = @properties if @properties.keys.count > 0
        h
      end

      # calculates and resolves attributes of this relation attribute
      def resolve!
        @required = (@required == true)
        if @parent.relation_type == RelationTypes::DERIVED
          @expression = @name unless @expression
        end
      end

      # @return [Hash]
      def validation_regex
        {
          name: {
            regex: ConvergDB::ValidationRegex::SQL_IDENTIFIER,
            mandatory: true
          },
          data_type: { regex: %r{^date$|
            ^time$|
            ^timestamp$|
            ^timestamptz$|
            ^varchar\(\s*\d+\s*\)$|
            ^byte$|
            ^word$|
            ^integer$|
            ^bigint$|
            ^float$|
            ^double$|
            ^numeric\(\s*\d+\s*,\s*\d+\s*\)$|
            ^boolean$}ix, mandatory: true },
          expression: { regex: /.*/, mandatory: false }
        }
      end

      # validates properties of this attribute.
      # raises error if invalid property applied.
      def validate_properties
        if @properties.keys.count > 0
          if @properties.key?(:label)
            unless @properties[:label] =~ /^[a-zA-Z][\w|\s]*$/
              raise 'property label validation error'
            end
          end

          if @properties.key?(:default_aggregate)
            unless @properties[:default_aggregate] =~ /^max$|^min$|^avg$|^count$/i
              raise 'property default_aggregate validation error'
            end
          end
        end
      end

      # validates this attribute.
      def validate
        validate_string_attributes
        validate_properties
      end
    end

    # used to create intermediate representation of the DSD.
    # designed to be utilized by the output of the parser ast.
    class DSDIRBuilder
      # top level Domains object
      attr_accessor :top_level

      # @return [Domain] currently scoped domain object
      attr_accessor :current_domain

      # @return [Schema] currently scoped schema object
      attr_accessor :current_schema

      # @return [Relation] currently scoped relation object
      attr_accessor :current_relation

      # @return [RelationAttribute] currently scoped relation attribute object
      attr_accessor :current_attribute

      # @return [Object] currently scoped property
      attr_accessor :current_property

      # set top_level to new Domains object
      def initialize
        @top_level = Domains.new
      end

      # constants to represent state levels during factory operation
      module States
        # domain
        DOMAIN = 0

        # schema
        SCHEMA = 1

        # relation
        RELATION = 2

        # attribute
        ATTRIBUTE = 3

        # attribute property
        PROPERTY = 4
      end

      # array of all state keeping objects
      # their positions in the array
      # @return [Array] objects for various depths of the state
      def states
        [
          @current_domain,
          @current_schema,
          @current_relation,
          @current_attribute,
          @current_property
        ]
      end

      # clears all state below specified level.
      # for example: level == ATTRIBUTE will clear
      # current_attribute and current_property.
      # @param [Fixnum] level state level to clear below
      def clear_state_below(level)
        return nil if level == States::PROPERTY
        @current_property = nil
        return nil if level == States::ATTRIBUTE
        @current_attribute = nil
        return nil if level == States::RELATION
        @current_relation = nil
        return nil if level == States::SCHEMA
        @current_schema = nil
        return nil if level == States::DOMAIN
      end

      # insures that all state down to specified level
      # has a valid reference.
      # @param[States] level as indicated by constants in state module
      def state_depth_must_be(level)
        states[0..level].each do |a|
          raise 'factory state error' if a.nil?
        end
      end

      # creates domain if not exists.
      # clears all state below domain.
      # insures that the specified domain is in scope.
      # @param [String] domain_name
      def domain(domain_name)
        d = domain_lookup(domain_name)
        if d.nil?
          n = Domain.new(@top_level, domain_name)
          top_level.domains << n
          @current_domain = n
        else
          @current_domain = d
        end
        clear_state_below(States::DOMAIN)
      end

      # creates schema if not exists.
      # clears all state below schema.
      # insures that the specified schema is in scope.
      # @param [String] schema_name
      def schema(schema_name)
        state_depth_must_be(States::DOMAIN)
        s = schema_lookup(schema_name)
        if s.nil?
          n = Schema.new(@current_domain, schema_name)
          @current_domain.schemas << n
          @current_schema = n
        else
          @current_schema = s
        end
        clear_state_below(States::SCHEMA)
      end

      # creates relation if not exists.
      # clears all state below relation.
      # insures that the specified relation is in scope.
      # @param [String] relation_name
      def relation(relation_name)
        state_depth_must_be(States::SCHEMA)
        r = relation_lookup(relation_name)
        raise "relation #{relation_name} already exists" if r
        n = Relation.new(@current_schema, relation_name)
        @current_schema.relations << n
        @current_relation = n
        clear_state_below(States::RELATION)
      end

      # sets type for current relation
      # @param [RelationTypes] relation_type
      def relation_type(relation_type)
        state_depth_must_be(States::RELATION)
        raise 'relation type already defined' if @current_relation.relation_type
        case relation_type.strip.downcase
        when 'base' then
          @current_relation.relation_type = RelationTypes::BASE
        when 'derived' then
          @current_relation.relation_type = RelationTypes::DERIVED
        end
      end

      # sets type for current relation
      # @param [String] relation_source
      def relation_source(relation_source)
        state_depth_must_be(States::RELATION)
        raise 'relation_source already defined' if @current_relation.relation_source
        @current_relation.relation_source = relation_source
      end

      # creates attribute if not exists.
      # clears all attribute below relation.
      # insures that the specified attribute is in scope.
      # @param [String] attribute_name
      def attribute(attribute_name)
        state_depth_must_be(States::RELATION)
        a = attribute_lookup(attribute_name)
        raise "attribute #{attribute_name} already exists" if a
        n = RelationAttribute.new(@current_relation, attribute_name)
        @current_relation.attributes << n
        @current_attribute = n
        clear_state_below(States::ATTRIBUTE)
      end

      # sets expression for in-scope attribute
      # @param [String] expression_string
      def expression(expression_string)
        state_depth_must_be(States::ATTRIBUTE)
        raise 'expression already defined' if @current_attribute.expression
        @current_attribute.expression = expression_string
      end

      # sets data_type for in-scope attribute
      # @param [String] data_type
      def data_type(data_type)
        state_depth_must_be(States::ATTRIBUTE)
        raise 'data_type already defined' if @current_attribute.data_type
        @current_attribute.data_type = data_type
      end

      # sets field_type for in-scope attribute
      # @param [String] field_type
      def field_type(field_type)
        state_depth_must_be(States::ATTRIBUTE)
        raise 'field_type already defined' if @current_attribute.field_type
        case field_type.strip.downcase
        when 'dimension' then
          @current_attribute.field_type = RelationAttributeFieldTypes::DIMENSION
        when 'measure' then
          @current_attribute.field_type = RelationAttributeFieldTypes::MEASURE
        else
          raise 'invalid field type'
        end
      end

      # sets required field for in-scope attribute
      # @param [String] required
      def required(required)
        state_depth_must_be(States::ATTRIBUTE)
        raise 'required already defined' if @current_attribute.required
        required = required.strip.downcase
        raise 'must be boolean' unless %w[true false].include?(required)
        r = (required == 'true')
        @current_attribute.required = r
      end

      # sets property for in-scope attribute
      # @param [String] key property key name
      # @param [String] value property value
      def property(key, value)
        if @current_attribute.properties.key?(key.to_sym)
          raise "attribute #{key} already defined"
        end
        state_depth_must_be(States::ATTRIBUTE)
        @current_attribute.properties[key.to_sym] = value
      end

      # appends partition key to partitions of current relation
      # @param [String] partition_key
      def partition(partition_key)
        state_depth_must_be(States::RELATION)
        if @current_relation.partitions.include?(partition_key)
          raise "duplicate partition key #{partition_key}"
        end
        @current_relation.partitions << partition_key
      end

      private

      # looks up a domain by it's name.
      # returns the domain object if found.
      # @param [String] domain_name
      # @return [Domain]
      def domain_lookup(domain_name)
        d = nil
        @top_level.domains.each do |domain|
          d = domain if domain.name == domain_name
        end
        d
      end

      # looks up a schema by it's name.
      # returns the schema object if found.
      # @param [String] schema_name
      # @return [Schema]
      def schema_lookup(schema_name)
        s = nil
        @current_domain.schemas.each do |schema|
          s = schema if schema.name == schema_name
        end
        s
      end

      # looks up a relation by it's name.
      # returns the relation object if found.
      # @param [String] relation_name
      # @return [Relation]
      def relation_lookup(relation_name)
        r = nil
        @current_schema.relations.each do |relation|
          r = relation if relation.name == relation_name
        end
        r
      end

      # looks up an attribute by it's name.
      # returns the attribute object if found.
      # @param [String] attribute_name
      # @return [RelationAttribute]
      def attribute_lookup(attribute_name)
        a = nil
        @current_relation.attributes.each do |attribute|
          return attribute if attribute.name == attribute_name
        end
        a
      end
    end
  end
end
