require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

require 'minitest'
require 'minitest/autorun'
require 'json'

require_relative '../lib/ir/dsd/dsd_ir.rb'

module ConvergDB
  module DSD
    class BaseDSDIRTest < Minitest::Test
      def raises_error?(object, method)
        is_err = false
        begin
          object.public_send(method.to_sym)
        rescue => e
          #puts e.message
          is_err = true
        end
        is_err
      end

      def catch_error
        yield
      rescue => e
        return e
      end
      
      def tree_down_to(depth)
        h = {}
        h[:domains] = Domains.new
        return h if depth == :domains

        h[:domain] = Domain.new(h[:domains], 'test_domain')
        h[:domains].domains << h[:domain]
        return h if depth == :domain

        h[:schema] = Schema.new(h[:domain], 'test_schema')
        h[:domain].schemas << h[:schema]
        return h if depth == :schema

        h[:relation] = Relation.new(h[:schema], 'test_relation')
        h[:schema].relations << h[:relation]
        return h if depth == :relation

        add_test_attributes_to_relation(h[:relation])
        return h if depth == :attributes
      end

      def add_test_attributes_to_relation(relation)
        attr1 = attr2 = attr3 = attr4 = nil
        [
          { obj: attr1, name: 'attr1', required: false, data_type: 'bigint', expression: 'src.attr1', field_type: RelationAttributeFieldTypes::DIMENSION},
          { obj: attr2, name: 'attr2', required: false, data_type: 'varchar(100)', expression: "src.attr1 || '_hello'", field_type: RelationAttributeFieldTypes::DIMENSION},
          { obj: attr3, name: 'attr3', required: false, data_type: 'timestamp', expression: 'sysdate', field_type: RelationAttributeFieldTypes::DIMENSION},
          { obj: attr4, name: 'attr4', required: false, data_type: 'float', expression: 'src.attr4 * 0.5', field_type: RelationAttributeFieldTypes::MEASURE}
        ].each do |a|
          a[:obj] = RelationAttribute.new(relation, a[:name])
          a[:obj].data_type = a[:data_type]
          a[:obj].required = a[:required]
          a[:obj].expression = a[:expression]
          a[:obj].field_type = a[:field_type]
          relation.attributes << a[:obj]
        end
      end

      def test_relation_with_attributes_structure
        {
          dsd_name: 'test_domain.test_schema.test_relation',
          source_dsd_name: nil,
          relation_name: 'test_relation',
          relation_type: nil,
          partitions: [],
          attributes: [
            {name: 'attr1', required: false, data_type: 'bigint', expression: 'src.attr1', field_type: RelationAttributeFieldTypes::DIMENSION},
            {name: 'attr2', required: false, data_type: 'varchar(100)', expression: "src.attr1 || '_hello'", field_type: RelationAttributeFieldTypes::DIMENSION},
            {name: 'attr3', required: false, data_type: 'timestamp', expression: 'sysdate', field_type: RelationAttributeFieldTypes::DIMENSION},
            {name: 'attr4', required: false, data_type: 'float', expression: 'src.attr4 * 0.5', field_type: RelationAttributeFieldTypes::MEASURE}
          ]
        }
      end
    end

    class TestDomains < BaseDSDIRTest
      # tests domains top level object
      def test_initialize
        t = tree_down_to(:domains)

        # confirm that domains attribute is initialized to empty array
        assert_equal(
          t[:domains].domains,
          []
        )
      end

      def test_resolve!
        t = tree_down_to(:domains)
        t[:domains].resolve!

        # no change for top level without domains
        assert_equal(
          [],
          t[:domains].structure
        )
      end

      def test_structure
        t = tree_down_to(:domains)

        assert_equal(
          [],
          t[:domains].structure,
          'initialized structure resolution should return an empty array'
        )

        t = tree_down_to(:domain)
        assert_equal(
          [{ domain_name: 'test_domain', schemas: [] }],
          t[:domains].structure
        )
      end

      def test_validate
        t = tree_down_to(:domain)

        assert_equal(
          false,
          raises_error?(t[:domains], :validate)
        )

        # test for invalid attribute
        t[:domain].name = 'in valid'
        assert_equal(
          true,
          raises_error?(t[:domains], :validate)
        )
      end
    end

    class TestDomain < BaseDSDIRTest
      def test_initialize
        t = tree_down_to(:domain)

        # confirm parent is assigned correctly
        assert_equal(
          t[:domains].object_id,
          t[:domain].parent.object_id
        )

        # confirm name set correctly
        assert_equal(
          'test_domain',
          t[:domain].name
        )

        # confirm schemas initialized
        assert_equal(
          [],
          t[:domain].schemas
        )
      end

      def test_resolve!
        t = tree_down_to(:schema)

        t[:domains].resolve!

        # nothing to resolve down to schema level
        assert_equal(
          [
            {
              domain_name: 'test_domain',
              schemas: [
                {
                  schema_name: 'test_schema',
                  relations: []
                }
              ]
            }
          ],
          t[:domains].structure
        )
      end

      def test_structure
        t = tree_down_to(:domain)

        # structure
        assert_equal(
          { :domain_name => 'test_domain', :schemas => [] },
          t[:domain].structure
        )
      end

      def test_validate
        t = tree_down_to(:domain)

        assert_equal(
          false,
          raises_error?(t[:domain], :validate)
        )

        # test for invalid attribute
        t[:domain].name = 'in valid'
        assert_equal(
          true,
          raises_error?(t[:domain], :validate)
        )
      end

      def test_validation_regex
        t = tree_down_to(:domain)

        # valid domain names
        %w{valid VALID Valid123 valid_123}.each do |name|
          assert_equal(
            true,
            name.match(t[:domain].validation_regex[:name][:regex]) ? true : nil
          )
        end

        # invalid domain names
        [
          'in valid',
          '113',
          '_abc',
          '$abc',
          ' space',
          'space '
        ].each do |name|
          assert_equal(
            false,
            name.match(t[:domain].validation_regex[:name][:regex]) ? nil : false
          )
        end
      end
    end

    class TestSchema < BaseDSDIRTest
      def test_initialize
        t = tree_down_to(:schema)

        # confirm parent is assigned correctly
        assert_equal(
          t[:domain].object_id,
          t[:schema].parent.object_id
        )

        # confirm relations initialized
        assert_equal(
          [],
          t[:schema].relations
        )
      end

      def test_resolve!
        t = tree_down_to(:schema)
        t[:schema].resolve!

        # initialized structure nothing to resolve
        assert_equal(
          {
            schema_name: 'test_schema',
            relations: []
          },
          t[:schema].structure
        )

        t = tree_down_to(:relation)
        t[:schema].resolve!

        # relation_type is resolved
        assert_equal(
          {
            schema_name: 'test_schema',
            relations: [
              {
                dsd_name: 'test_domain.test_schema.test_relation',
                source_dsd_name: nil,
                relation_name: 'test_relation',
                relation_type: RelationTypes::BASE,
                partitions: [],
                attributes: []
              }
            ]
          },
          t[:schema].structure
        )
      end

      def test_structure
        t = tree_down_to(:schema)

        # structure
        assert_equal(
          {:schema_name => 'test_schema', :relations => []},
          t[:schema].structure
        )

        # resolve down to test_relation
        t = tree_down_to(:relation)
        assert_equal(
          {
            schema_name: 'test_schema',
            relations: [
              {
                dsd_name: 'test_domain.test_schema.test_relation',
                source_dsd_name: nil,
                relation_name: 'test_relation',
                relation_type: nil,
                partitions: [],
                attributes: []
              }
            ]
          },
          t[:schema].structure
        )
      end

      def test_validate
        t = tree_down_to(:schema)

        # test for valid structure
        assert_equal(
          false,
          raises_error?(t[:domain], :validate)
        )

        # test for invalid attribute
        t[:schema].name = 'in valid'
        assert_equal(
          true,
          raises_error?(t[:domain], :validate)
        )
      end

      def test_validation_regex
        t = tree_down_to(:schema)

        # valid schema names
        %w{valid VALID Valid123 valid_123}.each do |name|
          assert_equal(
            true,
            name.match(t[:schema].validation_regex[:name][:regex]) ? true : nil
          )
        end

        # invalid domain names
        [
          'in valid',
          '113',
          '_abc',
          '$abc',
          ' space',
          'space '
        ].each do |name|
          assert_equal(
            false,
            name.match(t[:schema].validation_regex[:name][:regex]) ? nil : false
          )
        end
      end
    end

    class TestRelation< BaseDSDIRTest
      def test_initialize
        t = tree_down_to(:relation)

        # confirm parent is assigned correctly
        assert_equal(
          t[:schema].object_id,
          t[:relation].parent.object_id
        )

        # confirm relations initialized
        assert_equal(
          [],
          t[:relation].attributes
        )
      end

      def test_resolve!
        t = tree_down_to(:relation)

        # resolves relation_type
        t[:relation].resolve!
        # structure
        assert_equal(
          {
            dsd_name: 'test_domain.test_schema.test_relation',
            source_dsd_name: nil,
            relation_name: 'test_relation',
            relation_type: RelationTypes::BASE,
            partitions: [],
            attributes: []
          },
          t[:relation].structure
        )
      end

      def test_structure
        t = tree_down_to(:relation)

        # unresolved structure has empty/nil attributes
        assert_equal(
          {
            dsd_name: 'test_domain.test_schema.test_relation',
            source_dsd_name: nil,
            relation_name: 'test_relation',
            relation_type: nil,
            partitions: [],
            attributes: []
          },
          t[:relation].structure
        )

        t = tree_down_to(:attributes)
        assert_equal(
          test_relation_with_attributes_structure,
          t[:relation].structure
        )
      end

      def test_validate
        t = tree_down_to(:relation)
        # unresolved relation_type and lack of attributes
        # causes failure in validation.
        assert_equal(
          true,
          raises_error?(t[:relation], :validate)
        )

        # adding attributes and setting relation_type
        # fixes the issue.
        t = tree_down_to(:attributes)
        t[:relation].relation_type = RelationTypes::BASE
        assert_equal(
          false,
          raises_error?(t[:relation], :validate),
          JSON.pretty_generate(t[:relation].structure)
        )
      end

      def test_validate_partition_fields
        r = ConvergDB::DSD::Relation.new(nil, nil)
        
        # single field valid
        t = catch_error do
          r.validate_partition_fields(
            ['field1'],
            ['field1','field2','field3']
          )
        end
        
        assert_equal(
          NilClass,
          t.class
        )

        # single field invalid
        t = catch_error do
          r.validate_partition_fields(
            ['field1'],
            ['field2','field3']
          )
        end
        
        assert_equal(
          RuntimeError,
          t.class
        )
        
        # multi-field valid
        t = catch_error do
          r.validate_partition_fields(
            ['field1', 'field2'],
            ['field1','field2','field3']
          )
        end
        
        assert_equal(
          NilClass,
          t.class
        )
        
        # multi-field invalid
        t = catch_error do
          r.validate_partition_fields(
            ['field1', 'field2'],
            ['field1', 'field3']
          )
        end
        
        assert_equal(
          RuntimeError,
          t.class
        )

        # multi-field with convergdb_batch_id
        t = catch_error do
          r.validate_partition_fields(
            ['convergdb_batch_id','field1'],
            ['field1', 'field3']
          )
        end
        
        assert_equal(
          NilClass,
          t.class
        )
      end

      def test_validation_regex
        t = tree_down_to(:relation)

        # valid relation names
        %w{valid VALID Valid123 valid_123}.each do |name|
          assert_equal(
            true,
            name.match(t[:relation].validation_regex[:name][:regex]) ? true : nil
          )
        end

        # invalid relation names
        [
          'in valid',
          '113',
          '_abc',
          '$abc',
          ' space',
          'space '
        ].each do |name|
          assert_equal(
            false,
            name.match(t[:relation].validation_regex[:name][:regex]) ? nil : false
          )
        end
      end

      def test_attribute_names
        t = tree_down_to(:attributes)
        assert_equal(
          ['attr1', 'attr2', 'attr3', 'attr4'],
          t[:relation].attribute_names
        )
      end

      def test_attributes_are_defined?
        t = tree_down_to(:attributes)
        assert_equal(
          true,
          t[:relation].attributes_are_defined?
        )

        t = tree_down_to(:relation)
        assert_equal(
          false,
          t[:relation].attributes_are_defined?
        )
      end

      def test_attribute_names_valid?
        t = tree_down_to(:attributes)
        assert_equal(
          true,
          t[:relation].attribute_names_valid?
        )

        # check for invalid names (duplicate)
        t[:relation].attributes[2].name = 'attr1'
        assert_equal(
          false,
          t[:relation].attribute_names_valid?
        )
      end

      def test_dsd_name
        t = tree_down_to(:relation)
        assert_equal(
          'test_domain.test_schema.test_relation',
          t[:relation].dsd_name
        )
      end

      def test_source_dsd_name
        t = tree_down_to(:relation)
        t[:relation].relation_type = RelationTypes::BASE
        assert_nil(
          t[:relation].source_dsd_name(t[:relation].dsd_name)
        )

        t = tree_down_to(:relation)
        t[:relation].relation_type = RelationTypes::DERIVED
        t[:relation].relation_source = 'relation2'
        assert_equal(
          'test_domain.test_schema.relation2',
          t[:relation].source_dsd_name(t[:relation].dsd_name)
        )
      end
    end

    class TestRelationAttribute < BaseDSDIRTest
      def test_initialize
        t = tree_down_to(:relation)
        a = RelationAttribute.new(t[:relation], 'test_attribute')

        # confirm parent is assigned correctly
        assert_equal(
          t[:relation].object_id,
          a.parent.object_id
        )

        # confirm name assigned
        assert_equal(
          'test_attribute',
          a.name
        )

        # confirm properties assigned
        assert_equal(
          {},
          a.properties
        )
      end

      def test_resolve!
        # nothing to resolve
        t = tree_down_to(:relation)
        a = RelationAttribute.new(t[:relation], 'test_attribute')
        a.resolve!

        # all attributes unresolved except name
        assert_equal(
          {
            name: 'test_attribute',
            required: false,
            expression: nil,
            data_type: nil,
            field_type: nil
          },
          a.structure
        )
      end

      def test_structure
        t = tree_down_to(:relation)
        a = RelationAttribute.new(t[:relation], 'test_attribute')
        a.required = true
        a.expression = 'expr(field)'
        a.data_type = 'varchar(123)'
        a.field_type = RelationAttributeFieldTypes::MEASURE
        assert_equal(
          {
            name: 'test_attribute',
            required: true,
            expression: 'expr(field)',
            data_type: 'varchar(123)',
            field_type: 0
          },
          a.structure
        )
      end

      def test_validate
        t = tree_down_to(:relation)
        a = RelationAttribute.new(t[:relation], 'test_attribute')
        a.required = true
        a.expression = 'expr(field)'
        a.data_type = 'varchar(123)'
        a.field_type = RelationAttributeFieldTypes::MEASURE

        assert_equal(
          false,
          raises_error?(a, :validate)
        )

        a.required = true
        a.expression = 'expr(field)'
        a.data_type = 'nvarchar(22)' # erroneous
        a.field_type = RelationAttributeFieldTypes::MEASURE

        assert_equal(
          true,
          raises_error?(a, :validate)
        )
      end

      def test_validation_regex
        t = tree_down_to(:relation)
        a = RelationAttribute.new(t[:relation], 'test_attribute')

        # tests for expressions are not included because
        # regex is too wide open
        [
          { regex: :name, value: 'valid', valid: true },
          { regex: :name, value: 'in valid', valid: false },

          { regex: :data_type, value: 'time', valid: true },
          { regex: :data_type, value: 'date', valid: true },
          { regex: :data_type, value: 'timestamptz', valid: true },
          { regex: :data_type, value: 'varchar(100)', valid: true },
          { regex: :data_type, value: 'varchar( 100 )', valid: true },
          { regex: :data_type, value: 'byte', valid: true },
          { regex: :data_type, value: 'word', valid: true },
          { regex: :data_type, value: 'integer', valid: true },
          { regex: :data_type, value: 'bigint', valid: true },
          { regex: :data_type, value: 'float', valid: true },
          { regex: :data_type, value: 'numeric(10,2)', valid: true },
          { regex: :data_type, value: 'numeric( 10 , 2 )', valid: true },
          { regex: :data_type, value: 'boolean', valid: true },
          { regex: :data_type, value: 'datetime', valid: false },
          { regex: :data_type, value: 'varchar()', valid: false },
          { regex: :data_type, value: 'varchar( 100a )', valid: false },
          { regex: :data_type, value: 'numeric(10,b)', valid: false },
          { regex: :data_type, value: 'nvarchar', valid: false },
        ].each do |g|
          assert_equal(
            g[:valid],
            g[:value].match(a.validation_regex[g[:regex]][:regex]) ? true : false,
            "#{g[:value]} should be #{g[:valid]} from match #{g[:regex]}"
          )
        end
      end

      def test_validate_properties
        t = tree_down_to(:relation)
        a = RelationAttribute.new(t[:relation], 'test_attribute')

        a.properties[:label] = 'item'
        a.properties[:defaulte_aggregate] = 'max'
        assert_equal(
          false,
          raises_error?(a, :validate_properties)
        )

        a.properties[:label] = '1tem'
        a.properties[:default_aggregate] = 'max'
        assert_equal(
          true,
          raises_error?(a, :validate_properties)
        )

        a.properties[:label] = 'item'
        a.properties[:default_aggregate] = 'stdev'
        assert_equal(
          true,
          raises_error?(a, :validate_properties)
        )
      end
    end

    class TestDSDIRBuilder < Minitest::Test
      def raises_error?(&block)
        ret = false
        begin
          block.call
        rescue => e
          ret = true
        end
        ret
      end

      def factory_down_to(depth)
        f = DSDIRBuilder.new
        return f if depth == :top_level

        f.domain('ns1')
        return f if depth == :domain

        f.schema('schema1')
        return f if depth == :schema

        f.relation('relation1')
        return f if depth == :relation

        f.attribute('attribute1')
        return f if depth == :attribute

        f.property('label', 'item')
        return f if depth == :property
      end

      def test_initialize
        f = factory_down_to(:top_level)

        assert_equal(
          Domains,
          f.top_level.class
        )
      end

      def test_attribute
        f = factory_down_to(:relation)

        f.attribute('new')
        assert_equal(
          true,
          f.current_relation.attributes.map(&:name).include?('new')
        )
      end

      def test_clear_state_below
        f = factory_down_to(:property)
        f.clear_state_below(DSDIRBuilder::States::DOMAIN)
        assert_equal(
          [
            f.current_domain,
            nil,
            nil,
            nil,
            nil
          ],
          f.states
        )

        f = factory_down_to(:property)
        f.clear_state_below(DSDIRBuilder::States::SCHEMA)
        assert_equal(
          [
            f.current_domain,
            f.current_schema,
            nil,
            nil,
            nil
          ],
          f.states
        )

        f = factory_down_to(:property)
        f.clear_state_below(DSDIRBuilder::States::RELATION)
        assert_equal(
          [
            f.current_domain,
            f.current_schema,
            f.current_relation,
            nil,
            nil
          ],
          f.states
        )

        f = factory_down_to(:attribute)
        f.clear_state_below(DSDIRBuilder::States::RELATION)
        assert_equal(
          [
            f.current_domain,
            f.current_schema,
            f.current_relation,
            f.current_attribute,
            nil
          ],
          f.states
        )

        f = factory_down_to(:property)
        f.clear_state_below(DSDIRBuilder::States::PROPERTY)
        assert_equal(
          [
            f.current_domain,
            f.current_schema,
            f.current_relation,
            f.current_attribute,
            f.current_property
          ],
          f.states
        )
      end

      def test_data_type
        f = factory_down_to(:attribute)
        assert_nil(
          f.current_attribute.data_type
        )

        f.data_type('varchar(100)')
        assert_equal(
          'varchar(100)',
          f.current_attribute.data_type
        )
      end

      def test_domain
        f = factory_down_to(:top_level)
        assert_equal(
          true,
          f.top_level.domains == []
        )

        f.domain('ns2')
        assert_equal(
          true,
          f.top_level.domains.map(&:name).include?('ns2')
        )
      end

      def test_expression
        f = factory_down_to(:attribute)
        assert_nil(
          f.current_attribute.expression
        )

        f.expression('greatest(a,0)')
        assert_equal(
          'greatest(a,0)',
          f.current_attribute.expression
        )
      end

      def test_field_type
        f = factory_down_to(:attribute)
        assert_nil(
          f.current_attribute.field_type
        )

        f.field_type('dimension')
        assert_equal(
          RelationAttributeFieldTypes::DIMENSION,
          f.current_attribute.field_type
        )
      end

      def test_partition
        f = factory_down_to(:relation)
        assert_equal(
          [],
          f.current_relation.partitions
        )

        f.partition('attr1')
        f.partition('attr2')
        f.partition('attr3')
        assert_equal(
          ['attr1', 'attr2', 'attr3'],
          f.current_relation.partitions
        )
      end

      def test_property
        f = factory_down_to(:attribute)
        assert_equal(
          {},
          f.current_attribute.properties
        )

        f.property('label', 'item')
        assert_equal(
          { label: 'item' },
          f.current_attribute.properties
        )
      end

      def test_relation
        f = factory_down_to(:schema)
        assert_equal(
          [],
          f.current_schema.relations
        )

        f.relation('relation2')
        assert_equal(
          true,
          f.current_schema.relations.map(&:name).include?('relation2')
        )
      end

      def test_relation_type
        f = factory_down_to(:relation)
        assert_nil(
          f.current_relation.relation_type
        )

        f.relation_type('derived')
        assert_equal(
          RelationTypes::DERIVED,
          f.current_relation.relation_type
        )
      end

      def test_relation_source
        f = factory_down_to(:relation)

        f.relation_source('base_relation')
        assert_equal(
          'base_relation',
          f.current_relation.relation_source
        )
      end

      def test_required
        f = factory_down_to(:attribute)
        assert_nil(
          f.current_attribute.required
        )

        f.required('true')
        assert_equal(
          true,
          f.current_attribute.required
        )
      end

      def test_schema
        f = factory_down_to(:domain)
        assert_equal(
          [],
          f.current_domain.schemas
        )

        f.schema('schema2')
        assert_equal(
          true,
          f.current_domain.schemas.map(&:name).include?('schema2')
        )
      end

      def test_state_depth_must_be
        # note that property state isn't really used as it relates to current attribute
        # need to deprecate and remove?
        f = factory_down_to(:property)
        [
          { expected: false, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::DOMAIN) }},
          { expected: false, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::SCHEMA) }},
          { expected: false, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::RELATION) }},
          { expected: false, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::ATTRIBUTE) }},
          { expected: true, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::PROPERTY) }},
        ].each do |t|
          assert_equal(
            t[:expected],
            t[:actual]
          )
        end

        f.clear_state_below(DSDIRBuilder::States::DOMAIN)
        [
          { expected: false, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::DOMAIN) }},
          { expected: true, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::SCHEMA) }},
          { expected: true, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::RELATION) }},
          { expected: true, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::ATTRIBUTE) }},
          { expected: true, actual: raises_error? { f.state_depth_must_be(DSDIRBuilder::States::PROPERTY) }},
        ].each do |t|
          assert_equal(
            t[:expected],
            t[:actual]
          )
        end
      end

      def test_states
        f = factory_down_to(:attribute)
        assert_equal(
          [
            Domain,
            Schema,
            Relation,
            RelationAttribute,
            NilClass
          ],
          f.states.map(&:class)
        )
      end
    end
  end
end
