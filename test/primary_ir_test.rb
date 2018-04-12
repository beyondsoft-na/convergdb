require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

require 'minitest'
require 'minitest/autorun'
require 'json'
require 'pp'
require 'hashdiff'

require_relative '../lib/ir/primary/primary_ir.rb'
require_relative '../lib/ir/dsd/dsd_ir.rb'

module ConvergDB
  module PrimaryIR
    class PrimaryIRTest < Minitest::Test
      def catch_error
        yield
      rescue => e
        return e
      end

      def test_dir
        File.dirname(File.expand_path(__FILE__))
      end

      def test_dsd_ir
        nil
      end
      
      def ddd_ir_structure
        JSON.parse(
          File.read(
            "#{test_dir}/fixtures/primary_ir/ddd.json"
          ),
          :symbolize_names => true
        )
      end

      def dsd_ir_structure
        JSON.parse(
          File.read(
            "#{test_dir}/fixtures/primary_ir/dsd.json"
          ),
          :symbolize_names => true
        )
      end

      def primary_ir_structure
        tmp = JSON.parse(
          File.read(
            "#{test_dir}/fixtures/primary_ir/ir.json"
          ),
          :symbolize_names => true
        )
        h = {}
        tmp.each_key { |k| h[k.to_s] = tmp[k] }
        h
      end

      def primary_ir
        PrimaryIR.new
      end

      def test_initialize
        t = primary_ir

        assert_equal(
          [],
          t.ir
        )
      end

      def test_integrate!
        t = primary_ir

        t.integrate!(
          dsd_ir_structure,
          ddd_ir_structure
        )

        t.ir.each_key do |k|
          t.ir[k][:working_path] = '/tmp'
        end

        assert_equal(
          primary_ir_structure,
          t.ir,
          pp(
            HashDiff.diff(
              primary_ir_structure,
              t.ir
            )
          )
        )
      end

      def test_integrate_dsd!
        t = primary_ir
        t.integrate_dsd!(dsd_ir_structure)

        # basic check to see if dsd_ir gets set
        assert(t.dsd_ir.nil? == false)

        # not testing the circular check
      end

      def test_dsd_circular_dependency_check
        t = primary_ir

        e = catch_error do
          t.dsd_circular_dependency_check(
            t.dsd_relations_to_hash(
              dsd_ir_structure
            )
          )
        end

        # no cycles detected
        assert_equal(
          false,
          e.is_a?(Exception)
        )

        cycle = t.dsd_relations_to_hash(
          dsd_ir_structure
        )

        # forcing a cycle between the two relations
        cycle['ecommerce.inventory.books_source'][:source_dsd_name] = 'ecommerce.inventory.books'

        e = catch_error do
          t.dsd_circular_dependency_check(cycle)
        end

        # cycle is detected
        assert_equal(
          true,
          e.is_a?(Exception)
        )
      end

      def test_primary_ir_circular_dependency_check
        t = primary_ir

        ir = t.ddd_relations_to_hash(ddd_ir_structure)
        dsd = t.dsd_relations_to_hash(dsd_ir_structure)

        t.ir_add_dsd_attribute!(ir, dsd, :source_dsd_name)
        t.ir_add_full_source_relation_name!(ir, dsd)

        e = catch_error do
          t.primary_ir_circular_dependency_check(ir)
        end

        assert_equal(
          false,
          e.is_a?(Exception),
          pp(ir)
        )

        # let's add a circular reference!
        ir['production.ecommerce.inventory.books_source'][:full_source_relation_name] = 'production.ecommerce.inventory.books'

        e = catch_error do
          t.primary_ir_circular_dependency_check(ir)
        end

        assert_equal(
          true,
          e.is_a?(Exception),
          pp(ir)
        )
      end

      def test_integrate_ddd!
        # test_integrate! handles this well enough
      end

      def test_ir_add_dsd_attribute!
        t = primary_ir
        ir = t.ddd_relations_to_hash(ddd_ir_structure)

        t.ir_add_dsd_attribute!(
          ir,
          t.dsd_relations_to_hash(dsd_ir_structure),
          :relation_type
        )

        assert_equal(
          ConvergDB::DSD::RelationTypes::BASE,
          ir['production.ecommerce.inventory.books_source'][:relation_type],
          pp(ir)
        )

        assert_equal(
          ConvergDB::DSD::RelationTypes::DERIVED,
          ir['production.ecommerce.inventory.books'][:relation_type],
          pp(ir)
        )

        # create an error use case where dsd does not exist for lookup
        ir['production.ecommerce.inventory.books'][:dsd] = 'chicken.foot'

        e = catch_error do
          t.ir_add_dsd_attribute!(
            ir,
            t.dsd_relations_to_hash(dsd_ir_structure),
            :relation_type
          )
        end

        assert_equal(
          true,
          e.is_a?(Exception)
        )
      end

      def test_ir_add_full_source_relation_name!
        t = primary_ir

        ir = t.ddd_relations_to_hash(ddd_ir_structure)
        dsd = t.dsd_relations_to_hash(dsd_ir_structure)

        t.ir_add_dsd_attribute!(ir, dsd, :source_dsd_name)
        t.ir_add_full_source_relation_name!(ir, dsd)

        assert_equal(
          'production.ecommerce.inventory.books_source',
          ir['production.ecommerce.inventory.books'][:full_source_relation_name],
          pp(ir)
        )
      end

      def test_ddd_relations_to_hash
        t = primary_ir
        r = t.ddd_relations_to_hash(ddd_ir_structure)

        expected = {"production.ecommerce.inventory.books_source"=>
          {:generators=>["s3_source", "markdown_doc", "html_doc"],
           :dsd=>"ecommerce.inventory.books_source",
           :full_relation_name=>"production.ecommerce.inventory.books_source",
           :environment=>"production",
           :domain_name=>nil,
           :schema_name=>nil,
           :relation_name=>nil,
           :storage_bucket=>"demo-source-us-east-2.beyondsoft.us",
           :storage_format=>"json"},
         "production.ecommerce.inventory.books"=>
          {:generators=>[
              "athena", 
              "glue", 
              "markdown_doc", 
              "html_doc", 
              "control_table"
            ],
           :full_relation_name=>"production.ecommerce.inventory.books",
           :dsd=>"ecommerce.inventory.books",
           :environment=>"production",
           :domain_name=>nil,
           :schema_name=>nil,
           :relation_name=>nil,
           :service_role=>"glueService",
           :script_bucket=>"demo-utility-us-east-2.beyondsoft.us",
           :temp_s3_location=>nil,
           :storage_bucket=>"demo-target-us-east-2.beyondsoft.us",
           :state_bucket=>"demo-state-us-east-2.beyondsoft.us",
           :storage_format=>"parquet",
           :source_relation_prefix=>nil,
           :etl_job_name=>"nightly_batch",
           :etl_job_schedule=>"cron(0 0 * * ? *)",
           :etl_job_dpu=>2}}

        assert_equal(
          expected,
          r,
          puts(HashDiff.diff(expected, r))
        )
        
        # test error handling for duplicate relation names
        t = primary_ir
        d = ddd_ir_structure
        d << d.last
        
        e = catch_error do
          r = t.ddd_relations_to_hash(d)
        end
        
        assert_equal(
          true,
          e.is_a?(Exception),
          JSON.pretty_generate(d)
        )
      end

      def test_dsd_relations_to_hash
        t = primary_ir
        r = t.dsd_relations_to_hash(dsd_ir_structure)

        expected = {"ecommerce.inventory.books_source"=>
          {:dsd_name=>"ecommerce.inventory.books_source",
           :source_dsd_name=>nil,
           :relation_name=>"books_source",
           :relation_type=>0,
           :partitions=>[],
           :attributes=>
            [{:name=>"item_number",
              :required=>false,
              :expression=>nil,
              :data_type=>"integer",
              :field_type=>nil},
             {:name=>"title",
              :required=>false,
              :expression=>nil,
              :data_type=>"varchar(100)",
              :field_type=>nil},
             {:name=>"author",
              :required=>false,
              :expression=>nil,
              :data_type=>"varchar(100)",
              :field_type=>nil},
             {:name=>"price",
              :required=>false,
              :expression=>nil,
              :data_type=>"numeric(10,2)",
              :field_type=>nil},
             {:name=>"stock",
              :required=>false,
              :expression=>nil,
              :data_type=>"integer",
              :field_type=>nil}]},
         "ecommerce.inventory.books"=>
          {:dsd_name=>"ecommerce.inventory.books",
           :source_dsd_name=>"ecommerce.inventory.books_source",
           :relation_name=>"books",
           :relation_type=>1,
           :partitions=>[],
           :attributes=>
            [{:name=>"item_number",
              :required=>false,
              :expression=>"item_number",
              :data_type=>"integer",
              :field_type=>nil},
             {:name=>"title",
              :required=>false,
              :expression=>"title",
              :data_type=>"varchar(100)",
              :field_type=>nil},
             {:name=>"author",
              :required=>false,
              :expression=>"author",
              :data_type=>"varchar(100)",
              :field_type=>nil},
             {:name=>"price",
              :required=>false,
              :expression=>"price",
              :data_type=>"numeric(10,2)",
              :field_type=>nil},
             {:name=>"unique_id",
              :required=>false,
              :expression=>"concat('book-',md5(title))",
              :data_type=>"varchar(100)",
              :field_type=>nil},
             {:name=>"retail_markup",
              :required=>false,
              :expression=>"price * 0.25",
              :data_type=>"numeric(10,2)",
              :field_type=>nil}]}}

        assert_equal(
          expected,
          r
        )
        
        # test error handling for duplicate relation names
        t = primary_ir
        d = dsd_ir_structure
        d << d.last
        
        e = catch_error do
          r = t.dsd_relations_to_hash(d)
        end
        
        assert_equal(
          true,
          e.is_a?(Exception),
          JSON.pretty_generate(d)
        )
      end

      def test_full_source_relation_name
        t = primary_ir

        [
          {
            expected: 'environment.domain.schema.relation',
            dsd_name: 'domain.schema.relation',
            prefix: ''
          },
          {
            expected: 'environment.domain2.schema.relation',
            dsd_name: 'domain.schema.relation',
            prefix: 'domain2'
          },
          {
            expected: 'environment.domain2.schema2.relation',
            dsd_name: 'domain.schema.relation',
            prefix: 'domain2.schema2'
          },
          {
            expected: 'environment.domain2.schema2.relation2',
            dsd_name: 'domain.schema.relation',
            prefix: 'domain2.schema2.relation2'
          },
        ].each do |e|
          assert_equal(
            e[:expected],
            t.full_source_relation_name(
              e[:dsd_name],
              e[:prefix],
              'environment'
            )
          )
        end

      end

      def test_dsd_tsortable
        t = primary_ir

        assert_equal(
          {
            'ecommerce.inventory.books' => ['ecommerce.inventory.books_source'],
            'ecommerce.inventory.books_source' => []
          },
          t.dsd_tsortable(t.dsd_relations_to_hash(dsd_ir_structure))
        )
      end

      def test_primary_ir_tsortable
        t = primary_ir

        ir = t.ddd_relations_to_hash(ddd_ir_structure)
        dsd = t.dsd_relations_to_hash(dsd_ir_structure)

        t.ir_add_dsd_attribute!(ir, dsd, :source_dsd_name)
        t.ir_add_full_source_relation_name!(ir, dsd)

        ts = t.primary_ir_tsortable(ir)

        assert_equal(
          {
            'production.ecommerce.inventory.books' => ['production.ecommerce.inventory.books_source'],
            'production.ecommerce.inventory.books_source' => []
          },
          ts
        )
      end
      
      def test_ir_add_source_structure!
        t = primary_ir

        ir = t.ddd_relations_to_hash(ddd_ir_structure)
        dsd = t.dsd_relations_to_hash(dsd_ir_structure)

        t.ir_add_dsd_attribute!(ir, dsd, :source_dsd_name)
        t.ir_add_full_source_relation_name!(ir, dsd)
        t.ir_add_source_structure!(ir)

        assert_equal(
          ir['production.ecommerce.inventory.books_source'],
          ir['production.ecommerce.inventory.books'][:source_structure],
          pp(ir)
        )
      end
    end
  end
end
