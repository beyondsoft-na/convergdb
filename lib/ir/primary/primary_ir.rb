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

require 'pp'
require 'tsort'
require 'json'
require_relative '../../exceptions.rb'

module ConvergDB
  # classes to create the primary internal representation of the current
  # deployment.
  module PrimaryIR
    # provides an easy implementation for a topological sort.
    # uses ruby tsort library.
    class TsortableHash < Hash
      include TSort
      alias tsort_each_node each_key
      # implements the tsort
      def tsort_each_child(node, &block)
        fetch(node).each(&block)
      end
    end

    # primary intermediate representation of a ConvergDB deployment.
    # the ir attribute of this class represents an array of relations
    # and their associated attributes. there are several validations
    # to insure that there are no circular dependancies between relations.
    # the structure is initialized by creating an array element for each
    # relation specified in the ddd... then elements of the dsd are looked up
    # and pulled over when necessary.
    class PrimaryIR
      include ConvergDB::ErrorHandling
      # @return [Hash] structure for downstream consumption.
      attr_accessor :ir
      attr_reader :dsd_ir

      def initialize
        @ir = []
      end

      # integrates dsd_ir and ddd_ir into @ir attrbute.
      # @param [Array<Hash>] dsd_ir
      # @param [Array<Hash>] ddd_ir
      def integrate!(dsd_ir, ddd_ir)
        integrate_dsd!(dsd_ir)
        integrate_ddd!(ddd_ir)
      end

      # integration path for dsd.
      # sets dsd_ir as a side effect.
      # dsd_ir can be mutated and validated.
      # @param [Array<Hash>] dsd_ir
      def integrate_dsd!(dsd_ir)
        # create hash of all dsd relations
        # also insures that all values are unique
        @dsd_ir = dsd_relations_to_hash(dsd_ir)

        # insure that there are no circular dependencies
        dsd_circular_dependency_check(@dsd_ir)
      end

      # performs a circular dependency check with a topographical
      # sort. the dsd names are graphed with references to their
      # source relations. this is a validation that returns
      # nothing and raises and error if the check fails.
      # @param [Array<Hash>] dsd_ir
      def dsd_circular_dependency_check(dsd_ir)
        log_and_raise_error(
          "dependency failure in dsd"
        ) do
          dsd_tsortable(dsd_ir).tsort
        end
      end

      # performs a circular dependency check with a topographical
      # sort. the dsd names are graphed with references to their
      # source relations. this is a validation that returns
      # nothing and raises and error if the check fails.
      # @param [Array<Hash>] primary_ir
      def primary_ir_circular_dependency_check(primary_ir)
        log_and_raise_error(
          "dependency failure in implementation relations"
        ) do
          primary_ir_tsortable(primary_ir).tsort
        end
      end

      # integration steps for the ddd_ir. every relation in the
      # ir is based upon a relation specified in the ddd. this method
      # builds the ir by performing lookups against the ddd.
      # @param [Array<Hash>] ddd_ir
      def integrate_ddd!(ddd_ir)
        # create hash of all ddd relations
        # insure that all `full_relation_names` are unique
        @ir = ddd_relations_to_hash(ddd_ir)

        # perform lookup for attributes of each relation.
        # attributes are looked up against @dsd_ir based upon dsd_name.
        ir_add_dsd_attribute!(@ir, @dsd_ir, :attributes)
        ir_add_dsd_attribute!(@ir, @dsd_ir, :partitions)
        ir_add_dsd_attribute!(@ir, @dsd_ir, :relation_type)
        ir_add_dsd_attribute!(@ir, @dsd_ir, :source_dsd_name)

        # sets the fully resolved full_source_relation_name.
        ir_add_full_source_relation_name!(@ir, @dsd_ir)
        
        primary_ir_circular_dependency_check(@ir)
        
        # once set... full_source_relation_name is used to look up concrete
        # implementation details for the base relation (if any)
        ir_add_source_structure!(@ir)
        
        # create control table for derived relations
        ir_add_control_table!(@ir)
        
        validate_relation_references(@ir)
      end

      # iterates through the ir relations. each relation has the
      # attribute looked up on the dsd_ir... then stored as an attribute
      # of itself.
      # @param [Array<Hash>] ir
      # @param [Array<Hash>] dsd_ir
      # @param [Symbol] attribute new attribute name in ir target (ignored if nil)
      # @param [Symbol] override attribute name of attribute to copy from dsd_ir
      def ir_add_dsd_attribute!(ir, dsd_ir, attribute, override=nil)
        attribute = override ? override : attribute
        ir.keys.each do |k|
          if dsd_ir[ir[k][:dsd]]
            ir[k][attribute] = dsd_ir[ir[k][:dsd]][attribute]
          else
            raise "undefined schema object: #{ir[k][:dsd]}"
          end
        end
      end

      # iterates through the ir relations. each relation has the
      # attribute looked up on the dsd_ir... then stored as an attribute
      # of itself.
      # @param [Array<Hash>] ir
      # @param [Array<Hash>] dsd_ir
      # @param [Symbol] attribute name of attribute to copy from dsd_ir
      # @param [Symbol] override new attribute name in ir target
      def ir_add_dsd_attribute_with_override!(ir, dsd_ir, attribute, override)
        ir.keys.each do |k|
          if dsd_ir[ir[k][:dsd]]
            ir[k][attribute] = dsd_ir[ir[k][:dsd]][attribute]
          else
            raise "undefined schema object: #{ir[k][:dsd]}"
          end
        end
      end
      
      # sets the full_source_relation_name on each of the derived relations.
      # @param [Array<Hash>] ir
      # @param [Array<Hash>] dsd_ir
      def ir_add_full_source_relation_name!(ir, dsd_ir)
        ir.keys.each do |k|
          if ir[k][:source_dsd_name]
            ir[k][:full_source_relation_name] = full_source_relation_name(
              ir[k][:source_dsd_name],
              ir[k][:source_relation_prefix],
              ir[k][:environment]
            )
          end
        end
      end
      
      # adds the source structure to the current ir of a relation
      # if the relation is not a base relation.
      # @param [Array<Hash>] ir
      def ir_add_source_structure!(ir)
        ir.keys.each do |k|
          if ir[k][:full_source_relation_name]
            ir[k][:source_structure] = ir[
              ir[k][:full_source_relation_name]
            ]
          end
        end
      end
      
      # adds control table name to the IR for derived relations.
      # @param [Array<Hash>] ir
      def ir_add_control_table!(ir)
        ir.keys.each do |k|
          if ir[k][:full_source_relation_name]
            database = 'convergdb_control_${deployment_id}'
            table = ir[k][:full_relation_name].gsub('.','__')
            ir[k][:control_table] = "#{database}.#{table}"
          end
        end
      end

      # converts ddd relations into a hash format. much more useful when
      # performing lookups.
      # @return [Hash] all relations as a hash with full_relation_name as key
      def ddd_relations_to_hash(ddd)
        ret = {}
        ddd.each do |d|
          d[:relations].each do |r|
            if ret.key?(r[:full_relation_name])
              raise "duplicate relation #{r[:full_relation_name]}"
            end
            ret[r[:full_relation_name]] = r
          end
        end
        ret
      end

      # converts dsd relations into a hash format. much more useful when
      # performing lookups.
      # @param [Hash] dsd
      # @return [Hash] all relations as a hash with full_relation_name as key
      def dsd_relations_to_hash(dsd)
        ret = {}
        dsd.each do |domain|
          domain[:schemas].each do |schema|
            schema[:relations].each do |relation|
              if ret.key?(relation[:dsd_name])
                raise "duplicate dsd relation #{relation[:dsd_name]}"
              else
                ret[relation[:dsd_name]] = relation
              end
            end
          end
        end
        ret
      end

      # constructs full_source_relation_name by applying overrides to the
      # original source_dsd_name. the prefix override can be a domain,
      # domain.schema or domain.schema.relation
      # @param [String] source_dsd_name
      # @param [String] source_dsd_prefix override
      # @param [String] environment override
      # @return [String]
      def full_source_relation_name(
          source_dsd_name,
          source_dsd_prefix,
          environment
        )

        # lookup dsd source
        # override with prefix
        # apply environment
        prefix = []
        prefix = source_dsd_prefix.split('.') if source_dsd_prefix

        src_dsd = source_dsd_name.split('.')

        r = []
        r[0] = environment
        r[1] = prefix[0] || src_dsd[0]
        r[2] = prefix[1] || src_dsd[1]
        r[3] = prefix[2] || src_dsd[2]
        r.join('.')
      end

      # takes dsd_ir as input, and returns a hash object appropriate
      # for topological sorting. the object returned has dsd names as keys
      # and an array of dependant dsd names as values. used to check for
      # circular dependencies amongst derived relations.
      # @param [Hash] dsd_ir
      # @return [TsortableHash]
      def dsd_tsortable(dsd_ir)
        h = TsortableHash.new
        dsd_ir.keys.each do |k|
          if dsd_ir[k][:source_dsd_name]
            h[k] = [dsd_ir[k][:source_dsd_name]]
          else
            h[k] = []
          end
        end
        h
      end

      # takes primary_ir as input, and returns a hash object appropriate
      # for topological sorting. the object returned has relation names as keys
      # and an array of dependant relation names as values. used to check for
      # circular dependencies.
      # @param [Hash] primary_ir
      # @return [TsortableHash]
      def primary_ir_tsortable(primary_ir)
        h = TsortableHash.new()
        primary_ir.each_key do |k|
          if primary_ir[k][:full_source_relation_name]
            h[k] = [primary_ir[k][:full_source_relation_name]]
          else
            h[k] = []
          end
        end
        h
      end

      # @param [Array<Hash>] primary_ir
      # @param [String] full_relation_name
      # @return [Hash]
      def primary_ir_by_relation_name(primary_ir, full_relation_name)
        primary_ir.each do |primary|
          if primary[:full_relation_name] == full_relation_name
            return primary
          end
        end
        raise "#{full_relation_name} not found in primary representation"
      end

      # validates that all derived relations are pointing to a base
      # relation of the correct "class".. meaning that the dsd
      # of the source relation is what was specified in the dsd, regardless
      # of any overrides.
      # @param [Hash] primary_ir
      def validate_relation_references(primary_ir)
        primary_ir.keys.each do |k|
          if primary_ir[k][:source_dsd_name]
            unless primary_ir[k][:source_structure][:dsd] == primary_ir[k][:source_dsd_name]
              raise "#{primary_ir[k][:full_source_relation_name]} is not an implementation of #{primary_ir[k][:source_dsd_name]} as required by #{primary_ir[k][:full_relation_name]}... which is an implementation of #{primary_ir[k][:dsd]}"
            end
          end
        end
        nil
      end
    end
  end
end
