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

require_relative './lexer'
require_relative './ast'
require_relative './dsd_ir'
require_relative '../base_error.rb'
require_relative '../../exceptions.rb'
require 'json'

module ConvergDB
  module DSD
    # Parse the DSD
    class Parser < RLTK::Parser
      production(:domains) do
        clause('domain') do |domain|
          OneExp.new(domain)
        end
        clause('domain COMMA') do |domain, _|
          OneExp.new(domain)
        end
        clause('domain COMMA domains') do |domain, _, domains|
          TwoExp.new(domain, domains)
        end
        clause('domain domains') do |domain, domains|
          TwoExp.new(domain, domains)
        end
      end

      production(:domain) do
        clause('DOMAIN domain_name LBRACE schemas RBRACE') do
          |_, e0, _, schemas, _|
          ASTDomain.new(e0, schemas)
        end
      end

      production(:domain_name) do
        clause('IDENT') do |i|
          ArrayExp.new([:domain_name, i[1..i.length - 2]])
        end
      end

      production(:schemas) do
        clause('schema') do |schema|
          OneExp.new(schema)
        end
        clause('schema COMMA') do |schema, _|
          OneExp.new(schema)
        end
        clause('schema COMMA schemas') do |schema, _, schemas|
          TwoExp.new(schema, schemas)
        end
        clause('schema schemas') do |schema, schemas|
          TwoExp.new(schema, schemas)
        end
      end

      production(:schema) do
        clause('SCHEMA schema_name LBRACE relations RBRACE') do
          |_, e0, _, relations, _|
          ASTSchema.new(e0, relations)
        end
      end

      production(:schema_name) do
        clause('IDENT') do |i|
          ArrayExp.new([:schema_name, i[1..i.length - 2]])
        end
      end

      production(:relations) do
        clause('relation') do |relation|
          OneExp.new(relation)
        end
        clause('relation COMMA') do |relation, _|
          OneExp.new(relation)
        end
        clause('relation COMMA relations') do |relation, _, relations|
          TwoExp.new(relation, relations)
        end
        clause('relation relations') do |relation, relations|
          TwoExp.new(relation, relations)
        end
      end

      production(:relation) do
        clause('RELATION relation_name LBRACE relation_bodies RBRACE') do
          |_, e0, _, relation_bodies, _|
          ASTRelation.new(e0, relation_bodies)
        end
      end

      production(:relation_name) do
        clause('IDENT') do |i|
          ArrayExp.new([:relation_name, i[1..i.length - 2]])
        end
      end

      production(:relation_bodies) do
        clause('relation_body') do |relation_body|
          OneExp.new(relation_body)
        end
        clause('relation_body relation_bodies') do
          |relation_body, relation_bodies|
          TwoExp.new(relation_body, relation_bodies)
        end
      end

      production(:relation_body) do
        clause('RELATION_TYPE EQUAL base') do |_, _, base|
          OneExp.new(base)
        end
        clause('RELATION_TYPE EQUAL DERIVED LBRACE SOURCE
          EQUAL derived_name RBRACE') do
          |_, _, _, _, _, _, derived_name, _|
          OneExp.new(derived_name)
        end
        clause('partitions_body') do |partitions_body|
          OneExp.new(partitions_body)
        end
        clause('attributes') do |attributes|
          OneExp.new(attributes)
        end
        clause('derived_attributes') do |derived_attributes|
          OneExp.new(derived_attributes)
        end
      end

      production(:derived_name) do
        clause('IDENT') do |i|
          ArrayExp.new([:relation_type, 'derived', i[1..i.length - 2]])
        end
      end

      production(:base) do
        clause('BASE') do |i|
          ArrayExp.new([:relation_type, 'base'])
        end
      end

      production(:partitions_body) do
        clause('PARTITIONS EQUAL partitions') do |_, _, partitions|
          OneExp.new(partitions)
        end
      end

      production(:partitions) do
        clause('LSQUAREBRACKET partition_body RSQUAREBRACKET') do
          |_, partition_body, _|
          OneExp.new(partition_body)
        end
        clause('LSQUAREBRACKET RSQUAREBRACKET') do |_, _|
          ArrayExp.new([:partition])
        end
      end

      production(:partition_body) do
        clause('partition') do |partition|
          OneExp.new(partition)
        end
        clause('partition COMMA') do |partition, _|
          OneExp.new(partition)
        end
        clause('partition COMMA partition_body') do
          |partition, _, partition_body|
          TwoExp.new(partition, partition_body)
        end
      end

      production(:partition) do
        clause('IDENT') do |i|
          ArrayExp.new([:partition, i[1..i.length - 2]])
        end
      end

      production(:derived_attributes) do
        clause('ATTRIBUTES LBRACE derived_attributes_body RBRACE') do
          |_, _, derived_attributes_body, _|
          OneExp.new(derived_attributes_body)
        end
      end

      production(:attributes) do
        clause('ATTRIBUTES LBRACE attributes_body RBRACE') do
          |_, _, attributes_body, _|
          OneExp.new(attributes_body)
        end
      end

      production(:attributes_body) do
        clause('attribute') do |attribute|
          OneExp.new(attribute)
        end
        clause('attribute COMMA') do |attribute, _|
          OneExp.new(attribute)
        end
        clause('attribute attributes_body ') do |attribute, attributes_body|
          TwoExp.new(attribute, attributes_body)
        end
        clause('attribute COMMA attributes_body ') do
          |attribute, _, attributes_body|
          TwoExp.new(attribute, attributes_body)
        end
      end

      production(:derived_attributes_body) do
        clause('derived_attribute') do |derived_attribute|
          OneExp.new(derived_attribute)
        end
        clause('derived_attribute COMMA') do |derived_attribute, _|
          OneExp.new(derived_attribute)
        end
        clause('derived_attribute derived_attributes_body ') do
          |derived_attribute, derived_attributes_body|
          TwoExp.new(derived_attribute, derived_attributes_body)
        end
        clause('derived_attribute COMMA derived_attributes_body ') do
          |derived_attribute, _, derived_attributes_body|
          TwoExp.new(derived_attribute, derived_attributes_body)
        end
      end

      production(:attribute) do
        clause('ATTRIBUTE attribute_name LBRACE attribute_body RBRACE') do
          |_, attribute_name, _, attribute_body, _|
          Attribute.new(attribute_name, attribute_body)
        end
      end

      production(:attribute_name) do
        clause('IDENT') do |i|
          ArrayExp.new([:attribute_name, i[1..i.length - 2]])
        end
      end

      production(:derived_attribute) do
          clause(
            'ATTRIBUTE attribute_name LBRACE derived_attribute_body RBRACE'
          ) do
            |_, attribute_name, _, derived_attribute_body, _|
            Attribute.new(attribute_name, derived_attribute_body)
          end
      end

      production(:attribute_body) do
        clause('attr_item') do |attr_item|
          OneExp.new(attr_item)
        end
        clause('attr_item attribute_body') do |attr_item, attribute_body|
          TwoExp.new(attr_item, attribute_body)
        end
      end

      production(:derived_attribute_body) do
        clause('derived_attr_item') do |derived_attr_item|
          OneExp.new(derived_attr_item)
        end
        clause('derived_attr_item derived_attribute_body') do
          |derived_attr_item, derived_attribute_body|
          TwoExp.new(derived_attr_item, derived_attribute_body)
        end
      end

      production(:attr_item) do
        clause('data_type_item') do |data_type_item|
          OneExp.new(data_type_item)
        end
        clause('field_type_item') do |field_type_item|
          OneExp.new(field_type_item)
        end
        clause('optional_attr') do |optional_attr|
          OneExp.new(optional_attr)
        end
      end

      production(:derived_attr_item) do
        clause('data_type_item') do |data_type_item|
          OneExp.new(data_type_item)
        end
        clause('field_type_item') do |field_type_item|
          OneExp.new(field_type_item)
        end
        clause('optional_attr') do |optional_attr|
          OneExp.new(optional_attr)
        end
        # Good for parsing expression in the current phase
        clause('EXPRESSION EQUAL IDENT') do |_, _, exp|
          ArrayExp.new([:expression, exp[1..exp.length - 2]])
        end
      end

      production(:data_type_item) do
        clause('DATA_TYPE EQUAL data_type') do |_, _, data_type|
          OneExp.new(data_type)
        end
      end

      production(:field_type_item) do
        clause('FIELD_TYPE EQUAL field_type') do |_, _, field_type|
          OneExp.new(field_type)
        end
      end

      production(:data_type) do
        clause('BIGINT') do |i|
          ArrayExp.new([:data_type, 'bigint'])
        end
        clause('DATE') do |i|
          ArrayExp.new([:data_type, 'date'])
        end
        clause('TIME') do |i|
          ArrayExp.new([:data_type, 'time'])
        end
        clause('TIMESTAMP') do |i|
          ArrayExp.new([:data_type, 'timestamp'])
        end
        clause('TIMESTAMPTZ') do |i|
          ArrayExp.new([:data_type, 'timestamptz'])
        end
        clause('BYTE') do |i|
          ArrayExp.new([:data_type, 'byte'])
        end
        clause('WORD') do |i|
          ArrayExp.new([:data_type, 'word'])
        end
        clause('INTEGER') do |i|
          ArrayExp.new([:data_type, 'integer'])
        end
        clause('FLOAT') do |i|
          ArrayExp.new([:data_type, 'float'])
        end
        clause('DOUBLE') do |i|
          ArrayExp.new([:data_type, 'double'])
        end
        clause('BOOLEAN') do |i|
          ArrayExp.new([:data_type, 'boolean'])
        end
        clause('NUMERIC LPAREN NUMBER COMMA NUMBER RPAREN') do
          |_, _, n0, _, n1, _|
          ArrayExp.new(
            [:data_type, 'numeric(' + n0.to_s + ',' + n1.to_s + ')']
          )
        end
        clause('VARCHAR LPAREN NUMBER RPAREN') do |_, _, n, _|
          ArrayExp.new([:data_type, 'varchar(' + n.to_s + ')'])
        end
      end

      production(:field_type) do
        clause('DIMENSION') do |i|
          ArrayExp.new([:field_type, 'dimension'])
        end
        clause('MEASURE') do |i|
          ArrayExp.new([:field_type, 'measure'])
        end
      end

      production(:optional_attr) do
        clause('optional_attr_item') do |optional_attr_item|
          OneExp.new(optional_attr_item)
        end
        clause('optional_attr_item optional_attr') do
          |optional_attr_item, optional_attr|
          TwoExp.new(optional_attr_item, optional_attr)
        end
      end

      production(:optional_attr_item) do
        clause('REQUIRED EQUAL required_type') do |_, _, required_type|
          OneExp.new(required_type)
        end
        clause('ORDINAL_POSITION EQUAL INTEGER') do |_, _, _|
          ArrayExp.new([:ordinal_position, :integer])
        end
        clause('PROPERTIES properties_name properties RBRACE') do
          |_, properties_name, properties, _|
          Properties.new(properties_name, properties)
        end
        clause('PROPERTIES properties_name RBRACE') do
          |_, properties_name, _|
        end
      end

      production(:properties_name) do
        clause('LBRACE') do |i|
          ArrayExp.new([:properties])
        end
      end

      production(:required_type) do
        clause('TRUE') do |i|
          ArrayExp.new([:required_type, 'true'])
        end
        clause('FALSE') do |i|
          ArrayExp.new([:required_type, 'false'])
        end
      end

      production(:properties) do
        clause('property') do |property|
          OneExp.new(property)
        end
        clause('property properties') do |property, properties|
          TwoExp.new(property, properties)
        end
      end

      # make sure the item1 = item2 format inside the properties
      production(:property) do
        clause('LABEL EQUAL label_item') do |_, _, label_item|
          OneExp.new(label_item)
        end
        clause('DEFAULT_AGGREGATE EQUAL default_aggregate_item') do
          |_, _, default_aggregate_item|
          OneExp.new(default_aggregate_item)
        end
      end

      production(:label_item) do
        clause('IDENT') do |i|
          ArrayExp.new([:label_item, i[1..i.length - 2]])
        end
      end

      # Allowed items for default_aggregate
      production(:default_aggregate_item) do
        clause('SUM') do |i|
          ArrayExp.new([:default_aggregate, 'sum'])
        end
        clause('COUNT') do |i|
          ArrayExp.new([:default_aggregate, 'count'])
        end
        clause('DISTINCT') do |i|
          ArrayExp.new([:default_aggregate, 'distinct'])
        end
        clause('AVG') do |i|
          ArrayExp.new([:default_aggregate, 'avg'])
        end
        clause('MIN') do |i|
          ArrayExp.new([:default_aggregate, 'min'])
        end
        clause('MAX') do |i|
          ArrayExp.new([:default_aggregate, 'max'])
        end
      end
      finalize
    end

    # Populate all dsd into IR
    class IR
      include ConvergDB::ErrorHandling
      # tokenize input strings to get tokens
      # @param [Array<source>] source List of dsd inputs in string format
      # @return [Array<Token>] List of tokens
      def get_token_str(source)
        log_and_raise_error(
            'dsd lexer failure, contains unsupported tokens'
        ) do
          tokens = []
          source.each do |str|
            tokens.pop unless tokens.empty?
            tokens += ConvergDB::DSD::Lexer.lex(str)
          end
          tokens
        end
      end

      # tokenize input files to get tokens
      # @param [Array<source>] source List of dsd input files
      # @return [Array<Token>] List of tokens
      def get_token(source)
        log_and_raise_error(
            'dsd lexer failure, contains unsupported tokens'
        ) do
          tokens = []
          source.each do |file|
            tokens.pop unless tokens.empty?
            tokens += ConvergDB::DSD::Lexer.lex_file(file)
          end
          tokens
        end
      end

      # parse tokens to get ast
      # @param [Array<Token>] tokens List of tokens
      # @return [Array<AST>] List of internal representation
      def get_ast(tokens)
        log_and_raise_error(
            'dsd parser failure'
        ) do
          dsd_ast = []
          ast = ConvergDB::DSD::Parser.parse(tokens, { parse_tree: false })
          ast.each(order = :pre) do |c|
            c = c.values[0]
            dsd_ast << c if c.is_a?(Array)
          end
          dsd_ast
        end
      end

      # populate ast into intermediate representation
      # @param [Array<AST>] ast List of ASTs
      # @return [ConvergDB::DSD::DSDIRBuilder] Intermediate representation
      def get_ir(ast)
        log_and_raise_error(
            'populate dsd-ast into IR error'
        ) do
          f = ConvergDB::DSD::DSDIRBuilder.new
          ast.each do |a|
            case a[0]
            when :domain_name       then f.domain(a[1])
            when :schema_name       then f.schema(a[1])
            when :relation_name     then f.relation(a[1])
            when :relation_type
              f.relation_type(a[1])
              f.relation_source(a[2]) unless a[2].nil?
            when :partition
              f.partition(a[1]) unless a[1].nil?
            when :attribute_name    then f.attribute(a[1])
            when :expression        then f.expression(a[1])
            when :required_type     then f.required(a[1])
            when :data_type         then f.data_type(a[1])
            when :field_type        then f.field_type(a[1])
            when :label_item        then f.property('label', a[1])
            when :default_aggregate then f.property('default_aggregate', a[1])
            end
          end
          f
        end
      end
    end
  end
end
