# Copyright © 2020 Beyondsoft Consulting, Inc.

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the “Software”), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
# BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require_relative './lexer'
require_relative './ast'
require_relative './ddd_ir'
require_relative '../base_error.rb'
require_relative '../../exceptions.rb'
require 'json'

module ConvergDB
  module DDD
    # Parse the DDD
    class Parser < RLTK::Parser
      production(:deploys) do
        clause('deploy') do |deploy|
          OneExp.new(deploy)
        end
        clause('deploy deploys') do |deploy, deploys|
          TwoExp.new(deploy, deploys)
        end
      end

      production(:deploy) do
        clause('RIDENT IDENT LBRACE deploy_bodies RBRACE') do
          |e0, e1, _, deploy_bodies, _|
          TwoExp.new(
            Variable.new([:new_deployment, e0.to_sym, e1[1..e1.length - 2]]),
            deploy_bodies)
        end
      end

      production(:deploy_bodies) do
        clause('deploy_body') do |deploy_body|
          OneExp.new(deploy_body)
        end
        clause('deploy_body deploy_bodies') do |deploy_body, deploy_bodies|
          TwoExp.new(deploy_body, deploy_bodies)
        end
        #clause('') { || nil }
      end

      production(:deploy_body) do
        clause('RIDENT EQUAL IDENT') do |e0, _, e1|
          Variable.new([:deployment, e0.to_sym, e1[1..e1.length - 2]])
        end
        clause('RIDENT EQUAL RIDENT') do |e0, _, e1|
          Variable.new([:deployment, e0.to_sym, e1])
        end
        clause('RELATIONS LBRACE relations_body RBRACE') do
          |_, _, relations_body, _|
          OneExp.new(relations_body)
        end
      end

      production(:relations_body) do
        clause('relation') do |relation|
          OneExp.new(relation)
        end
        clause('relation relations_body') do |relation, relations_body|
          TwoExp.new(relation, relations_body)
        end
      end

      production(:relation) do
        clause('RELATION LBRACE relation_bodies RBRACE') do
          |_, _, relation_bodies, _|
          TwoExp.new(Variable.new([:new_relation]), relation_bodies)
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
        clause('RIDENT EQUAL IDENT') do |e0, _, e1|
          Variable.new([:relation, e0.to_sym, e1[1..e1.length - 2]])
        end
        clause('RIDENT EQUAL RIDENT') do |e0, _, e1|
          Variable.new([:relation, e0.to_sym, e1])
        end
      end
    finalize
    end

    # Populate all ddd into IR
    class IR
      include ConvergDB::ErrorHandling
      # tokenize input strings to get tokens
      # @param [Array<source>] source List of ddd inputs in string format
      # @return [Array<Token>] List of tokens
      def get_token_str(source)
        log_and_raise_error(
            'ddd lexer failure, contains unsupported tokens'
        ) do
          tokens = []
          source.each do |str|
            tokens.pop unless tokens.empty?
            tokens += ConvergDB::DDD::Lexer.lex(str)
          end
          tokens
        end
      end

      # tokenize input files to get tokens
      # @param [Array<source>] source List of ddd input files
      # @return [Array<Token>] List of tokens
      def get_token(source)
        log_and_raise_error(
            'ddd lexer failure, contains unsupported tokens'
        ) do
          tokens = []
          source.each do |file|
            tokens.pop unless tokens.empty?
            tokens += ConvergDB::DDD::Lexer.lex_file(file)
          end
          tokens
        end
      end

      # parse tokens to get ast
      # @param [Array<Token>] tokens List of tokens
      # @return [Array<AST>] List of internal representation
      def get_ast(tokens)
        log_and_raise_error(
            'ddd parser failure'
        ) do
          ast = ConvergDB::DDD::Parser.parse(tokens, { parse_tree: false })
          ddd_ast = []
          # traverse the tree
          ast.each(order = :pre) do |c|
            c = c.values[0]
            ddd_ast << c if c.is_a?(Array)
          end
          ddd_ast
        end
      end

      # populate ast into intermediate representation
      # @param [Array<AST>] ast List of ASTs
      # @return [ConvergDB::DSD::DSDIRBuilder] Intermediate representation
      def get_ir(ast)
        log_and_raise_error(
            'populate ddd-ast into IR error'
        ) do
          f = ConvergDB::DDD::DDDIRBuilder.new
          ast.each do |a|
            f.deployment(a[1], a[2]) if a[0] == :new_deployment
            f.relation if a[0] == :new_relation
            if [:deployment, :relation].include?(a[0])
              f.attribute(a[0], a[1], a[2])
            end
          end
          f
        end
      end
    end
  end
end
