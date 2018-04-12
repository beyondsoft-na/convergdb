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

require 'rltk/lexer'

module ConvergDB
  module DSD
    # Define token rules for lexer
    class Lexer < RLTK::Lexer
      # Skip whitespace
      rule(/\s/)
      # Keywords
      rule(/domain/) { :DOMAIN }
      rule(/schema/) { :SCHEMA }
      rule(/attribute/) { :ATTRIBUTE }
      rule(/attributes/) { :ATTRIBUTES }
      rule(/avg/) { :AVG }
      rule(/base/) { :BASE }
      rule(/bigint/) { :BIGINT }
      rule(/boolean/) { :BOOLEAN }
      rule(/byte/) { :BYTE }
      rule(/count/) { :COUNT }
      rule(/data_type/) { :DATA_TYPE }
      rule(/date/) { :DATE }
      rule(/default_aggregate/) { :DEFAULT_AGGREGATE }
      rule(/derived/) { :DERIVED }
      rule(/dimension/) { :DIMENSION }
      rule(/distinct/) { :DISTINCT }
      rule(/double/) { :DOUBLE }
      rule(/false/) { :FALSE }
      rule(/field_type/) { :FIELD_TYPE }
      rule(/float/) { :FLOAT }
      rule(/integer/) { :INTEGER }
      rule(/label/) { :LABEL }
      rule(/max/) { :MAX }
      rule(/measure/) { :MEASURE }
      rule(/min/) { :MIN }
      rule(/numeric/) { :NUMERIC }
      rule(/ordinal_position/) { :ORDINAL_POSITION }
      rule(/properties/) { :PROPERTIES }
      rule(/relation/) { :RELATION }
      rule(/relation_type/) { :RELATION_TYPE }
      rule(/required/) { :REQUIRED }
      rule(/source/) { :SOURCE }
      rule(/sum/) { :SUM }
      rule(/time/) { :TIME }
      rule(/timestamp/) { :TIMESTAMP }
      rule(/timestamptz/) { :TIMESTAMPTZ }
      rule(/true/) { :TRUE }
      rule(/varchar/) { :VARCHAR }
      rule(/word/) { :WORD }
      rule(/partitions/) { :PARTITIONS }
      # Operators and delimiters.
      rule(/\(/) { :LPAREN }
      rule(/\)/) { :RPAREN }
      rule(/\{/) { :LBRACE }
      rule(/\}/) { :RBRACE }
      rule(/:/)	 { :COLON  }
      rule(/,/)	 { :COMMA  }
      rule(/"/) { :QUOTE  }
      rule(/=/) { :EQUAL  }
      rule(/\./) { :DOT }
      rule(/'/) { :SINGLEQUOTE }
      rule(/!=/) { :NEQUAL }
      rule(/>/) { :LARGER }
      rule(/</) { :SMALLER }
      rule(/>=/) { :LARGEREEQUAL }
      rule(/<=/) { :SMALLEREQUAL }
      rule(/\|\|/) { :CONCAT }
      rule(/\+/) { :PLUS }
      rule(/\-/) { :MINUS }
      rule(/\_/) { :UNDERSCORE }
      rule(/\*/) { :MULT }
      rule(/\//) { :DIV }
      rule(/\[/) { :LSQUAREBRACKET }
      rule(/\]/) { :RSQUAREBRACKET }
      # expression
      rule(/expression/) { push_state(:EXP); :EXPRESSION }
      rule(/"([^\n])*"/, :EXP) { |t| set_flag(:e); [:IDENT, t] }
      rule(/\s/, :EXP, [:e]) { pop_state; clear_flags }
      rule(/\s/, :EXP)
      rule(/=/, :EXP) { :EQUAL }
      rule(/\}/, :EXP) { pop_state; :RBRACE }
      rule(/\n/, :EXP) { pop_state }
      rule(/\#/, :EXP) { pop_state; push_state(:comment) }
      # Identifier rule
      rule(/"(\S)*"/) { |t| [:IDENT, t] }
      # Integer Number rules.
      rule(/\d+/) { |t| [:NUMBER, t.to_i] }
      # Comment rules.
      rule(/#/) { push_state :comment }
      rule(/\n/, :comment) { pop_state }
      rule(/./, :comment)
    end
  end
end
