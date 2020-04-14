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
