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
  module DDD
    # Define token rules for lexer
    class Lexer < RLTK::Lexer
      # Skip whitespace.
      rule(/\s/)
      # Identifier embraced with quote
      rule(/"(.*)"/) { |t| [:IDENT, t] }
      # Keywords
      rule(/relations/) { :RELATIONS }
      rule(/relation/) { :RELATION }
      # Operators and delimiters.
      rule(/\{/) { :LBRACE }
      rule(/\}/) { :RBRACE }
      rule(/=/)  { :EQUAL  }
      # Comment rules.
      rule(/#/) { push_state :comment }
      rule(/\n/, :comment) { pop_state }
      rule(/./, :comment)
      # Identifier rule
      rule(/[\S]+/) { |t| [:IDENT, t] }
    end
  end
end
