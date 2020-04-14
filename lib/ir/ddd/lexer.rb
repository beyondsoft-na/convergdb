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
      # Identifier without quote
      rule(/^[A-Za-z0-9][A-Za-z0-9_]*/) { |t| [:RIDENT, t] }
    end
  end
end
