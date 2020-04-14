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

require 'rltk/parser'

module RLTK
  # A BadToken error indicates that a token was observed in the input stream
  # that wasn't used in the grammar's definition.
  class BadToken < StandardError
    # @return [String] String representation of the error.
    def initialize(token)
      @token = token
    end

    def to_s
      "!Error( Line #{@token.position.line_number},
      Column #{@token.position.line_offset}) in file
      #{@token.position.file_name}: Unexpected token #{@token.type}. \n\n"
    end
  end

  # A NotInLanguage error indicates that a token was observed in the input
  # stream that wasn't recognized in the parser prodocution definition.
  class NotInLanguage < StandardError
    # @return [Array<Token>]  Tokens that have been successfully parsed
    attr_reader :seen

		# @return [Token] Token that caused the parser to stop
		attr_reader :current

		# @return [Array<Token>]  List of tokens that have yet to be seen
		attr_reader :remaining

		# @param [Array<Token>]  seen Tokens that have been successfully parsed
		# @param [Token]         current Token that caused the parser to stop
		# @param [Array<Token>]  remaining Tokens that have yet to be seen
		def initialize(seen, current, remaining)
			@seen      = seen
			@current   = current
			@remaining = remaining
		end

		# @return [String] String representation of the error.
		def to_s
      "!Error( Line #{@current.position.line_number},
      Column #{@current.position.line_offset}) in file
      #{@current.position.file_name}: #{@current}
      not supported in grammar.  \n\n"
		end
  end
end
