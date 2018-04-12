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
