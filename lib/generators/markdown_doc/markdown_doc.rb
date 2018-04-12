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

require 'erb'
require_relative '../generate.rb'

module ConvergDB
  # generators to create markdown doc
  module Generators
    # used to generate markdown doc
    class MarkdownDoc < BaseGenerator
      # generates the artifacts
      def generate!
        structure = @structure
        File.open(doc_file_path, 'w') do |f|
          f.puts erb_output
        end
      end

      # return erb output
      # @return erb output
      def erb_output
        ERB.new(File.read(erb_path)).result(binding)
      end

      # local path to the markdown erb template file.
      # @return [String]
      def erb_path
        "#{File.dirname(__FILE__)}/markdown_doc.erb"
      end

      # local path to the erb output for this relation_name
      # @return [String]
      def doc_file_path
        "#{@structure[:working_path]}/docs/#{@structure[:full_relation_name]}.md"
      end
    end
  end
end
