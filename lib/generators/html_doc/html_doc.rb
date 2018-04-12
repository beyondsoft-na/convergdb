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
  module Generators
    # used to generate html doc
    class HtmlDoc < BaseGenerator
      # generates the html doc
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

      # specify the erb path
      # @return [string] erb path
      def erb_path
        "#{File.dirname(__FILE__)}/html_doc.erb"
      end

      # specify the html file path
      # @return [string] html path
      def doc_file_path
        "#{@structure[:working_path]}/docs/#{@structure[:full_relation_name]}.html"
      end
    end
  end
end
