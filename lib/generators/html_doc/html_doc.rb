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
