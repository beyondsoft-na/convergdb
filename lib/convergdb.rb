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

[
  'deployment/terraform/terraform.rb',
  'generators/generate.rb',
  'generators/athena/athena.rb',
  'generators/glue/glue.rb',
  'generators/s3_source/s3_source.rb',
  'generators/streaming_inventory/streaming_inventory.rb',
  'interactions/interactions.rb',
  'ir/base_ir.rb',
  'ir/ddd/ast.rb',
  'ir/ddd/ddd_ir.rb',
  'ir/ddd/lexer.rb',
  'ir/ddd/parser.rb',
  'ir/dsd/ast.rb',
  'ir/dsd/dsd_ir.rb',
  'ir/dsd/lexer.rb',
  'ir/dsd/parser.rb',
  'ir/live/live.rb',
  'ir/primary/primary_ir.rb',
  'exceptions.rb',
  'version.rb'
].each do |lib|
  require_relative "#{File.dirname(__FILE__)}/#{lib}"
end
