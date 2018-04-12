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

[
  'deployment/terraform/terraform.rb',
  'generators/generate.rb',
  'generators/athena/athena.rb',
  'generators/glue/glue.rb',
  'generators/s3_source/s3_source.rb',
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
