#! /usr/bin/env ruby

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

require 'json'
require 'rainbow'

module ConvergDB
  # contains classes for high level actions that can be performed by
  # users. these classes do the actual work of the CLI commands.
  module Interactions
    class Licenses
      def initialize
        puts %{
ConvergDB - DevOps for Data
Copyright (C) 2018 Beyondsoft Consulting, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

Please visit https://github.com/beyondsoft-na/convergdb to see licenses
for the libraries utilized in ConvergDB.
}
      end
    end    

    # action to generate a ConvergDB deployment
    class Generate
      attr_accessor :master_generator

      # creates a dsd from a list of dsd files.
      # @param [Array<String>] dsd_files
      # @return [Hash] resolved and validated dsd structure
      def dsd(dsd_files)
        if dsd_files == []
          raise("no schema files found")
        end

        dsd_ir = ConvergDB::DSD::IR.new
        begin
          puts("parsing schema files...")
          dsd_tmp = dsd_ir.get_ir(
            dsd_ir.get_ast(
              dsd_ir.get_token(dsd_files)
            )
          ).top_level
        rescue => e
          puts "dsd parser error"
          raise e
        end

        puts("resolving schema...")
        dsd_tmp.resolve!
        puts("validating schema...")
        dsd_tmp.validate
        return dsd_tmp.structure
      rescue => e
        puts "dsd error"
        raise e
      end

      # creates a ddd from a list of dsd files.
      # @param [Array<String>] ddd_files
      # @return [Hash] resolved and validated ddd structure
      def ddd(ddd_files)
        if ddd_files == []
          raise("no deployment files found")
        end

        ddd_ir = ConvergDB::DDD::IR.new
        begin
          puts("parsing deployment files...")
          ddd_tmp = ddd_ir.get_ir(
            ddd_ir.get_ast(
              ddd_ir.get_token(ddd_files)
            )
          ).top_level
        rescue => e
          puts "ddd parser error"
          raise e
        end

        puts("resolving deployment...")
        ddd_tmp.resolve!

        puts("validating deployment...")
        ddd_tmp.validate
        return ddd_tmp.structure
      rescue => e
        puts "ddd error"
        raise e
      end

      # creates the primary internal representation.
      # @param [Hash] dsd
      # @param [Hash] ddd
      # @return [Hash] fully integrated internal representation
      def ir(dsd, ddd)
        puts("integrating schema and deployment...")
        s = ConvergDB::PrimaryIR::PrimaryIR.new
        s.integrate!(dsd, ddd)
        s.ir
      rescue => e
        puts "error creating internal primary representation"
        raise e
      end

      # set working_path for each entry in ir.
      # @param [Hash] ir
      # @param [String] working_path
      def apply_working_path!(ir, working_path)
        ir.keys.each do |k|
          ir[k][:working_path] = File.expand_path(working_path)
        end
      end

      # @param [Array<String>] dsd array of dsd files
      # @param [Array<String>] ddd array of ddd files
      # @param [Array<String>] working_path
      def initialize(dsd, ddd, working_path)
        puts(banner)
        puts("version #{ConvergDB::VERSION}")
        puts

        ddd = ddd(ddd)
        dsd = dsd(dsd)
        ir = ir(dsd, ddd)

        apply_working_path!(ir, working_path)

        # separated to an attribute for unit testing purposes
        puts("generating artifacts...")
        puts("")
        @master_generator = ConvergDB::Generators::MasterGenerator.new(ir)
      end

      # outputs the convergdb banner
      def banner
        %q{
                                         _ _
                                        | | |
   ___ ___  _ ____   _____ _ __ __ _  __| | |__
  / __/ _ \| '_ \ \ / / _ \ '__/ _` |/ _` | '_ \
 | (_| (_) | | | \ V /  __/ | | (_| | (_| | |_) |
  \___\___/|_| |_|\_/ \___|_|  \__, |\__,_|_.__/
                                __/ |
                               |___/             }
      end
    end
  end
end
