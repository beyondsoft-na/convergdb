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

require 'logger'

module ConvergDB
  module ErrorHandling
    # @param [String] msg customer error msg if provided
    # @param [Logger] logger customer logger if provided
    def log_and_raise_error(msg = nil, logger = nil)
        yield
      rescue => e
        logger = logger ? logger : Logger.new(STDOUT)
        logger.error(msg) if msg
        logger.error(e.class)
        logger.error(e.message)
        #logger.error(e.backtrace.join("\n"))
        e.backtrace { |b| logger.error(b) }
        raise e
    end

    # @param [String] msg customer error msg if provided
    # @param [Logger] logger customer logger if provided
    def log_warning(msg = nil, logger = nil)
        yield
      rescue => e
        logger = logger ? logger : Logger.new(STDOUT)
        logger.warn(msg) if msg
        logger.warn(e.class)
        logger.warn(e.message)
        #logger.error(e.backtrace.join("\n"))
        e.backtrace { |b| logger.warn(b) }
    end

    # ignore the error
    def ignore_error
      begin
        yield
      rescue => e
      end
    end
  end
end
