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
        return nil
    end

    # ignore the error
    def ignore_error
      begin
        yield
      rescue => e
        nil
      end
    end
  end
end
