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

require 'rltk/ast'

module ConvergDB
  module DDD
    # An Expression class is inherited from RLTK::ASTNode, which
    # represents the internal parser results.
    class Expression < RLTK::ASTNode; end

    # An Variable class that contains array-typed expression
    class Variable < Expression
      value :name, Array
    end

    # A OneExp class that contains one-child ast
    class OneExp < Expression
      child :exp, Expression
    end

    # A TwoExp class that contains two-child ast
    class TwoExp < Expression
      child :left, Expression
      child :right, Expression
    end
  end
end
