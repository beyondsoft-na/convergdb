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
  module DSD
    # An Expression class is inherited from RLTK::ASTNode, which
    # represents the internal parser results.
    class Expression < RLTK::ASTNode; end

    # A Number class that contains number-typed expression
    class Number < Expression
      value :value, Integer
    end

    # A Variable class that contains string-typed expression
    class Variable < Expression
      value :name, String
    end

    # An ArrayExp class that contains array-typed expression
    class ArrayExp < Expression
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

    # A ThreeExp class that contains three-child ast
    class ThreeExp < Expression
      child :children1, Expression
      child :children2, Expression
      child :children3, Expression
    end

    # A FourExp class that contains four-child ast
    class FourExp < Expression
      child :children1, Expression
      child :children2, Expression
      child :children3, Expression
      child :children4, Expression
    end

    # A FiveExp class that contains five-child ast
    class FiveExp < Expression
      child :children1, Expression
      child :children2, Expression
      child :children3, Expression
      child :children4, Expression
      child :children5, Expression
    end

    # A Equal is a type of TwoExp
    class Equal < TwoExp; end

    # An ASTDomain class is a type of TwoExp
    class ASTDomain < TwoExp; end

    # An ASTSchema class is a type of TwoExp
    class ASTSchema < TwoExp; end

    # An ASTRelation class is a type of TwoExp
    class ASTRelation < TwoExp; end

    # A Properties class is a type of TwoExp
    class Properties < TwoExp; end

    # An Attribute class is a type of TwoExp
    class Attribute < TwoExp; end
  end
end
