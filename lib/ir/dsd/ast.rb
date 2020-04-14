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
