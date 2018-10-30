require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

require 'minitest'
require 'minitest/autorun'

require_relative '../lib/ir/dsd/dsd_ir.rb'
require_relative '../lib/ir/dsd/lexer.rb'
require_relative '../lib/ir/dsd/ast.rb'
require_relative '../lib/ir/dsd/parser.rb'

module ConvergDB
  module DSD
    class TestDSDParser < Minitest::Test
        # @return [Array<Hash>]
        def token_test_cases
        [
          # each hash is a test case with a str: and desired_tokens:
          {
            str: %{domain "domain_name" {
                }},
            desired_tokens: [
              "DOMAIN",
              "IDENT(\"domain_name\")",
              "LBRACE",
              "RBRACE",
              "EOS"
            ]
          },
          {
            str: %{schema "schema_name" {
                }},
            desired_tokens: [
              "SCHEMA",
              "IDENT(\"schema_name\")",
              "LBRACE",
              "RBRACE",
              "EOS"
            ]
          },
          {
            str: %{relation "relation_name" {
                }},
            desired_tokens: [
              "RELATION",
              "IDENT(\"relation_name\")",
              "LBRACE",
              "RBRACE",
              "EOS"
            ]
          },
          {
            str: %{
              partitions = []
              partitions = ["user_id", "user_name"]
              },
            desired_tokens: [
              "PARTITIONS",
              "EQUAL",
              "LSQUAREBRACKET",
              "RSQUAREBRACKET",
              "PARTITIONS",
              "EQUAL",
              "LSQUAREBRACKET",
              "IDENT(\"user_id\")",
              "COMMA",
              "IDENT(\"user_name\")",
              "RSQUAREBRACKET",
              "EOS"
            ]
          },
          {
            str: %{
              relation_type = base
              relation_type = derived { source = "users.user" }
              },
            desired_tokens: [
              "RELATION_TYPE",
              "EQUAL",
              "BASE",
              "RELATION_TYPE",
              "EQUAL",
              "DERIVED",
              "LBRACE",
              "SOURCE",
              "EQUAL",
              "IDENT(\"users.user\")",
              "RBRACE",
              "EOS"
            ]
          },
          {
            str: %{attributes{
                }},
            desired_tokens: [
              "ATTRIBUTES",
              "LBRACE",
              "RBRACE",
              "EOS"
            ]
          },
          {
            str: %{attribute "user_id" {
                }},
            desired_tokens: [
              "ATTRIBUTE",
              "IDENT(\"user_id\")",
              "LBRACE",
              "RBRACE",
              "EOS"
            ]
          },
          {
            str: %{
              required = true
              required = false
              },
            desired_tokens: [
              "REQUIRED",
              "EQUAL",
              "TRUE",
              "REQUIRED",
              "EQUAL",
              "FALSE",
              "EOS"
            ]
          },
          {
            str: %{
              field_type = measure
              field_type = dimension
              },
            desired_tokens: [
              "FIELD_TYPE",
              "EQUAL",
              "MEASURE",
              "FIELD_TYPE",
              "EQUAL",
              "DIMENSION",
              "EOS"
            ]
          },
          {
            str: %{
              data_type = bigint
              data_type = time
              data_type = timestamp
              data_type = timestamptz
              data_type = byte
              data_type = word
              data_type = integer
              data_type = float
              data_type = double
              data_type = boolean
              data_type = numeric(2, 3)
              data_type = varchar(100)
              },
            desired_tokens: [
              "DATA_TYPE",
              "EQUAL",
              "BIGINT",
              "DATA_TYPE",
              "EQUAL",
              "TIME",
              "DATA_TYPE",
              "EQUAL",
              "TIMESTAMP",
              "DATA_TYPE",
              "EQUAL",
              "TIMESTAMPTZ",
              "DATA_TYPE",
              "EQUAL",
              "BYTE",
              "DATA_TYPE",
              "EQUAL",
              "WORD",
              "DATA_TYPE",
              "EQUAL",
              "INTEGER",
              "DATA_TYPE",
              "EQUAL",
              "FLOAT",
              "DATA_TYPE",
              "EQUAL",
              "DOUBLE",
              "DATA_TYPE",
              "EQUAL",
              "BOOLEAN",
              "DATA_TYPE",
              "EQUAL",
              "NUMERIC",
              "LPAREN",
              "NUMBER(2)",
              "COMMA",
              "NUMBER(3)",
              "RPAREN",
              "DATA_TYPE",
              "EQUAL",
              "VARCHAR",
              "LPAREN",
              "NUMBER(100)",
              "RPAREN",
              "EOS"
            ]
          },
          {
            str: %{properties {
                }},
            desired_tokens: [
              "PROPERTIES",
              "LBRACE",
              "RBRACE",
              "EOS"
            ]
          },
          {
            str: %{
              label = "UserID"
              },
            desired_tokens: [
              "LABEL",
              "EQUAL",
              "IDENT(\"UserID\")",
              "EOS"
            ]
          },
          {
            str: %{
              default_aggregate = sum
              default_aggregate = count
              default_aggregate = distinct
              default_aggregate = avg
              default_aggregate = min
              default_aggregate = max
              },
            desired_tokens: [
              "DEFAULT_AGGREGATE",
              "EQUAL",
              "SUM",
              "DEFAULT_AGGREGATE",
              "EQUAL",
              "COUNT",
              "DEFAULT_AGGREGATE",
              "EQUAL",
              "DISTINCT",
              "DEFAULT_AGGREGATE",
              "EQUAL",
              "AVG",
              "DEFAULT_AGGREGATE",
              "EQUAL",
              "MIN",
              "DEFAULT_AGGREGATE",
              "EQUAL",
              "MAX",
              "EOS"
            ]
          },
          {
            str: %{
              expression = "users.user_id"
          expression = "md5(\"string with literal space and embedded quotes\")"
              expression = "md5(id || upper(users.user_name))"
              expression = "case when t.a = 0 then t.a else t.b end"
              },
            desired_tokens: [
              "EXPRESSION",
              "EQUAL",
              "IDENT(\"users.user_id\")",
              "EXPRESSION",
              "EQUAL",
          "IDENT(\"md5(\"string with literal space and embedded quotes\")\")",
              "EXPRESSION",
              "EQUAL",
              "IDENT(\"md5(id || upper(users.user_name))\")",
              "EXPRESSION",
              "EQUAL",
              "IDENT(\"case when t.a = 0 then t.a else t.b end\")",
              "EOS"
            ]
          },
          {
            str: %{
              # I am a comment
              },
            desired_tokens: [
              "EOS"
            ]
          }
        ]
      end

      # @return [Array<Hash>]
      def token_error_test_cases
        [
          {
            str: %{
                domain domain_name" {}
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema schema_name" {}
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation relation_name" {}
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      relation_typee = base
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      relation_type = base
                      partitionss = [user_id", "user_name"]
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributess {
                        attribute "attribute_name" {}
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attributee "attribute_name" {}
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          requiredd = false
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          relation_typee = measure
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          data_typee = bigint
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          field_typee = bigint
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          propertiess{}
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          expressionn = "user.userid"
                        }
                      }
                    }
                  }
                }
              }
          }
        ]
      end

      # @return [Array<Hash>]
      def parser_test_cases
      [
        # each hash is a test case with a str: and desired_ast:
        {
          str: %{domain "domain_name" {
                 schema "schema_name" {
                    relation "relation_name" {
                      relation_type = base
                    }
                 }
              }},
          desired_ast: [
            "[:domain_name, \"domain_name\"]",
            "[:schema_name, \"schema_name\"]",
            "[:relation_name, \"relation_name\"]",
            "[:relation_type, \"base\"]"
          ]
        },
        {
          str: %{domain "domain_name" {
                 schema "schema_name" {
                    relation "relation_name" {
                      relation_type = derived { source = "users.user" }
                    }
                 }
              }},
          desired_ast: [
            "[:domain_name, \"domain_name\"]",
            "[:schema_name, \"schema_name\"]",
            "[:relation_name, \"relation_name\"]",
            "[:relation_type, \"derived\", \"users.user\"]"
          ]
        },
        {
          str: %{domain "domain_name" {
                 schema "schema_name" {
                    relation "relation_name" {
                      relation_type = base
                      partitions = ["user_id", "user_name"]
                    }
                 }
              }},
          desired_ast: [
            "[:domain_name, \"domain_name\"]",
            "[:schema_name, \"schema_name\"]",
            "[:relation_name, \"relation_name\"]",
            "[:relation_type, \"base\"]",
            "[:partition, \"user_id\"]",
            "[:partition, \"user_name\"]"
          ]
        },
        {
          str: %{domain "domain_name" {
                 schema "schema_name" {
                    relation "relation_name" {
                      relation_type = base
                      partitions = []
                      attributes {
                        attribute "attribute_name" {
                          data_type = bigint
                          field_type = measure
                          required = true
                        }
                      }
                    }
                 }
              }},
          desired_ast: [
            "[:domain_name, \"domain_name\"]",
            "[:schema_name, \"schema_name\"]",
            "[:relation_name, \"relation_name\"]",
            "[:relation_type, \"base\"]",
            "[:partition]",
            "[:attribute_name, \"attribute_name\"]",
            "[:data_type, \"bigint\"]",
            "[:field_type, \"measure\"]",
            "[:required_type, \"true\"]"
          ]
        },
        {
          str: %{domain "domain_name" {
                 schema "schema_name" {
                    relation "relation_name" {
                      relation_type = base
                      partitions = []
                      attributes {
                        attribute "attribute_name" {
                          data_type = bigint
                          field_type = measure
                          required = true
                          properties {
                            label = "UserID"
                            default_aggregate = distinct
                          }
                        }
                      }
                    }
                 }
              }},
          desired_ast: [
            "[:domain_name, \"domain_name\"]",
            "[:schema_name, \"schema_name\"]",
            "[:relation_name, \"relation_name\"]",
            "[:relation_type, \"base\"]",
            "[:partition]",
            "[:attribute_name, \"attribute_name\"]",
            "[:data_type, \"bigint\"]",
            "[:field_type, \"measure\"]",
            "[:required_type, \"true\"]",
            "[:properties]",
            "[:label_item, \"UserID\"]",
            "[:default_aggregate, \"distinct\"]"
          ]
        },
        {
          str: %{domain "domain_name" {
                 schema "schema_name" {
                    relation "relation_name" {
                      relation_type = derived { source = "users.user" }
                      attributes {
                        attribute "user_id" { expression = "users.user_id" }
                      }
                    }
                 }
              }},
          desired_ast: [
            "[:domain_name, \"domain_name\"]",
            "[:schema_name, \"schema_name\"]",
            "[:relation_name, \"relation_name\"]",
            "[:relation_type, \"derived\", \"users.user\"]",
            "[:attribute_name, \"user_id\"]",
            "[:expression, \"users.user_id\"]"
          ]
        },
        {
          str: %{domain "domain_name" {
                 schema "schema_name" {
                    relation "relation_name" {
                      relation_type = derived { source = "users.user" }
                      attributes {
                        attribute "user_id" {
        expression = "md5('string with \}\}\}\} == spaces')" #this is a comment
                        }
                      }
                    }
                 }
              }},
          desired_ast: [
            "[:domain_name, \"domain_name\"]",
            "[:schema_name, \"schema_name\"]",
            "[:relation_name, \"relation_name\"]",
            "[:relation_type, \"derived\", \"users.user\"]",
            "[:attribute_name, \"user_id\"]",
            "[:expression, \"md5('string with }}}} == spaces')\"]"
          ]
        },
        {
          str: %{domain "domain_name" {
                 schema "schema_name" {
                    relation "relation_name" {
                      relation_type = derived { source = "users.user" }
                      attributes {
                        attribute "user_id" {
                          expression = "md5(id || upper(users.user_name))"
                        }
                      }
                    }
                 }
              }},
          desired_ast: [
            "[:domain_name, \"domain_name\"]",
            "[:schema_name, \"schema_name\"]",
            "[:relation_name, \"relation_name\"]",
            "[:relation_type, \"derived\", \"users.user\"]",
            "[:attribute_name, \"user_id\"]",
            "[:expression, \"md5(id || upper(users.user_name))\"]"
          ]
        },
        {
          str: %{domain "domain_name" {
                 schema "schema_name" {
                    relation "relation_name" {
                      relation_type = derived { source = "users.user" }
                      attributes {
                        attribute "user_id" {
                          expression = "case when t.a = 0 then t.a else t.b end"
                        }
                      }
                    }
                 }
              }},
          desired_ast: [
            "[:domain_name, \"domain_name\"]",
            "[:schema_name, \"schema_name\"]",
            "[:relation_name, \"relation_name\"]",
            "[:relation_type, \"derived\", \"users.user\"]",
            "[:attribute_name, \"user_id\"]",
            "[:expression, \"case when t.a = 0 then t.a else t.b end\"]"
          ]
        }
      ]
      end

      # @return [Array<Hash>]
      def parser_error_test_cases
        [
          {
            str: %{
                domain "domain_name" []
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" []
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" []
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      relation_type =[] base
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      relation_type = base
                      partitions = ["user_id"[] "user_name"]
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes []
                        attribute "attribute_name" {}

                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" []
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          required =[] false
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          relation_type =[] measure
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          data_type =[] bigint
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          field_type =[] bigint
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          properties []
                        }
                      }
                    }
                  }
                }
              }
          },
          {
            str: %{
                domain "domain_name" {
                  schema "schema_name" {
                    relation "relation_name" {
                      attributes {
                        attribute "attribute_name" {
                          expression   "user.userid"
                        }
                      }
                    }
                  }
                }
              }
          }
        ]
      end

      # performs the actual token test
      def test_get_token
        # iterate through all the test cases
        token_test_cases.each do |t|
          token = IR.new.get_token_str([t[:str]])
          assert_equal(
            t[:desired_tokens],
            token.map(&:to_s)
          )
        end
      end

      # performs the actual token test for error cases
      def test_get_token_error
        # iterate through all the test cases
        token_error_test_cases.each do |t|
          assert_raises(RLTK::LexingError) do
            IR.new.get_token_str([t[:str]])
          end
        end
      end

      # performs the actual parser test
      def test_get_ast
        parser_test_cases.each do |t|
          token = IR.new.get_token_str([t[:str]])
          ast = IR.new.get_ast(token).map(&:to_s)
          assert_equal(
            t[:desired_ast],
            ast
          )
        end
      end

      # performs the actual parser test for error cases
      def test_get_ast_error
        parser_error_test_cases.each do |t|
          token = IR.new.get_token_str([t[:str]])
          assert_raises(RLTK::NotInLanguage) do
            IR.new.get_ast(token)
          end
        end
      end
    end
  end
end
