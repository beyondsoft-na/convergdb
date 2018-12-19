require 'simplecov'
SimpleCov.start if ENV["COVERAGE"]

require 'minitest'
require 'minitest/autorun'

require_relative '../lib/ir/ddd/ddd_ir.rb'
require_relative '../lib/ir/ddd/lexer.rb'
require_relative '../lib/ir/ddd/ast.rb'
require_relative '../lib/ir/ddd/parser.rb'

module ConvergDB
  module DDD
    class TestDDDParser < Minitest::Test
      # @return [Array<Hash>]
      def token_test_cases
        [
          # each hash is a test case with a str: and desired_tokens:
          # {
          #   str: %{athena "ns" {
          #       }},
          #   desired_tokens: [
          #     "RIDENT(athena)",
          #     "IDENT(\"ns\")",
          #     "LBRACE",
          #     "RBRACE",
          #     "EOS"
          #   ]
          # },
          # {
          #   str: %{s3_source "ns" {
          #       }},
          #   desired_tokens: [
          #     "RIDENT(s3_source)",
          #     "IDENT(\"ns\")",
          #     "LBRACE",
          #     "RBRACE",
          #     "EOS"
          #   ]
          # },
          {
            str: %{
              region = "us-west-2"
              domain_name = "domainname"
              schema_name = "schemaname"
              service_role = "glueService"
              script_bucket = "bucket-name"
              temp_s3_location = "s3://bucket/location"
              storage_format = "parquet" # optional
              etl_job_name = "etl_job"
              etl_job_schedule = "cron(0 0 * \" # * ? *)\""
              storage_bucket = "storage-bucket"
              state_bucket = "state-bucket"
              relation_name = "relation2"
              source_relation_prefix = "ns.db.schema"
              },
            desired_tokens: [
              "RIDENT(region)",
              "EQUAL",
              "IDENT(\"us-west-2\")",
              "RIDENT(domain_name)",
              "EQUAL",
              "IDENT(\"domainname\")",
              "RIDENT(schema_name)",
              "EQUAL",
              "IDENT(\"schemaname\")",
              "RIDENT(service_role)",
              "EQUAL",
              "IDENT(\"glueService\")",
              "RIDENT(script_bucket)",
              "EQUAL",
              "IDENT(\"bucket-name\")",
              "RIDENT(temp_s3_location)",
              "EQUAL",
              "IDENT(\"s3://bucket/location\")",
              "RIDENT(storage_format)",
              "EQUAL",
              "IDENT(\"parquet\")",
              "RIDENT(etl_job_name)",
              "EQUAL",
              "IDENT(\"etl_job\")",
              "RIDENT(etl_job_schedule)",
              "EQUAL",
              "IDENT(\"cron(0 0 * \" # * ? *)\"\")",
              "RIDENT(storage_bucket)",
              "EQUAL",
              "IDENT(\"storage-bucket\")",
              "RIDENT(state_bucket)",
              "EQUAL",
              "IDENT(\"state-bucket\")",
              "RIDENT(relation_name)",
              "EQUAL",
              "IDENT(\"relation2\")",
              "RIDENT(source_relation_prefix)",
              "EQUAL",
              "IDENT(\"ns.db.schema\")",
              "EOS"
            ]
          }
        ]
      end

      # @return [Array<Hash>]
      def parser_error_test_cases
        [
          {
            str: %{
                athena "ns" a_12
              }
          },
          {
            str: %{
                athena "ns" {
                  region  "us-west-2"
                }
              }
          },
          {
            str: %{
                athena "ns" {
                  relations {}
                    relation {}
                }
              }
          },
          {
            str: %{
                athena "ns" {
                  relations {
                    relat11ion
                  }
                }
              }
          },
          {
            str: %{
                athena "ns" {
                  relations {
                    relation {
                      temp_s3_location  "s3://bucket/name"
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
          # {
          #   str: %{athena "ns" {
          #       }},
          #   desired_ast: [
          #     "[:new_deployment, :athena, \"ns\"]"
          #   ]
          # },
          # {
          #   str: %{s3_source "ns" {
          #       }},
          #   desired_ast: [
          #     "[:new_deployment, :s3_source, \"ns\"]"
          #   ]
          # },
          {
            str: %{athena "ns" {
                  region = "us-west-2"
                }},
            desired_ast: [
              "[:new_deployment, :athena, \"ns\"]",
              "[:deployment, :region, \"us-west-2\"]"
            ]
          },
          {
            str: %{athena "ns" {
                  domain_name = "domain_name"
                  schema_name = "schema_name"
                  service_role = "glueService"
                  script_bucket = "bucket-name"
                  temp_s3_location = "s3://bucket/location"
                  storage_format = "parquet" # optional
                  etl_job_name = "etl_job"
                  etl_job_schedule = "cron(0 0 * \" # * ? *)\""
                  source_relation_prefix = "ns.db.schema"
                }},
            desired_ast: [
              "[:new_deployment, :athena, \"ns\"]",
              "[:deployment, :domain_name, \"domain_name\"]",
              "[:deployment, :schema_name, \"schema_name\"]",
              "[:deployment, :service_role, \"glueService\"]",
              "[:deployment, :script_bucket, \"bucket-name\"]",
              "[:deployment, :temp_s3_location, \"s3://bucket/location\"]",
              "[:deployment, :storage_format, \"parquet\"]",
              "[:deployment, :etl_job_name, \"etl_job\"]",
          "[:deployment, :etl_job_schedule, \"cron(0 0 * \\\" # * ? *)\\\"\"]",
              "[:deployment, :source_relation_prefix, \"ns.db.schema\"]"
            ]
          },
          {
            str: %{athena "ns" {
                   relations {
                      relation {
                        dsd = "dbname.schema.dsdrelation"
                        domain_name = "db2"
                        schema_name = "schema2"
                        relation_name = "relation2"
                        service_role = "glue2"
                        script_bucket = "script-bucket"
                        temp_s3_location = "s3://bucket/name"
                        storage_bucket = "storage-bucket"
                        state_bucket = "state-bucket"
                        storage_format = "json"
                        source_relation_prefix = "ns2.db3"
                      }
                    }
                }},
            desired_ast: [
              "[:new_deployment, :athena, \"ns\"]",
              "[:new_relation]",
              "[:relation, :dsd, \"dbname.schema.dsdrelation\"]",
              "[:relation, :domain_name, \"db2\"]",
              "[:relation, :schema_name, \"schema2\"]",
              "[:relation, :relation_name, \"relation2\"]",
              "[:relation, :service_role, \"glue2\"]",
              "[:relation, :script_bucket, \"script-bucket\"]",
              "[:relation, :temp_s3_location, \"s3://bucket/name\"]",
              "[:relation, :storage_bucket, \"storage-bucket\"]",
              "[:relation, :state_bucket, \"state-bucket\"]",
              "[:relation, :storage_format, \"json\"]",
              "[:relation, :source_relation_prefix, \"ns2.db3\"]"
            ]
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

      # performs the actual parser test for error cases
      def test_get_ast_error
        parser_error_test_cases.each do |t|
          token = IR.new.get_token_str([t[:str]])
          assert_raises(RLTK::NotInLanguage) do
            IR.new.get_ast(token)
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
    end
  end
end
