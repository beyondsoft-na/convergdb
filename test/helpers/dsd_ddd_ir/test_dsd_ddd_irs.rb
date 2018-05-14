module ConvergDB
  module Generators
    module TestIR
      def self.dsd_ddd_test_01
        {
          table_name: 'target_table',
          database_name: 'test_database',
          temp_location: 's3://fakedata.beyondsoft.us/test-ddl-output/',
          attributes: [
            {name: 'title', data_type: 'varchar(100)', expression: 'title' },
            {name: 'author', data_type: 'varchar(100)', expression: 'author' },
            {name: 'publisher', data_type: 'varchar(100)', expression: 'publisher' },
            {name: 'genre', data_type: 'varchar(100)', expression: 'genre' }
          ],
          storage: {
            format: 'parquet',
            location: 'fakedata-target.beyondsoft.us'
          },
          source: {
            format: 'json',
            location: 'fakedata-source.beyondsoft.us'
          },
          state: {
            location: 'fakedata-state.beyondsoft.us'
          },
          temp: {
            location: 'fakedata.beyondsoft.us/temp/'
          },
          tblproperties: {
            classification: 'parquet'
          }
        }
      end
      
      def self.dsd_ddd_test_02
        {
          full_relation_name: "production.test_database.test_schema.books_target",
          dsd: 'test_database.test_schema.books_target',
          namespace: "production",
          database_name: "test_database",
          schema_name: "test_schema",
          relation_name: "books_target",
          generators: [:athena, :glue], # indicates which structure to pass to as parameters
          region: "us-west-2",
          service_role: "glueService",
          script_bucket: "fakedata.beyondsoft.us",
          temp_s3_location: "s3://fakedata.beyondsoft.us/temp/", # defaulted via tf
          storage_bucket: "fakedata-target.beyondsoft.us", # defaulted via tf
          state_bucket: "fakedata-state.beyondsoft.us", # defaulted via tf
          storage_format: "parquet", # defaulted via tf
          source_bucket: "fakedata-source.beyondsoft.us", # dep res pass
          source_type: "json", # dep res pass
          working_path: './', # from cli
          etl_job_name: 'test_etl_job',
          etl_job_schedule: 'cron(0 * * * ? *)',
          partitions: [],
          attributes: [
            {
              name: "title",
              data_type: "varchar(100)",
              expression: "title"
            },
            {
              name: "author",
              data_type: "varchar(100)",
              expression: "author"
            },
            {
              name: "publisher",
              data_type: "varchar(100)",
              expression: "publisher"
            },
            {
              name: "genre",
              data_type: "varchar(100)",
              expression: "genre"
            }
          ],
          resolved: true,
          depends_on: ["production.test_database.test_schema.books_source"]
        }
      end

      def self.dsd_ddd_test_s3_source
        {
          full_relation_name: "production.test_database.test_schema.books",
          dsd: 'test_database.test_schema.books',
          namespace: "production",
          database_name: "test_database",
          schema_name: "test_schema",
          relation_name: "books",
          generators: [:streaming_inventory, :s3_source], # indicates which structure to pass to as parameters
          storage_bucket: "fakedata-source.beyondsoft.us", # defaulted via tf
          streaming_inventory: 'true',
          streaming_inventory_output_bucket: 'bucket/location',
          storage_format: "json", # defaulted via tf
          attributes: [
            {
              name: "title",
              data_type: "varchar(100)",
              expression: "title"
            },
            {
              name: "author",
              data_type: "varchar(100)",
              expression: "author"
            },
            {
              name: "publisher",
              data_type: "varchar(100)",
              expression: "publisher"
            },
            {
              name: "genre",
              data_type: "varchar(100)",
              expression: "genre"
            }
          ],
          resolved: true
        }
      end
      
      # has multiple generator input structures
      def self.dsd_ddd_test_03
        {
          "production.test_database.test_schema.books_target" => dsd_ddd_test_02
        }
      end
    end
  end
end

# require 'json'
# 
# puts JSON.pretty_generate(ConvergDB::Generators::TestIR.dsd_ddd_test_03)