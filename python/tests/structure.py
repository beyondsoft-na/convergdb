import json

def structure_1():
  return json.loads(
"""
{
  "generators": [
    "athena",
    "glue",
    "markdown_doc",
    "html_doc",
    "control_table"
  ],
  "full_relation_name": "production.ecommerce.inventory.books",
  "dsd": "ecommerce.inventory.books",
  "environment": "production",
  "domain_name": null,
  "schema_name": null,
  "relation_name": null,
  "service_role": "",
  "script_bucket": "convergdb-admin-e969ca618e222a58",
  "temp_s3_location": null,
  "storage_bucket": "convergdb-data-e969ca618e222a58/e969ca618e222a58/production.ecommerce.inventory.books",
  "state_bucket": "convergdb-admin-e969ca618e222a58",
  "storage_format": "parquet",
  "source_relation_prefix": null,
  "use_inventory": "true",
  "inventory_source": "s3",
  "etl_job_name": "nightly_batch",
  "etl_job_schedule": "cron(0 0 * * ? *)",
  "etl_job_dpu": 2,
  "attributes": [
    {
      "name": "item_number",
      "required": false,
      "expression": "item_number",
      "data_type": "integer",
      "field_type": null,
      "cast_type": "integer"
    },
    {
      "name": "title",
      "required": false,
      "expression": "title",
      "data_type": "varchar(100)",
      "field_type": null,
      "cast_type": "string"
    },
    {
      "name": "author",
      "required": false,
      "expression": "author",
      "data_type": "varchar(100)",
      "field_type": null,
      "cast_type": "string"
    },
    {
      "name": "price",
      "required": false,
      "expression": "price",
      "data_type": "numeric(10,2)",
      "field_type": null,
      "cast_type": "decimal(10,2)"
    },
    {
      "name": "part_id",
      "required": false,
      "expression": "substring(md5(title),1,1)",
      "data_type": "varchar(100)",
      "field_type": null,
      "cast_type": "string"
    },
    {
      "name": "retail_markup",
      "required": false,
      "expression": "price * 0.25",
      "data_type": "numeric(10,2)",
      "field_type": null,
      "cast_type": "decimal(10,2)"
    },
    {
      "name": "source_file",
      "required": false,
      "expression": "convergdb_source_file_name",
      "data_type": "varchar(100)",
      "field_type": null,
      "cast_type": "string"
    }
  ],
  "partitions": [
    "part_id"
  ],
  "relation_type": 1,
  "source_dsd_name": "ecommerce.inventory.books_source",
  "full_source_relation_name": "production.ecommerce.inventory.books_source",
  "source_structure": {
    "generators": [
      "s3_source",
      "markdown_doc",
      "html_doc"
    ],
    "dsd": "ecommerce.inventory.books_source",
    "full_relation_name": "production.ecommerce.inventory.books_source",
    "environment": "production",
    "domain_name": null,
    "schema_name": null,
    "relation_name": null,
    "storage_bucket": "demo-source-us-west-2.beyondsoft.us",
    "storage_format": "json",
    "inventory_table": "s3_inventory.production__ecommerce__inventory__books_source",
    "streaming_inventory": "false",
    "streaming_inventory_output_bucket": null,
    "streaming_inventory_table": null,
    "attributes": [
      {
        "name": "item_number",
        "required": false,
        "expression": null,
        "data_type": "integer",
        "field_type": null,
        "cast_type": "integer"
      },
      {
        "name": "title",
        "required": false,
        "expression": null,
        "data_type": "varchar(100)",
        "field_type": null,
        "cast_type": "string"
      },
      {
        "name": "author",
        "required": false,
        "expression": null,
        "data_type": "varchar(100)",
        "field_type": null,
        "cast_type": "string"
      },
      {
        "name": "price",
        "required": false,
        "expression": null,
        "data_type": "numeric(10,2)",
        "field_type": null,
        "cast_type": "decimal(10,2)"
      },
      {
        "name": "stock",
        "required": true,
        "expression": null,
        "data_type": "integer",
        "field_type": null,
        "cast_type": "integer"
      }
    ],
    "partitions": [

    ],
    "relation_type": 0,
    "source_dsd_name": null,
    "working_path": "/coding/demo/demo2"
  },
  "control_table": "convergdb_control_e969ca618e222a58.production__ecommerce__inventory__books",
  "working_path": "/coding/demo/demo2",
  "deployment_id": "e969ca618e222a58",
  "region": "us-west-2",
  "sns_topic": "arn:aws:sns:us-west-2:692977618922:convergdb-e969ca618e222a58",
  "cloudwatch_namespace": "convergdb/e969ca618e222a58"
}
"""
  )

def structure_2():
  return json.loads(
"""
{
  "generators": [
    "athena",
    "glue",
    "markdown_doc",
    "html_doc",
    "control_table"
  ],
  "full_relation_name": "production.ecommerce.inventory.books",
  "dsd": "ecommerce.inventory.books",
  "environment": "production",
  "domain_name": null,
  "schema_name": null,
  "relation_name": null,
  "service_role": "",
  "script_bucket": "convergdb-admin-e969ca618e222a58",
  "temp_s3_location": null,
  "storage_bucket": "convergdb-data-e969ca618e222a58/e969ca618e222a58/production.ecommerce.inventory.books",
  "state_bucket": "convergdb-admin-e969ca618e222a58",
  "storage_format": "parquet",
  "source_relation_prefix": null,
  "use_inventory": "false",
  "inventory_source": "streaming",
  "etl_job_name": "nightly_batch",
  "etl_job_schedule": "cron(0 0 * * ? *)",
  "etl_job_dpu": 2,
  "attributes": [
    {
      "name": "item_number",
      "required": false,
      "expression": "item_number",
      "data_type": "integer",
      "field_type": null,
      "cast_type": "integer"
    },
    {
      "name": "title",
      "required": false,
      "expression": "title",
      "data_type": "varchar(100)",
      "field_type": null,
      "cast_type": "string"
    },
    {
      "name": "author",
      "required": false,
      "expression": "author",
      "data_type": "varchar(100)",
      "field_type": null,
      "cast_type": "string"
    },
    {
      "name": "price",
      "required": false,
      "expression": "price",
      "data_type": "numeric(10,2)",
      "field_type": null,
      "cast_type": "decimal(10,2)"
    },
    {
      "name": "part_id",
      "required": false,
      "expression": "substring(md5(title),1,1)",
      "data_type": "varchar(100)",
      "field_type": null,
      "cast_type": "string"
    },
    {
      "name": "retail_markup",
      "required": false,
      "expression": "price * 0.25",
      "data_type": "numeric(10,2)",
      "field_type": null,
      "cast_type": "decimal(10,2)"
    },
    {
      "name": "source_file",
      "required": false,
      "expression": "convergdb_source_file_name",
      "data_type": "varchar(100)",
      "field_type": null,
      "cast_type": "string"
    }
  ],
  "partitions": [
    "part_id"
  ],
  "relation_type": 1,
  "source_dsd_name": "ecommerce.inventory.books_source",
  "full_source_relation_name": "production.ecommerce.inventory.books_source",
  "source_structure": {
    "generators": [
      "s3_source",
      "markdown_doc",
      "html_doc"
    ],
    "dsd": "ecommerce.inventory.books_source",
    "full_relation_name": "production.ecommerce.inventory.books_source",
    "environment": "production",
    "domain_name": null,
    "schema_name": null,
    "relation_name": null,
    "storage_bucket": "demo-source-us-west-2.beyondsoft.us",
    "storage_format": "json",
    "inventory_table": "s3_inventory.production__ecommerce__inventory__books_source",
    "streaming_inventory": "true",
    "streaming_inventory_output_bucket": "streaming_inventory_bucket",
    "streaming_inventory_table": "streaming_inventory_table",
    "csv_header": "false",
    "csv_separator": 124,
    "csv_quote": 34,
    "csv_null": 0,
    "csv_escape": 92, 
    "csv_trim": "false",
    "attributes": [
      {
        "name": "item_number",
        "required": false,
        "expression": null,
        "data_type": "integer",
        "field_type": null,
        "cast_type": "integer"
      },
      {
        "name": "title",
        "required": false,
        "expression": null,
        "data_type": "varchar(100)",
        "field_type": null,
        "cast_type": "string"
      },
      {
        "name": "author",
        "required": false,
        "expression": null,
        "data_type": "varchar(100)",
        "field_type": null,
        "cast_type": "string"
      },
      {
        "name": "publisher",
        "required": false,
        "expression": null,
        "data_type": "varchar(100)",
        "field_type": null,
        "cast_type": "string"
      },
      {
        "name": "genre",
        "required": false,
        "expression": null,
        "data_type": "varchar(100)",
        "field_type": null,
        "cast_type": "string"
      },
      {
        "name": "price",
        "required": false,
        "expression": null,
        "data_type": "numeric(10,2)",
        "field_type": null,
        "cast_type": "decimal(10,2)"
      }
    ],
    "partitions": [

    ],
    "relation_type": 0,
    "source_dsd_name": null,
    "working_path": "/coding/demo/demo2"
  },
  "control_table": "convergdb_control_e969ca618e222a58.production__ecommerce__inventory__books",
  "working_path": "/coding/demo/demo2",
  "deployment_id": "e969ca618e222a58",
  "region": "us-west-2",
  "sns_topic": "arn:aws:sns:us-west-2:692977618922:convergdb-e969ca618e222a58",
  "cloudwatch_namespace": "convergdb/e969ca618e222a58"
}
"""
  )
