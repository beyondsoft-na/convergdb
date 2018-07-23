convergdb.source_to_target(
  sql_context(),
"""
{
  "generators": [
    "athena",
    "glue",
    "fargate",
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
  "service_role": "glueService",
  "script_bucket": "demo-utility-us-east-2.beyondsoft.us",
  "temp_s3_location": null,
  "storage_bucket": "demo-target-us-east-2.beyondsoft.us",
  "state_bucket": "demo-state-us-east-2.beyondsoft.us",
  "storage_format": "parquet",
  "source_relation_prefix": null,
  "etl_job_name": "nightly_batch",
  "etl_job_schedule": "cron(0 0 * * ? *)",
  "etl_job_dpu": null,
  "etl_technology": "aws_fargate",
  "etl_docker_image": "beyondsoftna/convergdb",
  "etl_docker_image_digest": "abc123",
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
      "name": "unique_id",
      "required": false,
      "expression": "concat('book-',md5(title))",
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
    }
  ],
  "partitions": [

  ],
  "relation_type": 1,
  "source_dsd_name": "ecommerce.inventory.books_source",
  "full_source_relation_name": "production.ecommerce.inventory.books_source",
  "source_structure": {
    "generators": [
      "streaming_inventory",
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
    "storage_bucket": "demo-source-us-east-2.beyondsoft.us",
    "storage_format": "json",
    "inventory_table": "",
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
        "required": false,
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
    "working_path": "/tmp"
  },
  "control_table": "convergdb_control_${deployment_id}.production__ecommerce__inventory__books",
  "working_path": "/tmp",
  "deployment_id": "${deployment_id}",
  "region": "${region}",
  "sns_topic": "${sns_topic}",
  "cloudwatch_namespace": "${cloudwatch_namespace}"
}
"""
)