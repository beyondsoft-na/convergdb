## ConvergDB Pyspark Library

This library is used to perform the actual ETL for ConvergDB deployments. The code can be used in AWS Glue, or locally. In theory it can be adapted to in any Pyspark environment.

### Building the zip file

In order to facilitate a clean build of the library packaged as a zip, use the `package.sh` script.

### Overview

This library is intended for use with ConvergDB, as it is tightly bound to the internal representation structure that is created in the ConvergDB binary. This structure is communicated to this library in a JSON format.

The `source_to_target` method accepts two parameters:

* Spark SQL context
* ConvergDB structure for a given relation (JSON)

Below is an example script that runs locally. Note the `local_header` import.

```
import sys
sys.path.insert(0, '/coding/glue-testing/convergdb.zip')
import convergdb
from convergdb.local_header import *

convergdb.source_to_target(
  sql_context(),
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
    "working_path": "/coding/demo/demo2"
  },
  "working_path": "/coding/demo/demo2",
  "deployment_id": "e969ca618e222a58",
  "region": "us-west-2",
  "sns_topic": "arn:aws:sns:us-west-2:692977618922:convergdb-e969ca618e222a58",
  "cloudwatch_namespace": "convergdb/e969ca618e222a58"
}
"""
)
```

### Using in AWS Glue

When running in AWS Glue, the only change is the header, which performs some Glue specific configuration to create the SQL context. Glue does not require any special `sys` handling for the zip library. Simply store the library in S3 and refer to it as a Python lib in the Glue job configuration.

```
import convergdb
from convergdb.glue_header import *

convergdb.source_to_target(
  sql_context(),
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
    "working_path": "/coding/demo/demo2"
  },
  "working_path": "/coding/demo/demo2",
  "deployment_id": "e969ca618e222a58",
  "region": "us-west-2",
  "sns_topic": "arn:aws:sns:us-west-2:692977618922:convergdb-e969ca618e222a58",
  "cloudwatch_namespace": "convergdb/e969ca618e222a58"
}
"""
)
```
