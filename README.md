<img src="https://github.com/beyondsoft-na/convergdb/blob/master/images/convergdb.png" alt="convergdb" width="800">

# DevOps for Data

## About this documentation

Hey there! Glad you're interested in ConvergDB! The documentation in this README is in it's early stages. Soon we will migrate the docs to the wiki which will be much better organized, with fun examples and everything!

## Fancy Features of ConvergDB

ConvergDB has many fancy features to sweeten your data experience! Here is a list of the current hot value props:

* Open source software - Free to use 
* Idempotent ETL processing
* Serverless architecture
* Leverage cloud services such as Amazon Glue, Amazon Athena, Amazon Redshift Spectrum 
* Automatic batching of large data sets to mitigate the cost impact of failures  
* Cloudwatch metrics, alerts, and monitoring dashboards are created automatically

## Data lake creator

The focus of ConvergDB is the creation and management of an S3 data lake, using serverless technologies and cloud best practices. 

In addition to creating and managing these lakes, ConvergDB intends to support integration of your S3 data into other tools, such as Redshift, Postgres, and even more exotic targets like Amazon Neptune. Support for these technologies will be based upon community and user needs.


## ConvergDB? Where is the database? Why are you converging it? What does that even mean?

With ConvergDB, your data lake **is** the DB. ConvergDB is a codified methodology, not a running application.

State [convergence](http://devopsdictionary.com/wiki/Convergence) is a common pattern in devops, meaning that with each successive run the configuration converges toward it's target state. This pattern is used in the ETL jobs created by ConvergDB, through the use of a decoupled, producer/consumer model. This approach prevents data duplication from double loading, provides resiliency, and allows for manipulation of the state if something does need to be fixed or reloaded.

## Installation

ConvergDB is a command line utility written as a ruby gem using [jruby](http://jruby.org/) which can be installed in several ways:

* **Jar file** - This is the quickest and easiest way to run ConvergDB, especially if you are not familiar with the subtleties of installing and managing jruby. All it requires is that you have java 1.8 installed on your machine.
* **Gem install** - COMING SOON
* **Build gem from source** - For advanced users and contributors.

In addition to the ConvergDB CLI tool, you will need the following:

* **Terraform** - ConvergDB uses Hashicorp Terraform to manage deployment. You will need to install Terraform locally on your machine. [https://www.terraform.io/intro/getting-started/install.html]()
* **AWS API credentials** In order to build anything in AWS, you will need AWS credentials (AWS\_ACCESS\_KEY\_ID, AWS\_SECRET\_ACCESS\_KEY). Note that ConvergDB uses the default credentials as resolved by the AWS SDK. If you install and configure the AWS CLI, then default profile will be used by ConvergDB. [https://docs.aws.amazon.com/cli/latest/userguide/installing.html]()

### Jar file install

The convergdb jar file provides all of the functionality of the software, bundled into a single file. If you have java installed on your system, the jar file is all you need.

The jar file is included in this repository. Simply right click on the jar file in the file listing above and select "Save link as.." or the equivalent command in your browser.

To invoke ConvergDB jar file from the command line:

```
$ java -jar convergdb-0.0.6.5.jar 
Commands:
  <script> generate        # generate
  <script> help [COMMAND]  # Describe available commands or one specific command
$
```

### Gem install - COMING SOON

ConvergDB is available via rubygems. You can install it using the `gem` or `bundle` utilities. 

```
$ gem install convergdb
```

With the gem installed, you can invoke ConvergDB directly:

```
$ convergdb
Commands:
  convergdb generate        # generate
  convergdb help [COMMAND]  # Describe available commands or one specific com...
```

NOTE: ConvergDB gem only supports jruby-9.1.x at this time.

### Build gem from source

Building the gem from source works like any gem build. First clone the ConvergDB repository, then run:

```
$ gem build convergdb.gemspec
```

## Designing a Data Lake with ConvergDB

ConvergDB configurations have a minimum of two files:

* Schema file (.schema extension) is used to define the relations in your environment.
* Deployment file (.deployment extension) maps the relations in your schema files to concrete cloud resources.

All of your schema and deployment files must be in the same directory. You can have as many of each file type as you like.

### Stanzas

ConvergDB is modeled after the HCL language used by Terraform. Note that it does not support features such as interpolation.

The key concept is that you will define schema objects and deployment configurations by way of nested stanzas. A stanza is represented as follows:

```
# this is a comment
stanza_type "identifier" {
  ... # attributes of stanza, which may contain nested stanzas
}
```

### Schema Files

Schema files contain technology agnostic definitions for your data. There are two types of relations:

* **base** - defines the field structure, data types, and transformations for the data in your source files.
* **derived** - defines the field structure, data types, and transformations that result in the target table, which is your data lake.


* **domain**: Each schema file can contain one or more domains. Domains are a top level namespace that is equivalent to "database" in most RDBMS systems.
* **schema**: Domains must contain one or more schemas, providing an additional layer of namespacing. 
* **relation**: Schemas must contain one or more relations. Akin to a *table* in relation databases 
* **attribute**: Relations must contain one or more attributes. Attributes are akin to a *column* in a relation database

Following snippet gives a skeleton of one schema definition. In ConvergDB, multiple schema files are allowed.

```
domain "string_expression" {
  schema "string_expression" {
    relation "string_expression" {
      relation_type = base | derived { source= "source_relation" } 
      partitions = ["attribute_name",...]
      attributes { 
        attribute "string_expression" {
          required = true|false
          data_type = date|time|timestamp|timestamptz|varchar(integer)
                    | byte|word|integer|bigint|float|double
                    | numeric(integer,integer)|boolean
          expression = expression syntax
        }
      }
    }
  }
}
```

Note that a string identifier has the following rules:

* Begin with a letter
* Can contain letters, numbers, or underscores


Each element above is defined in detail below:

--
#### domain

```
domain "string_expression" {
  ...
}
```

Domains must be named with a string identifier, and must contain at least 1 schema. You can have multiple domain stanzas with the same name, as long as the domain/schema/relation combination is unique across the deployment.

--
#### schema

```
schema "string_expression" {
  ...
}
```

Schemas must be named with a string identifier, and must contain at least 1 relation. You can have multiple schema stanzas with the same name, as long as the domain/schema/relation combination is unique across the deployment.

--
#### relation

```
relation "string_expression" {
  relation_type = base | derived { source= "source_relation" } 
  partitions = ["attribute_name",...]
  attributes { 
    ...
  }
}
```

Relations must be named with a string identifier.

Relations have the following parameters:

--
##### relation\_type = base | derived { source = "base\_relation" }

This is a required field for every relation.

If you are specifying a base relation, simply set this value to base. If you are specifying a derived relation, you must also specify the source, which is the name of a base relation.

When specifying the name of the base relation, the assumption is that the relation resides in the same schema and domain as the current, derived relation. You can optionally specify a schema, or a domain and schema to explicitly refer to a base relation in another namespace. Here are some examples:

```
relation_type = derived { source = "base_relation" }
...or...
relation_type = derived { source = "another_schema.base_relation" }
...or...
relation_type = derived { source = "another_domain.another_schema.base_relation" }
```

--
##### partitions = ["attribute_name",...]

Not required.

You can specify an ordered list of attributes used for partitioning the data. The attributes must exist in the current relation. Note that you can use any field in the relation for partitioning, including fields that have been calculated using expressions. 

NOTE: ConvergDB does not currently support changing the partition structure of your tables.

##### attributes { ... }

Every relation must have an `attributes` block containing at least one attribute. Attributes should be created as separate stanzas. You do not need to comma separate the attributes.

--
#### attribute

```
attribute "string_expression" {
  required = true|false

  data_type = date|time|timestamp|timestamptz|varchar(integer)
            | byte|word|integer|bigint|float|double
            | numeric(integer,integer)|boolean
  expression = expression syntax
}
```

--
##### required = true | false

Indicates whether or not this is a required field. During the ETL process, any record containing a null value for this field will be rejected.

--
##### data_type

This field must be one of the following SQL types:

* date
* time
* timestamp
* varchar(integer)
* byte
* word
* integer
* bigint
* float
* double
* numeric(integer,integer)
* boolean

--
#### expression = expression syntax

The expression field is used differently depending upon the type of relation:

**base relation expressions** 

Expressions in base relations refer to nested paths in the source data documents.

For example, if your JSON data is structured as follows:

```
{"data":{"attr1":22, "attr2":"hello"}}
```

...you can create an expression using dot notation to extract nested data.

```
attribute "greeting" {
  ...
  expression = "data.attr2"
}
```

The above definition will yield `hello` from the JSON document, and make it available to derived relations with the attribute name `greeting`.

JSON path expressions are case sensitive, and do not currently support nested array references.

**derived relation expressions**

Expressions in derived relations are SQL snippets that allow you to create a new value.

Using the `greeting` attribute example in the section above, we can create an attribute in a derived relation as follows:

```
attribute "fancy" {
  ...
  expression = "substring(greeting,1,2)"
}
```

...which will yield `he` as the output.

In the current version of ConvergDB, derived relation expressions support any Spark SQL function. Also, we do not yet validate that your function is correct. You will need to test your ETL jobs to be sure that they are syntactically corrected and supported.

NOTE: In a future version we will be defining an explicit subset of Spark SQL functions that we will support. Standard SQL functions can be used without worry, but if you use Spark specific functions they may not be supported.

### Deployment Files

Deployment files are used to create concrete implementations of relations that you have defined with schema files.

There are two stanzas that can be specified in the deployment file:

* `s3_source` - Indicates a base relation sourced from data in an S3 bucket.
* `athena` - Indicates a derived relation stored in S3 and accessed via Athena.

```
s3_source "environment_name" {
    relations {
        relation {
            dsd = "domain.schema.base_relation_name"
            storage_format  = "string" # "json"
            storage_bucket  = "string"
            inventory_table = "string"
            domain_name     = "string"
            schema_name     = "string"
            relation_name   = "string"
        }
    }
}

athena "environment_name" {
    etl_job_name = "demo_etl_job"
    etl_job_schedule = "cron expression"
    relations {
        relation {
            storage_format = "parquet"
            dsd = "domain.schema.derived_relation_name"
            use_inventory = "true"
            domain_name     = "string"
            schema_name     = "string"
            relation_name   = "string"
            dsd_prefix      = "string"
        }
    }
}
```

--
### s3\_source

This stanza defines an implementation of a base relation as an S3 source. This is done by marrying relations to S3 buckets and associated resources.

The stanza has the form:

```
s3_source "environment_name" {
  relations {
    ...
  }
}
```

Below is an example of a single table implemented as an s3\_source:

```
s3_source "production" {
  relations {
    relation {
      dsd = "ecommerce.inventory.books_source"
      storage_format = "json"
      storage_bucket = "books.example.beyondsoft.us"
    }
  }
}
```

Each aspect of the example (and more) is detailed in the following sections:

--
#### environment_name

Indicates the environment name, which is used as a top level namespace. 

See the section on namespacing for more information.

--
#### relations

Must contain one or more relation stanzas.

```
relations {
  ...
}
```

--
### Relation

`s3_source` relation definitions have the following form:

```
relation {
  # required
  dsd             = "domain.schema.relation"
  storage_format  = "json"
  storage_bucket  = "string"
  
  # optional
  relation_name   = "string"
  schema_name     = "string"
  domain_name     = "string"
  inventory_table = "athena_database_name.athena_table_name"
}
```

--
#### dsd = "string"

`dsd` indicates the schema object that is implemented as this relation. The object must be fully qualified as `domain.schema.relation`

--
#### storage_format = "json"

`storage_format` has the following supported format:

* `json` - Files of JSON (optionally compressed as `gz` or `bz2`) containing newline records as separate JSON objects.

New formats will be added soon, which is why you must specify `json` even though it is the only option.

--
#### storage_bucket = "string"

Bucket/prefix where the files can be found:

```
storage_bucket = "bucket.name/prefix/to/the/files"
```

Will search bucket `bucket.name` for any files matching prefix `prefix/to/the/files`. 

--
#### relation_name = "string"

This is an optional override to the relation name, which will be applied to the relation name portion of the specified `dsd`.

Given the following relation:

```
relation {
  dsd = "ecommerce.inventory.books"
  relation_name = "books23"
  storage_bucket ...
}
```

...the resulting relation name will be an implementation of `ecommerce.inventory.books` named `ecommerce.inventory.books23`.

See the section on namespacing for more information about when and why you would want to do this.

--
#### schema_name = "string"

This is an optional override to the schema name, which will be applied to the schema name portion of the specified `dsd`.

Given the following relation:

```
relation {
  dsd = "ecommerce.inventory.books"
  schema_name = "inventory_test"
  storage_bucket ...
}
```

...the resulting relation name will be an implementation of `ecommerce.inventory.books` named `ecommerce.inventory_test.books`.

See the section on namespacing for more information about when and why you would want to do this.

--
#### domain_name = "string"

This is an optional override to the schena name, which will be applied to the domain name portion of the specified `dsd`.

Given the following relation:

```
relation {
  dsd = "ecommerce.inventory.books"
  domain_name = "marketing"
  storage_bucket ...
}
```

...the resulting relation name will be an implementation of `ecommerce.inventory.books` named `marketing.inventory.books`.

See the section on namespacing for more information about when and why you would want to do this.

--
#### inventory\_table = "athena\_database\_name.athena\_table\_name"

Optionally use an Athena table built from an S3 inventory report. This is helpful when you have an incredibly large number of objects to compare to the control table. The comparison is used to determine which files need to be loaded at this time.

Read up on S3 inventory tables: [https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html](https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html).

ConvergDB will not create an inventory table for you as it must be created by the owner of the S3 bucket. It is best to use the Athena DDL in the instructions above to build the table. For best results, specify ORC as the output format.

### athena 

`athena` stanza implements derived relations as managed data lake tables.  The tables will be loaded with a single Glue ETL job, and exposed via Athena tables (well... really the Glue catalog underneath).
 
This stanza allows you to define:

* A named ETL job.
* A schedule for that ETL job
* the compute power of the ETL job
* A list of relations to be loaded, in the order specified, by said ETL job


```
athena "environment_name" {
    etl_job_name = "string"
    etl_job_schedule = "cron expression"
    relations {
        relation {
            # required
            storage_format = "parquet"
            dsd = "domain.schema.derived_relation_name"
            
            # optional
            use_inventory = "true"
            domain_name     = "string"
            schema_name     = "string"
            relation_name   = "string"
            dsd_prefix      = "string"
        }
    }
}
```

--
#### etl\_job\_name = "string"

Name for the ETL job. Must be unique in this ConvergDB deployment (and in AWS).

--
#### etl\_job\_schedule = "cron expression"

Cron expression defining the schedule that this job should execute, in UTC time. 

See the AWS page for more information: [https://docs.aws.amazon.com/glue/latest/dg/monitor-data-warehouse-schedule.html](https://docs.aws.amazon.com/glue/latest/dg/monitor-data-warehouse-schedule.html).

--
#### etl\_job\_dpu = "integer"

Optional parameter to specify DPU used to execute Glue ETL job. This can have a dramatic impact on your compute costs so be sure to read about it here: [https://aws.amazon.com/glue/pricing/](). 

Default = "2" (the minimum)

--
#### relations

Contains one or more relation stanzas:

```
relations {
  ...
}
```

### Relation

`athena` relation definitions have the following form:

```
relation {
  # required
  storage_format = "string" # parquet
  dsd            = "domain.schema.derived_relation_name"
    
  # optional
  storage_bucket         = "string"
  script_bucket          = "string"
  state_bucket           = "string"
  use_inventory          = "true"
  relation_name          = "string"
  schema_name            = "string"
  domain_name            = "string"
  source_relation_prefix = "string"
}
```

--
#### storage_format = "parquet"

Parquet is a columnar file format, and is currently the only output format available. This will change in the future, so "parquet" needs to be specified.

--
#### dsd = "domain.schema.derived_relation_name"

Indicates the derived relation definition to implement. Must be fully qualified as `domain.schema.relation`.

--
#### storage_bucket = "string"

Optionally specify an s3 bucket/prefix in the form: `bucket_name/path/to/prefix` for storage of the data files in this table.

This value defaults to the data bucket automaticlaly created by ConvergDB. If you do specify a value for `source_bucket`... you must create the bucket outside of ConvergDB (or in the Terraform bootstrap).

--
#### script_bucket = "string"

Optionally specify an s3 bucket/prefix in the form: `bucket_name/path/to/prefix` for storage of the Glue ETL script used to populate and manage this table.

This value defaults to the admin bucket automatically created by ConvergDB. If you do specify a value for `script_bucket`... you must create the bucket outside of ConvergDB (or in the Terraform bootstrap).

It is advised that you do not change this value unless you have a really good reason.

--
#### state_bucket = "string"

Optionally specify an s3 bucket/prefix in the form: `bucket_name/path/to/prefix` for storage of the state files used in the management of this table.

This value defaults to the admin bucket automatically created by ConvergDB. If you do specify a value for `state_bucket`... you must create the bucket outside of ConvergDB (or in the Terraform bootstrap).

It is advised that you do not change this value unless you have a really good reason.

--
#### use_inventory = "true"

Specify an S3 inventory table to use when determining which files need to be loaded. Must be fully qualified as `athena_database_name.athena_table_name`.

Read up on S3 inventory tables: [https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html](https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html).

--
#### relation_name = "string"

This is an optional override to the relation name, which will be applied to the relation name portion of the specified `dsd`.

Given the following relation:

```
relation {
  dsd = "ecommerce.inventory.books"
  relation_name = "books23"
  storage_bucket ...
}
```

...the resulting relation name will be an implementation of `ecommerce.inventory.books` named `ecommerce.inventory.books23`.

See the section on namespacing for more information about when and why you would want to do this.

--
#### schema_name = "string"

This is an optional override to the schena name, which will be applied to the schema name portion of the specified `dsd`.

Given the following relation:

```
relation {
  dsd = "ecommerce.inventory.books"
  schema_name = "inventory_test"
  storage_bucket ...
}
```

...the resulting relation name will be an implementation of `ecommerce.inventory.books` named `ecommerce.inventory_test.books`.

See the section on namespacing for more information about when and why you would want to do this.

--
#### domain_name = "string"

This is an optional override to the schema name, which will be applied to the domain name portion of the specified `dsd`.

Given the following relation:

```
relation {
  dsd = "ecommerce.inventory.books"
  domain_name = "marketing"
  storage_bucket ...
}
```

...the resulting relation name will be an implementation of `ecommerce.inventory.books` named `marketing.inventory.books`.

See the section on namespacing for more information about when and why you would want to do this.

--
#### source\_relation\_prefix = "string"

Optional attribute allows you to specify a prefix override for the source relation.

Source relations are defined in the schema file when `relation_type = derived { source = "base_relation"}`.

So... suppose that you are implementing a relation with the fully qualified name of `ecommerce.inventory.books`... which refers to base relation `ecommerce.inventory.books_source`.

Using namespace manipulation.. it may be that `ecommerce.inventory.books_source` was deployed as `new_ecommerce.inventory.books_source`. In that case... you can provide `source_relation_prefix = "new_ecommerce"` which will allow you to refer to the implemented base relation.

You can override the `domain`... `domain.schema`... or `domain.schema.relation`.

Essentially, this is the other side of namespace manipulation.

See the section on namespacing for more information about when and why you would want to do this.

--
## Namespaces

Every relation that is deployed in ConvergDB has a fully qualified name with four, dot-notated components: `environment.domain.schema.relation`. This is referred to as the **full relation name**.

These names must be unique across the environment.

Here are the components:

* `environment` - Environment is meant to distinguish between things like `production` and `development`. Environment is set by the deployment stanza and can *not* be overridden.
*  `domain` - Domain is equivalent to "database" in an RDBMS.
*  `schema` - Schema is equivalent to "schema" in an RDBMS.
*  `relation` - Relation is equivalent to "table" in an RDBMS.

The full relation name for the following relation...

```
s3_source "production" {
  relations {
    relation {
      dsd = "ecommerce.inventory.books_source"
      schema_name = "inventory2"
    }
  }
}
```

...is determined with the following steps:

1. combine `environment` and `dsd` from relation stanza: `production.ecommerce.inventory.books_source`
2. apply `domain_name`, `schema_name`, `relation_name` overrides where specified: `production.ecommerce.inventory2.books_source`

In our sample schema, `ecommerce.inventory.books` is a derived relation sourced from `ecommerce.inventory.books_source`. If we implement `ecommerce.inventory.books` as an `athena` relation, the source relation must be present, or we will receive an error. In our case above, the source relation does not exist *with it's original name* because we did a `schema_name` override to the namespace. In order to find the implementation of the source relation, we need to provide the same override, by way of the `source_relation_prefix`.

```
athena "production" {
  relations {
    relation {
      dsd = "ecommerce.inventory.books"
      source_relation_prefix = "domain.inventory2"
    }
  }
}
```

In the example above.. the full relation name of the table will be `production.ecommerce.inventory.books`. 

When ConvergDB looks up the source relation, it will search for `production.ecommerce.inventory2.books_source` because:

1. `source_relation_prefix` provided an override to the domain and schema, reflecting the override in the `s3_source` stanza.
2. the source relation must be in the same environment.

In our case, the relation will be found, and all will be well!

#### But... why?

Namespacing is really just a way to uniquely identify a relation in your cloud environment.

In most cases, namespace overrides should not be needed, but they may come in handy in the future if you want to make a copy of a table.. or a copy of an entire environment.

## Changing Things

At this time, the following things can not be changed once they have been implemented:

* `environment`
* `domain`
* `schema`
* `relation`
* `data_type`
* `partition` fields (and ordering)
* `storage_format`
* `storage_bucket`

You can add or remove fields, but you can not change their names. You can change expressions.

Any changes that you make will be applied to the data going forward, but not retroactively. If you want to retroactively reprocess your data, this can be done through manipulation of the control table, and removal of the data files.

(Hint: This would be a good time to rename your table with an override, even if just for testing purposes)

We have plans to support safe, yet automated rebuilding of tables based upon changes.

## Terraform

[Terraform](https://www.terraform.io/) is an elegant and powerful, open source tool created by [Hashicorp](https://www.hashicorp.com/). Terraform provides infrastructure as code, and has deep integration with cloud services such as AWS.

In order to use ConvergDB you do not need to learn a lot of terraform. We think you should! But... you can get away with only learning a few terraform commands.

### Terraform commands you should learn

1. `terraform init` - Initializes a terraform deployment that has never been used before.
2. `terraform get` - Loads any modules that this deployment is dependent upon.
3. `terraform plan` - Preview infrastructure changes before applying them.
4. `terraform apply` - Apply changes to infrastructure.

All of the above are used when deploying ConvergDB.

### Terraform deployments

A single ConvergDB configuration has two Terraform deployments, in separate folders:

* `bootstrap/` - This deployment creates objects required for the other Terraform deployment to run. It also creates and manages the S3 buckets that are used by ConvergDB as default data and state storage. 
* `terraform/` - Contains the "ephemeral" portion of the infrastructure, such as the ETL job and table definitions. 

--
#### `bootstrap/`

Terraform deployment used to bootstrap this ConvergDB configuration. Contains the definition for several key objects:

* **data bucket** Default S3 bucket used to store the output data for ConvergDB.
* **admin bucket** Default S3 bucket used to store the state and control data for each target table. Also contains the Terraform backend for the primary Terraform config.
* **lock table** DynamoDB table used by Terraform to provide a lock on the Terraform backend state file in S3.

Both buckets above default to versioning enabled. The attributes of this bucket should be managed in the bootstrap.

The important concept here is that this repository manages the *persistent* objects, such as those used for data storage. This is in contrast to the primary Terraform repo, which is used to manage *ephemeral* objects... such as ETL code and table definition metadata.

This deployment contains the locally managed state files. You should migrate the backend to S3 using Terraform.

NOTE: ConvergDB does not attempt to manage source data buckets through Terraform.

--
#### `terraform/`

This Terraform config is used to manage the *ephemeral* objects in the configuration. If you were to accidentally delete all objeects in this deployment, no data or state would be lost.

Here is a breakdown of the contents:

`aws.tf` - Connects this config to AWS and creates some deployment level objects.

`aws_glue/` - AWS Glue scripts and libraries for the ETL jobs.

`cloudformation/` AWS Cloudformation scripts used to manage the table deployments.

`deployment.tf.json` - This is the actual deployment of ConvergDB objects. All of these objects are created by custom modules.

`modules/` - Terraform modules for managing ConvergDB objects.

`terraform.tf.json` - Backend configuration, created by the bootstrap.

`variables.tf` - Deployment level variables, created by the bootstrap.

--
## State Handling 

Every relation has a `state_bucket` associated with it. This bucket defaults to the "admin_bucket" created by the bootstrap.

ConvergDB state files are used by the ETL jobs to provide idempotent processing. At key points in the job, the state file is updated to reflect what is coming next. Inconsistent state can be handled by the ETL job before proceeding. 

For example, if an ETL job fails mid-batch, the next run of the job will identify the files containing inconsistent data, delete them, reset the state to "success".. then proceed with retrying the load.

--
### State File Structure

ConvergDB tables have two aspects of state that are managed with JSON files in S3. Here is an example of the state for a given table, as shown with the AWS CLI:

```
$ aws s3 ls s3://convergdb-admin-777f57850c8dab54/777f57850c8dab54/state/production.customer.app_logs.app_10/ --recursive
2018-03-12 16:13:14     106577 777f57850c8dab54/state/production.customer.app_logs.app_10/control/20180312214503000.json.gz
2018-03-13 10:00:41      32236 777f57850c8dab54/state/production.customer.app_logs.app_10/control/20180313165227000.json.gz
2018-03-13 22:53:21      22209 777f57850c8dab54/state/production.customer.app_logs.app_10/control/20180314054604000.json.gz
2018-03-14 21:20:01      26367 777f57850c8dab54/state/production.customer.app_logs.app_10/control/20180315041010000.json.gz
2018-03-15 21:19:36      28807 777f57850c8dab54/state/production.customer.app_logs.app_10/control/20180316041127000.json.gz
2018-03-16 21:18:25      25554 777f57850c8dab54/state/production.customer.app_logs.app_10/control/20180317040903000.json.gz
2018-03-17 21:16:54      21265 777f57850c8dab54/state/production.customer.app_logs.app_10/control/20180318040904000.json.gz
2018-03-18 21:19:40      29049 777f57850c8dab54/state/production.customer.app_logs.app_10/control/20180319040853000.json.gz
2018-03-18 21:19:40       6597 777f57850c8dab54/state/production.customer.app_logs.app_10/state.json
```

`control/*.jzon.gz` contains file level control records for the `convergdb_batch_id` in the file name. These control files are accessed as an Athena table by the ETL job, and used to determine the which files need to be loaded.

`state.json` is written by the ETL job at significant points in the process. If the job fails while data is being written, the next run of the ETL job will identify the `convergdb_batch_id` of the failed batch from the state file, then use the information to determine which files need to be deleted before proceeding with a retry of the data load.

--
## License

ConvergDB - DevOps for Data
Copyright (C) 2018 Beyondsoft Consulting, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.


