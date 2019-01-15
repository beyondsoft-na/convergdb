## Changelog

### 0.0.6.14 2019-01-14

* integrated handling for runtime when AWS connection can not be created.
* add `spark_partition_count` attribute for athena relations to allow for manual override of spark partitions in glue cluster. 

### 0.0.6.13 2018-12-18

* fixed convergdb.zip

### 0.0.6.12 2018-12-18

* broke schema parser into multiple chunks instead of parsing all files as one

### 0.0.6.11 2018-11-29

* functionality to accept environment variables in deployment file.

### 0.0.6.10 2018-11-14

* python api based diff will ignore pathnames that end in a backslash, in order to handle edge cases where a folder appears as an object.

### 0.0.6.9 2018-11-07

* changed convergdb\_terraform commit ID to reflect SSE changes in convergdb\_bootstrap.

### 0.0.6.8 2018-10-30

* fixed parser issue which degraded performance and required very large heap size

### 0.0.6.7 2018-05-21

* moved terraform modules to external repo
* `aws_fargate` option for `etl_technology`
* fargate integration bootstrap changes
* refactored bootstrap
* relative paths in terraform deployment to support devops pipeline
* rebuilt diff to AWS functionality
* added support for CSV files
* revamped pyspark schema handling for nested JSON objects
* added version command

### 0.0.6.6 2018-05-13

* added check for "canonical" dsd name in primary representation
* integrated streaming inventory option for S3 sources
* migrate updated pyspark library into repository
* minor cosmetic updates to thor output
* rebuilt integration tests
* reintegrated schema pyspark schema definition to prevent inferSchema from running
* expiration policy for /tmp objects in admin s3 bucket default to 3 days
* expiration policy for old versions in data s3 bucket default to 7 days

### 0.0.6.5 2018-03-30

* added license command
* added ConvergDB AGPLV3 license and included verbiage in every source file
* included licenses for dependent software

### 0.0.6.4 2018-03-15

* batched delete of files leftover from failed jobs
* refined the calculations for cluster memory
* reduced the max number of partitions coalesce will create
* added --conf spark.yarn.executore.memoryOverhead per AWS suggestion to glue module

### 0.0.6.3 2018-03-07

* pyspark logging less verbose (no long file lists)
* ability to parse athena query results directly from S3 csv (as opposed to paginated calls to API)

### 0.0.6.2 2018-02-24

* deployment id now comes from bootstrap, allowing for full destroy/rebuild of ephemeral objects
* athena queries in pyspark now operate in a retry loop in order to weather resource based failures
* coalesce moved to end of pyspark transform in order to maximize CPU usage

### 0.0.6.1 2018-02-21

* fix performance bug in ddd parser
* added md5 hashing of table comments in glue catalog to handling long expressions
* error handling and propagation changes to pyspark

### 0.0.6 2018-02-20

* removed RDDs from pyspark processing
* integrated s3 inventory handling into pyspark and ddd
* generalized pyspark error handling
* integrated cloudwatch metrics
* integrated SNS notifications
* added cloudwatch dashboard for summary and data volume metrics

### 0.0.5 2018-02-13

* a lot of work on pyspark library
  * updated state handling
  * removed accumulators that aren't working in Glue
* ruby unit tests updated
* fixed decimal storage bug in parquet output to support Redshift Spectrum and Athena better

### 0.0.4.9 2018-02-09

* fixed unit test that was failing pipeline
* added `etl_job_dpu` option for athena deployment

### 0.0.4.8 2018-02-08

* caught up on A LOT of unit tests since 0.0.3.
* pyspark library upgraded to include much richer functionality
  * JSON based control table accessed via Athena
  * nested JSON source data support (no arrays yet)
  * partitioned output
  * smaller batch size optimizations for large transforms (history)
  * entire relation structure is passed to pyspark
* a lot of functional refactoring
* attempts to isolate methods that depend on external resources (AWS)
* introduction of bootstrap terraform configuration

### 0.0.3 2017-10-10

* integrated newer design for IR structures into athena and glue generators

### 0.0.2 2017-10-09

* fixed bad athena DDL path without /

### 0.0.1 2017-10-09

* minimal working prototype
* accepts a JSON structure as input
* creates the following artifacts
  * pyspark script for use in Glue ETL job
  * python script create glue job
  * python script to create athena table
* all tests passing
* generated code runs successfully in AWS
