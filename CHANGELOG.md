## Changelog

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
