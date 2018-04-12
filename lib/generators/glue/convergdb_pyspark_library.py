# ConvergDB - DevOps for Data
# Copyright (C) 2018 Beyondsoft Consulting, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# glue header
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from awsglue.job import Job
import json
import boto3
import time
import cStringIO
import gzip

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
sq = SQLContext(sc)


# !PYSPARK FUNCTIONS
import gc

# simple logging... makes our entries easier to find in cloudwatch logs
def convergdb_log(entry):
  print("[CONVERGDB] " + entry)

# this is important for handling of decimal types.
# parquet needs the legacy format to work well in spectrum and athena
sq.setConf("spark.sql.parquet.writeLegacyFormat", "true")

# these are helpful when problems arise...
convergdb_log("python version: " + str(sys.version))
convergdb_log("spark version: " + str(sc.version))

from pyspark.sql.functions import lit, when, udf
from pyspark.sql.functions import input_file_name

# creates all of the functional rdd/dataframe layers then
# triggers the actual data transformation.
def data_load(sc, structure, s3a_paths, source_map_func, batch_id, total_bytes, file_count, dpu):
  convergdb_log("starting data load for " + structure["full_relation_name"])
  st = time.time()

  # determine the number of spark partitions to use for this batch.
  spark_partitions = calculate_spark_partitions(
    structure,
    total_bytes,
    dpu
  )

  # create a data frame from the source data.
  # dataframe will be all strings at this point.
  d1 = json_file_to_df(
    sc,
    s3a_paths
  )

  d2 = apply_expressions(
    d1,
    structure["source_structure"]
  )

  d3 = source_file_name(d2)

  # cast to the data type specified in the source relation
  d4 = apply_casting(
    d3,
    structure["source_structure"]
  )

  # reject any records where required fields are null in source relation
  d5 = null_reject(
    d4,
    structure["source_structure"]
  )

  d6 = d5.withColumn(
    "convergdb_source_file_name",
    input_file_name()
  )

  # apply expressions from target relation
  d7 = apply_expressions(
    d6,
    structure
  )

  # cast to data type specified in target relation
  d8 = apply_casting(
    d7,
    structure
  )

  # reject any records where required fields are null in target relation
  d9 = null_reject(
    d8,
    structure
  )

  d10 = d9.coalesce(
    spark_partitions
  )

  # add convergdb_batch_id to the data rows
  d11 = apply_housekeeping_fields(
    d10,
    batch_id
  )

  # output a plan for reference
  d11.explain(True)

  write_partitions(d11, structure)

  et = time.time()

  convergdb_log("data load completed in " + str(et - st) + " seconds")
  convergdb_log("bytes loaded: " + str(total_bytes) + " (compressed)")
  convergdb_log("files loaded: " + str(file_count))

  # explicit clean up
  for ref in [d1, d2, d3, d4, d5, d6, d7, d7, d9, d10, d11]:
    del ref
  gc.collect()

# accepts a dataframe object, and a dict for a given attribute (column).
# returns a dataframe column reference with casting applied.
# this reference is suitable for use in a df.select().
def casted_attribute(df, attribute):
  return df[attribute["name"]].cast(
    attribute["cast_type"]
  ).alias(
    attribute["name"]
  )

# accepts a SparkContext and a file_path to JSON files.
# returns a dataframe created from the JSON file specified.
# note that the dataframe has all original data types.
def json_file_to_df(sc, file_paths):
  convergdb_log("defining dataframe from JSON source with extract function applied...")
  return sq.read.json(
    list(file_paths)
  )

# applies casting to the dataframe, based upon the attributes in params.
def apply_casting(df, params):
  # note that params is a convergdb structure, but it is generalized
  # because it can be used with source and target relations.
  convergdb_log("defining data type casting...")
  return df.select(
    *[casted_attribute(df, c) for c in params["attributes"]]
  )

# text format for attribute expression, to be used in selectExpr.
def expression_text(attribute):
  exp = attribute["expression"]
  if exp == None:
    exp = attribute["name"]
  return exp + " as " + attribute["name"]

# applies expressions to provided dataframe, based upon attributes in params.
def apply_expressions(df, params):
  convergdb_log("defining target attribute expressions...")
  return df.selectExpr(
    *[expression_text(c) for c in params["attributes"]]
  )

def source_file_name(df):
  return df.withColumn(
    "convergdb_source_file_name",
    input_file_name()
  )

# note that the convergdb_batch_id is always written as the
# last partition in the list unless explicitly specified by convergdb.
# by default... convergdb_batch_id is *not* recognized as a partition
# by the glue catalog table.
def target_partitions(structure):
  partition_by = structure["partitions"]
  if 'convergdb_batch_id' not in partition_by:
    partition_by.append("convergdb_batch_id")
  return partition_by

def count_only(accum):
  accum.add(1)
  return 1

# performs the actual write out of the data into S3.
# if the data is partitioned, the partitions will be created in this step.
def write_partitions(df, structure):
  convergdb_log("executing job and writing to target storage...")
  df.write.partitionBy(
    target_partitions(structure)
  ).format(
    structure["storage_format"]
  ).save(
    "s3a://" + structure["storage_bucket"],
    mode="append"
  )

# creates a list of conditions to determine rejected records.
# at this time, rejects are only based upon required/null.
def reject_filter(structure):
  filt = []
  for attribute in structure["attributes"]:
    if attribute["required"] == True:
      filt.append(
        attribute["name"] + " is not null"
      )
      if attribute["cast_type"] == "string":
        filt.append(
          attribute["name"] + " != ''"
        )
  reject_filter = ' or '.join(filt)
  convergdb_log(reject_filter)
  if reject_filter != '':
    return reject_filter
  else:
    return 'true = true'

# reject records with any null values
def null_reject(df, structure):
  return df.filter(
    reject_filter(
      structure
    )
  )

# adds any housekeeping fields to the dataframe.
def apply_housekeeping_fields(df, batch_id):
  return df.withColumn(
    "convergdb_batch_id",
    lit(batch_id)
  )

# !SPARK PARTITION HANDLING

# creates a list of tuples representing the list slice ranges
# for each chunk.
def split_indices(count, grouping):
  # creating a set of tuples
  ret = []
  if (grouping == 1) and (count == 1):
    ret.append((0, 1))
    return ret
  elif grouping >= count:
    # full range provided as one group (tuple)
    ret.append((0, count))
    return ret
  else:
    # number of groups to create
    grouping_range = 0
    if count % grouping == 0:
      # grouping is a factor of count
      grouping_range = int(count / grouping)
    else:
      # add a group for the remainder
      grouping_range = int(count / grouping) + 1
    # will execute at least once
    for i in range(grouping_range):
      lo = i * grouping
      hi = min(lo + grouping, count)
      ret.append((lo, hi))
  return ret

# calculates the total size in bytes for the list of files in diff...
# as looked up by the value stored in available dict
def loadable_file_size(available, diff):
  convergdb_log("calculating total size of " + str(len(diff)) + " files...")
  st = time.time()
  size = 0
  for d in diff:
    size += available[d]
  et = time.time()
  convergdb_log("total size calculation took " + str(et - st) + " seconds")
  convergdb_log("source data total " + str(size) + " bytes")
  return size

def coalesce_partition_target(dpu, estimated_source_bytes):
  max_partitions_per_dpu = 4
  target_compression_factor = 3

  target_file_size = 256*(1024**2)
  max_partitions = dpu * max_partitions_per_dpu

  partition_count = round(float(estimated_source_bytes) / float(target_compression_factor) / float(target_file_size))
  partition_count = min([max([partition_count, 1]), max_partitions])
  return partition_count

def calculate_spark_partitions(structure, total_bytes, dpu):
  convergdb_log("dpu from current run_id: " + str(dpu))
  partitions = coalesce_partition_target(
    dpu,
    total_bytes
  )
  convergdb_log("calculated partition count: " + str(partitions))
  return int(partitions)

def available_memory_in_this_cluster(dpu):
  bytes_per_dpu = 5 * (1024**3) # 5GB executor setting in glue
  usable_portion = 0.3          # less than half of the memory
  return int(dpu * bytes_per_dpu * usable_portion)

def calculate_chunk_count(batch_uncompressed_bytes, batch_file_count, dpu):
  return batch_file_count / int(
    max(
      1,
      batch_uncompressed_bytes / available_memory_in_this_cluster(dpu)
    )
  )

# !TIME AND BATCH

# generate sortable timestamp string based on UTC
def batch_id(utc_time):
  this_batch_id = time.strftime("%Y%m%d%H%M%S000", utc_time)
  convergdb_log("batch_id : " + this_batch_id)
  return this_batch_id

def sql_utc_timestamp(utc_time):
  return time.strftime("%Y-%m-%d %H:%M:%S.000", utc_time)

def add_control_file(bucket, key, batch_id):
  convergdb_log("adding control file: " + key)
  client = boto3.client('s3')
  response = client.put_object(
    Body = batch_id,
    Bucket = bucket,
    Key = key
  )

# !AWS GLUE INTERACTIONS

def get_running_job_id(job_name, region):
  client = boto3.client("glue", region_name = region)
  next_token = ""
  while next_token != None:
    resp = None
    if next_token == '':
      resp = client.get_job_runs(
        JobName = job_name
      )
    else:
      resp = client.get_job_runs(
        JobName = job_name,
        NextToken = next_token
      )
    for job in resp["JobRuns"]:
      if job["JobRunState"] == "RUNNING":
        # this is our current job
        return job["Id"]
    next_token = resp.get("NextToken", None)
  return ""

def get_current_job_dpu(job_name, run_id, region):
  try:
    if run_id != '':
      client = boto3.client("glue", region_name = region)
      resp = client.get_job_run(
        JobName = job_name,
        RunId = run_id
      )
      return resp.get("JobRun",{}).get("AllocatedCapacity", 2)
    else:
      return 2
  except:
    return 2

def current_job_dpu(job_name, region):
  run_id = get_running_job_id(
    job_name,
    region
  )
  convergdb_log("run_id :" + run_id)
  if run_id != '':
    return get_current_job_dpu(
      job_name,
      run_id,
      region
    )
  else:
    return 2

# !S3 INTERACTIONS
def gzip_to_s3(bucket, key, body):
  convergdb_log("writing to s3://" + bucket + "/" + key)
  buffer = cStringIO.StringIO()
  writer = gzip.GzipFile(None, 'wb', 6, buffer)
  writer.write(body)
  writer.close()
  buffer.seek(0)
  s3 = boto3.client('s3')
  s3.upload_fileobj(buffer, bucket, key)
  convergdb_log("writing to s3://" + bucket + "/" + key + "complete!")

def append_s3_search_results_to_dict(target, search_results):
  for s3_object in search_results:
    target[s3_object["Key"]] = s3_object["Size"]

def append_s3_search_results_to_list(target, search_results):
  for s3_object in search_results:
    target.append(
      {
        "key": s3_object["Key"],
        "size": int(s3_object["Size"]),
      }
    )

# Search for objects in S3 by bucket and prefix
def s3_search_to_dict(bucket, prefix):
  convergdb_log("searching s3://" + bucket + "/" + prefix + " ...")
  s3 = boto3.client('s3')
  response = s3.list_objects_v2(
    Bucket=bucket,
    Prefix=prefix
  )
  available = {}
  if "Contents" in response:
    contents = response["Contents"]
    append_s3_search_results_to_dict(
      available,
      contents
    )
    while (response.get("NextContinuationToken",False) != False):
      response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        ContinuationToken=response["NextContinuationToken"]
      )
      if "Contents" in response:
        contents = response["Contents"]
        append_s3_search_results_to_dict(
          available,
          contents
        )
  convergdb_log("found " + str(len(available.keys())) + " objects")
  return available

# Search for objects in S3 by bucket and prefix
def s3_search_to_list(bucket, prefix):
  convergdb_log("searching s3://" + bucket + "/" + prefix + " ...")
  s3 = boto3.client('s3')
  response = s3.list_objects_v2(
    Bucket=bucket,
    Prefix=prefix
  )
  available = []
  if "Contents" in response:
    contents = response["Contents"]
    append_s3_search_results_to_list(
      available,
      contents
    )
    while (response.get("NextContinuationToken",False) != False):
      response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        ContinuationToken=response["NextContinuationToken"]
      )
      if "Contents" in response:
        contents = response["Contents"]
        append_s3_search_results_to_list(
          available,
          contents
        )
  convergdb_log("found " + str(len(available)) + " available S3 objects")
  return available

def write_s3_object(bucket, key, content):
  s3 = boto3.client('s3')
  resp = s3.put_object(
    Bucket = bucket,
    Key = key,
    Body = content
  )

# returns an empty string if the object is not found.
def get_s3_object(bucket, key):
  resp = {}
  try:
    s3 = boto3.client('s3')
    resp = s3.get_object(
      Bucket=bucket,
      Key=key
    )
  except:
    # I don't like this but I am not sure why standard
    # python exception handling doesn't work here.
    if sys.exc_info()[0].__name__ == 'NoSuchKey':
      None
    else:
      raise
  if 'Body' in resp:
    return resp['Body'].read()
  else:
    return ''

def dict_to_s3_json(bucket, key, d):
  write_s3_object(
    bucket,
    key,
    json.dumps(d)
  )

def s3_json_to_dict(bucket, key):
  content = get_s3_object(
    bucket,
    key
  )
  if content == '':
    return {}
  else:
    return json.loads(content)

# !CLOUDWATCH and SNS

def put_cloudwatch_metric(region, namespace, metric, value, unit):
  try:
    metric_data = [
      {
        'MetricName': metric,
        'Value': value,
        'Unit': unit
      }
    ]
    convergdb_log("publishing cloudwatch metric: " + str(metric_data) + "to namespace: " + namespace)
    client = boto3.client('cloudwatch', region_name = region)
    response = client.put_metric_data(
      Namespace = namespace,
      MetricData = metric_data
    )
  except:
    convergdb_log("failed to publish cloudwatch metric: " + str(metric_data) + "to namespace: " + namespace)

def publish_sns(region, topic_arn, subject, message):
  try:
    convergdb_log("publishing sns subject: " + str(subject) + " to topic: " + str(topic_arn) + "...")
    client = boto3.client('sns', region_name = region)
    response = client.publish(
      TopicArn = topic_arn,
      Subject = subject,
      Message = message
    )
    convergdb_log("sns message sent successfully!")
  except:
    pass


# !CONTROL TABLE AND DIFF HANDLING

from functools import reduce
def file_sizing(file_dict_list):
  if len(file_dict_list) == 0:
    return 0
  else:
    return reduce(
      (lambda a,b: a + b),
      map(lambda v: v["size"], file_dict_list)
    )

import re
gz_file_re = re.compile('.*\.gz$')
gz_factor = 7
bz2_file_re = re.compile('.*\.bz2$')
bz2_factor = 10

def uncompressed_estimate(file_record):
  if gz_file_re.match(file_record["key"]):
    return file_record["size"] * gz_factor
  elif bz2_file_re.match(file_record["key"]):
    return file_record["size"] * bz2_factor
  else:
    return file_record["size"]

def file_estimated_sizing(file_dict_list):
  if len(file_dict_list) == 0:
    return 0
  else:
    return reduce(
      (lambda a,b: a + b),
      map(lambda v: uncompressed_estimate(v), file_dict_list)
    )

def diff_s3a(structure, file_diff):
  # file list needs to be in a specific string format for processing
  s3a = file_list_to_s3a(
    file_diff,
    structure["source_structure"]["storage_bucket"].split('/')[0]
  )
  return s3a

def file_list(s3a):
  #return ','.join(s3a)
  return s3a

# returns a set of available files
def available_files(structure):
  convergdb_log("searching for available files...")
  s = time.time()
  spl = structure["source_structure"]["storage_bucket"].split("/", 1)
  bucket = spl[0]
  prefix = spl[1] if len(spl) > 1 else ''
  ret = s3_search_to_list(
    bucket,
    prefix
  )
  e = time.time()
  convergdb_log("available file search took " + str(e - s) + " seconds")
  convergdb_log("available file count: " + str(len(ret)))
  return ret

def control_table_database(structure):
  return "convergdb_control_" + structure["deployment_id"]

def control_table_name(structure):
  return structure["full_relation_name"].replace('.','__')

# returns a set representing the files already loaded into the target
# relation.
def loaded_files(structure):
  convergdb_log("searching for loaded files...")
  s = time.time()
  loaded = athena_query_to_list(
    "select source_key  from " + control_table_name(structure),
    control_table_database(structure),
    tmp_results_location(structure),
    structure["region"]
  )
  ret = {}
  for x in loaded:
    ret[x["source_key"]] = None
  e = time.time()
  convergdb_log("loaded file search took " + str(e - s) + " seconds")
  return ret

def s3_file_loaded_record(structure, file_path, batch_id, start_time, end_time):
  return {
    "convergdb_batch_id" : batch_id,
    "batch_start_time" : sql_utc_timestamp(start_time),
    "batch_end_time" : sql_utc_timestamp(start_time),
    "source_type" : "s3_file",
    "source_format" : structure["source_structure"]["storage_format"],
    "source_relation" : structure["source_structure"]["full_relation_name"],
    "source_bucket" : structure["source_structure"]["storage_bucket"],
    "source_key" : file_path,
    "load_type" : "append",
    "status" : "success"
  }

def control_file_key(structure, batch_id):
  return structure["deployment_id"] + "/state/" + structure["full_relation_name"] + "/control/" + str(batch_id) + ".json.gz"

def file_loaded_records(structure, keys_loaded, batch_id, start_time, end_time):
  recs = []
  for k in keys_loaded:
    recs.append(
      json.dumps(
        s3_file_loaded_record(
          structure,
          k,
          batch_id,
          start_time,
          end_time
        )
      )
    )
  return recs

def write_control_records(structure, batch_id, recs):
  convergdb_log("writing " + str(len(recs)) + " control records")
  gzip_to_s3(
    structure["state_bucket"],
    control_file_key(structure, batch_id),
    "\n".join(recs)
  )

# converts a file list into a set of s3a paths.
def file_list_to_s3a(file_list, bucket):
  return ['s3a://' + bucket + '/' + x for x in file_list]

def aws_api_based_diff(structure):
  convergdb_log("using AWS API based search for available files")
  s = time.time()
  # get available files as a list of dict
  available = available_files(structure)
  convergdb_log("available file count: " + str(len(available)))

  # get a dict with loaded files as keys and None as value
  convergdb_log("retrieving loaded files from the control table...")
  loaded = loaded_files(structure)
  convergdb_log("loaded objects: " + str(len(loaded)))

  d = []
  for f in available:
    # uses dict lookup to check if the file has been loaded yet
    if loaded.has_key(f['key']):
      # file has been loaded
      pass
    else:
      # file needs to be loaded
      d.append(f)

  e = time.time()
  convergdb_log("AWS API based diff took " + str(e - s) + " seconds")
  return d

def athena_results_csv_s3_path(execution_id, region):
  client = boto3.client('athena', region_name = region)
  resp = client.get_query_execution(
    QueryExecutionId = execution_id
  )
  output = resp["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
  return {
    'bucket': output[5:len(output)].split('/',1)[0],
    'key': output[5:len(output)].split('/',1)[1]
  }

def control_query_diff_from_csv(csv_path, region):
  csv = get_s3_object(csv_path['bucket'], csv_path['key'])
  ret = []
  for line in csv.decode('utf-8').split("\n"):
    l = line.replace('"','').split(',')
    # last row
    if len(l) == 1:
      break
    # header row
    if l[1] == "size":
      continue
    ret.append(
      {
        "key": l[0],
        "size": int(l[1])
      }
    )
  return ret

def aws_athena_based_diff(structure):
  # use inventory athena query instead of API based search
  convergdb_log("using S3 inventory based search for control diff computation...")
  # get attributes of inventory table
  s = time.time()
  tbl = structure["source_structure"]["inventory_table"].split('.', 1)
  inventory_table = athena_describe_table(
    tbl[0],
    tbl[1],
    structure["region"]
  )

  try:
    # wrapped in a try block in case multiple ETL jobs step on each other
    # trying to refresh the partitions at the same time
    idk = run_athena_query(
      "msck repair table " + structure["source_structure"]["inventory_table"],
      'default',
      tmp_results_location(structure),
      structure["region"]
    )
  except:
    pass

  d = control_query_diff_from_csv(
    athena_results_csv_s3_path(
      run_athena_query(
        query(
          structure,
          " and ".join(
            where_clause(
              structure,
              inventory_table['Table']['StorageDescriptor']['Columns']
            )
          ),
          control_table_database(structure) + '.' + control_table_name(structure)
        ),
        'default',
        tmp_results_location(structure),
        structure["region"]
      ),
      structure["region"]
    ),
    structure["region"]
  )
  e = time.time()
  convergdb_log("S3 inventory based diff took " + str(e - s) + " seconds")
  return d

# calculates the diff between available and loaded files
def file_diff(structure):
  convergdb_log("computing diff between control table and source data...")
  if structure.has_key("use_inventory"):
    if structure["use_inventory"].lower().strip() in ['t','true']:
      # use inventory athena query instead of API based search
      return aws_athena_based_diff(structure)
    else:
      # API based search
      return aws_api_based_diff(structure)
  else:
    # API based search
    return aws_api_based_diff(structure)

# !STATE HANDLING
def get_state(structure):
  r = s3_json_to_dict(
    structure["state_bucket"],
    current_state_key(structure)
  )
  # blank state means first run... so assume "success" for downstream handling
  if r == {}:
    r = {"state": "unknown"}
  convergdb_log("current state: " + r["state"])
  return r

def data_files_for_batch(structure, batch_id):
  convergdb_log("searching for data files leftover from batch: " + batch_id)
  spl = structure["storage_bucket"].split("/", 1)
  bucket = spl[0]
  prefix = spl[1] if len(spl) > 1 else ''
  ret = s3_search_to_dict(
    bucket,
    prefix
  )

  retval = list(
    filter(
      lambda x: x.find("convergdb_batch_id=" + batch_id) > -1,
      ret.keys()
    )
  )
  return retval

def state_folder_prefix(structure):
  return structure["deployment_id"] + "/state/" + structure["full_relation_name"]

def current_state_key(structure):
  return state_folder_prefix(structure) + "/state.json"

def state_success(batch_id, start_time, end_time, structure):
  return {
    "state" : "success",
    "state_time" : sql_utc_timestamp(time.gmtime()),
    "batch_id": batch_id,
    "start_time" : start_time,
    "end_time" : end_time,
    "structure" : structure
  }

def state_load_in_progress(structure, batch_id, start_time, source_objects):
  return {
    "state" : "load_in_progress",
    "state_time" : sql_utc_timestamp(time.gmtime()),
    "batch_id": batch_id,
    "start_time" : start_time,
    "source_objects" : source_objects,
    "structure" : structure
  }

def write_success(structure, batch_id, start_time, end_time):
  dict_to_s3_json(
    structure["state_bucket"],
    current_state_key(structure),
    state_success(
      batch_id,
      sql_utc_timestamp(start_time),
      sql_utc_timestamp(end_time),
      structure
    )
  )

def write_load_in_progress(structure, batch_id, start_time, source_objects):
  dict_to_s3_json(
    structure["state_bucket"],
    current_state_key(structure),
    state_load_in_progress(
      structure,
      batch_id,
      sql_utc_timestamp(start_time),
      source_objects
    )
  )

# !ATHENA QUERY HANDLING
def run_athena_query(query, database, s3_output, region):
  retries = 3
  for retry in range(0,retries):
    try:
      convergdb_log("executing: " + query + " on database " + database + " to " + s3_output)
      st = time.time()
      client = boto3.client('athena', region_name = region)
      response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
          'Database': database
            },
        ResultConfiguration={
          'OutputLocation': s3_output,
          }
        )
      convergdb_log('Execution ID: ' + response['QueryExecutionId'])
      while True:
        check = client.get_query_execution(
          QueryExecutionId = response['QueryExecutionId']
        )
        time.sleep(0.125)
        if check["QueryExecution"]["Status"]["State"] in ['SUCCEEDED','FAILED','CANCELLED']:
          break
        else:
          convergdb_log("still executing: " + response['QueryExecutionId'])
      et = time.time()
      convergdb_log("query execution took " + str(et - st) +" seconds")
      return response['QueryExecutionId']
    except:
      convergdb_log("query failed")
      if retry == retries - 1:
        raise
      convergdb_log("retrying...")
      pass

def run_athena_query_async(query, database, s3_output, region):
  retries = 3
  for retry in range(0,retries):
    try:
      convergdb_log("asynchronously executing: " + query + " on database " + database + " to " + s3_output)
      client = boto3.client('athena', region_name = region)
      response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
          'Database': database
            },
        ResultConfiguration={
          'OutputLocation': s3_output,
          }
        )
      convergdb_log('Execution ID: ' + response['QueryExecutionId'])
      return response['QueryExecutionId']
    except:
      convergdb_log("query failed")
      if retry == retries - 1:
        raise
      convergdb_log("retrying...")
      pass

def column_headers(response):
  headers = []
  for col in response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]:
    d = {}
    d["name"] = col["Name"]
    d["type"] = col["Type"]
    headers.append(d)
  return headers

def control_query_row_to_dict(row, headers):
  d = {}
  for i in range(len(headers)):
    this_field = headers[i]["name"]
    this_value = row["Data"][i]["VarCharValue"]
    if this_field == 'size':
      # header row will have value matching field name
      if this_value != 'size':
        d[this_field] = int(this_value)
      else:
        # handling for header row
        d[this_field] = this_value
    else:
      # otherwise return varchar value unchanged
      d[this_field] = this_value
  return d

def row_to_dict(row, headers):
  d = {}
  for i in range(len(headers)):
    d[headers[i]["name"]] = row["Data"][i]["VarCharValue"]
  return d

def athena_results_to_list(execution_id, region, dict_transform_function=row_to_dict):
  st = time.time()
  ret = []
  client = boto3.client('athena', region_name = region)
  continuation_token = None
  resp = client.get_query_results(
    QueryExecutionId = execution_id
  )
  headers = column_headers(resp)
  for row in resp["ResultSet"]["Rows"]:
    ret.append(dict_transform_function(row, headers))
  if "NextToken" in resp:
    continuation_token = resp["NextToken"]
    resp = client.get_query_results(
      QueryExecutionId = execution_id,
      NextToken = continuation_token
    )
    for row in resp["ResultSet"]["Rows"]:
      ret.append(dict_transform_function(row, headers))
  # the first row is coming back with the header... unacceptable!
  et = time.time()
  convergdb_log("query results gathering took " + str(et - st) +" seconds")
  return ret[1:len(ret)]

def athena_query_to_list(query, database, s3_location, region, dict_transform_function=row_to_dict):
  id = run_athena_query(
    query,
    database,
    s3_location,
    region
  )
  return athena_results_to_list(
    id,
    region,
    dict_transform_function
  )

def tmp_results_location(structure):
  return "s3://" + structure["script_bucket"] + "/" + structure["deployment_id"] + "/tmp/"

def athena_database_name(full_relation_name):
  return ('__').join(full_relation_name.split('.')[0:3])

def athena_table_name(full_relation_name):
  return full_relation_name.split('.')[3]

def athena_describe_table(database, table, region):
  client = boto3.client('glue', region_name = region)
  t = client.get_table(
    DatabaseName = database,
    Name = table
  )
  return t

# expects a list for attributes as returned from get-table API
def has_attribute(attributes, attribute_name):
  for a in attributes:
    if a['Name'] == attribute_name:
      return True
  return False

def where_clause(structure, inventory_attributes):
  predicates = []
  s = structure["source_structure"]["storage_bucket"].split('/',1)
  print(s)
  # bucket name
  predicates.append('"bucket"=' + chr(39) + s[0] + chr(39))
  # prefix
  if len(s) == 2:
    predicates.append('"key" like ' + chr(39) + s[1] + '%' + chr(39))
  else:
    predicates.append('"key" like ' + chr(39) + '%' + chr(39))
  # the following are added if they are added
  if has_attribute(inventory_attributes, 'is_latest'):
    predicates.append("is_latest=true")
  if has_attribute(inventory_attributes, 'is_delete_marker'):
    predicates.append("is_delete_marker=false")
  return predicates

from string import Template
def query(structure, predicates, control_table):
  t = Template("""
with i as
  (select
    key,
    size
  from
    $inventory_table
  where
    dt = (select max(dt) from $inventory_table)
    and
    $predicates),
c as
  (select
     source_key
   from
     $control_table),
diff as
  (select
    i.key,
    i.size,
    c.source_key
  from
    i
    left join
    c
      on
        i.key = c.source_key
  where
    c.source_key is null)
select
  key
  ,size
from
  diff;
""")
  return t.substitute(
    inventory_table = structure["source_structure"]["inventory_table"],
    predicates = predicates,
    control_table = control_table
  )

# !PARTITIONS

def msck_repair_table(structure):
  run_athena_query_async(
    "msck repair table " + athena_table_name(structure["full_relation_name"]),
    athena_database_name(structure["full_relation_name"]),
    tmp_results_location(structure),
    structure["region"]
  )

# !HIGH LEVEL INTERACTIONS

def remove_batch(structure, old_batch_id):
  convergdb_log("removing batch: " + old_batch_id)
  # first identify all data files to be deleted
  delete_data_files = data_files_for_batch(
    structure,
    old_batch_id
  )

  s3 = boto3.client('s3')

  if len(delete_data_files):
    indices = split_indices(
      len(delete_data_files),
      1000
    )

    for indx in indices:
      delete_these = list(
        map(
          lambda fname: {"Key" : fname },
          delete_data_files[indx[0]:(indx[1])]
        )
      )
      bucket = structure["storage_bucket"].split('/')[0]
      convergdb_log("deleting " + str(len(delete_these)) + " objects from failed batch: " + str(old_batch_id))
      s3.delete_objects(
        Bucket=bucket,
        Delete={'Objects': delete_these}
      )

  convergdb_log("removing control logs for batch: " + old_batch_id)
  # delete control file for the given batch
  s3.delete_object(
    Bucket = structure["state_bucket"].split('/')[0],
    Key = control_file_key(
      structure,
      old_batch_id
    )
  )

def load_batch(sc, structure, diff, source_map_func, dpu):
  # get the current state for this table.
  current_state = get_state(structure)

  # first let's perform any clean up from previous runs
  if current_state["state"] == "load_in_progress":
    if current_state.has_key("batch_id"):
      this_start_time = time.gmtime()
      remove_batch(
        structure,
        current_state["batch_id"]
      )
      # write success state
      write_success(
        structure,
        current_state["batch_id"],
        this_start_time,
        time.gmtime()
      )
      current_state = get_state(structure)

  # only proceed if not in a failure state
  if (current_state["state"] in ["success", "unknown"]):
    this_start_time = time.gmtime()

    # generate a timestamp sortable batch_id
    this_batch_id = batch_id(this_start_time)

    # convert the file keys to list of s3a paths
    diff_paths = map(
      lambda x: x["key"],
      diff
    )
    s3a = diff_s3a(structure, diff_paths)

    # only load if there are files
    if len(s3a) > 0:
      write_load_in_progress(
        structure,
        this_batch_id,
        this_start_time,
        diff
      )

      bytes_to_load_compressed = file_sizing(diff)
      convergdb_log("compressed byte size for this batch: " + str(bytes_to_load_compressed))

      bytes_to_load_uncompressed_estimate = file_estimated_sizing(diff)
      convergdb_log("uncompressed byte estimate for this batch: " + str(bytes_to_load_uncompressed_estimate))
      data_load(
        sc,
        structure,
        file_list(s3a),
        source_map_func,
        this_batch_id,
        bytes_to_load_uncompressed_estimate,
        len(diff),
        dpu
      )

      # refresh partitions
      msck_repair_table(structure)

      this_end_time = time.gmtime()

      # these are the control records for the current diff
      flr = file_loaded_records(
        structure,
        diff_paths,
        this_batch_id,
        this_start_time,
        this_end_time
      )

      # write control records
      write_control_records(
        structure,
        this_batch_id,
        flr
      )

      # write success state
      write_success(
        structure,
        this_batch_id,
        this_start_time,
        this_end_time
      )

      # log cloudwatch metrics
      put_cloudwatch_metric(
        structure["region"],
        structure['cloudwatch_namespace'],
        'batch_success',
        1,
        'Count'
      )

      # log cloudwatch metrics
      put_cloudwatch_metric(
        structure["region"],
        structure['cloudwatch_namespace'],
        'source_data_processed_uncompressed_estimate',
        bytes_to_load_uncompressed_estimate,
        'Bytes'
      )

      # log cloudwatch metrics
      put_cloudwatch_metric(
        structure["region"],
        structure['cloudwatch_namespace'],
        'source_data_processed',
        bytes_to_load_compressed,
        'Bytes'
      )

      # log cloudwatch metrics
      put_cloudwatch_metric(
        structure["region"],
        structure['cloudwatch_namespace'],
        'source_files_processed',
        len(diff),
        'Count'
      )

      # send sns success message
      publish_sns(
        structure["region"],
        structure["sns_topic"],
        "SUCCESS - ConvergDB - " + structure["full_relation_name"],
        "files processed: " + str(len(diff)) + "\n" +
        "bytes processed: " + str(bytes_to_load_uncompressed_estimate) + "\n"# +
        "bytes processed (uncompressed estimate): " + str(bytes_to_load_uncompressed_estimate) + "\n"
      )
    else:
      convergdb_log("no new data to load for relation: " + structure["full_relation_name"])

      # log cloudwatch metrics
      put_cloudwatch_metric(
        structure["region"],
        structure['cloudwatch_namespace'],
        'batch_success',
        1,
        'Count'
      )

      publish_sns(
        structure["region"],
        structure["sns_topic"],
        "SUCCESS - ConvergDB - " + structure["full_relation_name"],
        "files processed: " + str(0) + "\n" +
        "bytes processed: " + str(0) + "\n"# +
        "bytes processed (uncompressed estimate): " + str(0) + "\n"
      )

# high level procedure transforms data from the source to the target,
# using idempotent producer/consumer ETL pattern.
def source_to_target(structure_json, source_map_func):
  try:
    # first parse the json representation of the structure into a dict
    structure = json.loads(structure_json)
    # start time is used for batch_id
    start_time = time.gmtime()

    # determine the number of DPUs applied to the current run
    dpu = current_job_dpu(
      structure["etl_job_name"],
      structure["region"]
    )
    convergdb_log("dpu for current_job: " + str(dpu))

    # gets a list of files from the diff process
    # this process may be API based or s3 inventory based
    diff = file_diff(
      structure
    )
    convergdb_log("loadable file count: " + str(len(diff)))

    source_bytes = file_sizing(diff)
    convergdb_log("total loadable bytes (compressed): " + str(source_bytes))

    chunk_size = calculate_chunk_count(
      source_bytes,
      len(diff),
      dpu
    )
    convergdb_log("max files per batch: " + str(chunk_size))

    # tuples of (lo, hi) index ranges for arrays.
    # note that these are correct, unlike python indexes.
    indices = split_indices(
      len(diff),
      chunk_size
    )
    convergdb_log("number of splits for this run: " + str(len(indices)))

    for indx in indices:
      convergdb_log("processing split for index range: " + str(indx[0]) + ":" + str(indx[1]))
      this_diff = diff[indx[0]:(indx[1])]
      load_batch(
        sc,
        structure,
        this_diff,
        source_map_func,
        dpu
      )
  except:
    if 'structure' in vars():
      convergdb_log("error in processing relation: " + structure["full_relation_name"] + str(sys.exc_info()[0]))
      # log cloudwatch metrics
      put_cloudwatch_metric(
        structure["region"],
        structure['cloudwatch_namespace'],
        'batch_failure',
        1,
        'Count'
      )

      # send sns success message
      publish_sns(
        structure["region"],
        structure["sns_topic"],
        "FAILURE - ConvergDB - " + structure["full_relation_name"],
        str(sys.exc_info()[0])
      )
      raise
    else:
      convergdb_log("error in processing relation")
      raise
