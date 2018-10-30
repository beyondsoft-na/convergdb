# !PYSPARK FUNCTIONS
from convergdb_logging import *

from convergdb.spark_partitions import *

import time

from pyspark.sql.functions import lit, when, udf
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import StructType, StructField, StringType

# returns an all string schema to enable dataframe creation when fields are missing
def csv_source_schema(structure):
  return StructType(
    [ StructField(a["name"], StringType(), True) for a in structure["source_structure"]["attributes"]]
  )

def coalesce_expression(attribute):
  if attribute['expression'] == None:
    return attribute['name']
  else:
    return attribute['expression']

# creates a schema that supports nested structures for use with json etc.
def nestable_source_schema(structure):
  return dict_to_spark_schema(
    expressions_to_schema_dict(
      list(
        map(
          lambda x: coalesce_expression(x),
          structure['source_structure']['attributes']
        )
      )
    )
  )

# creates all of the functional rdd/dataframe layers then
# triggers the actual data transformation.
def data_load(sql_context, structure, s3a_paths, source_map_func, batch_id, total_bytes, file_count, dpu):
  convergdb_log("starting data load for " + structure["full_relation_name"])
  st = time.time()

  # determine the number of spark partitions to use for this batch.
  spark_partitions = calculate_spark_partitions(
    total_bytes,
    dpu
  )

  # create a data frame from the source data.
  # dataframe will be all strings at this point.
  d1 = None
  
  if structure['source_structure']['storage_format'] == 'json':
    d1 = json_file_to_df(
      sql_context,
      s3a_paths,
      nestable_source_schema(structure)
    )
  elif structure['source_structure']['storage_format'] == 'csv':
    d1 = csv_file_to_df(
      sql_context,
      s3a_paths,
      csv_source_schema(structure),
      structure
    )

  d3 = None

  if structure['source_structure']['storage_format'] == 'json':
    d2 = apply_expressions(
      d1,
      structure["source_structure"]
    )
  elif structure['source_structure']['storage_format'] == 'csv':
    # expressions are not applied to csv data because it is not nested
    d2 = d1
    
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

  d6 = source_file_name(d5)

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
def json_file_to_df(sq, file_paths, schema):
  convergdb_log("defining dataframe from JSON source...")
  return sq.read.json(
    list(file_paths),
    schema=schema
  )

def csv_file_to_df(sq, file_paths, schema, structure):
  convergdb_log("defining data frame from CSV source...")
  return sq.read.csv(
    file_paths,
    schema = schema,
    sep = csv_param(structure['source_structure']['csv_separator']),
    quote = csv_param(structure['source_structure']['csv_quote']),
    escape = csv_param(structure['source_structure']['csv_escape']),
    header = structure['source_structure']['csv_header'],
    ignoreLeadingWhiteSpace = structure['source_structure']['csv_trim'],
    ignoreTrailingWhiteSpace = structure['source_structure']['csv_trim']
  )

# applies casting to the dataframe, based upon the attributes in params.
def apply_casting(df, structure):
  # note that params is a convergdb structure, but it is generalized
  # because it can be used with source and target relations.
  convergdb_log("defining data type casting...")
  return df.select(
    *[casted_attribute(df, c) for c in structure["attributes"]]
  )

# text format for attribute expression, to be used in selectExpr.
def expression_text(attribute):
  exp = attribute["expression"]
  if exp == None:
    exp = attribute["name"]
  return exp + " as " + attribute["name"]

# applies expressions to provided dataframe, based upon attributes in params.
def apply_expressions(df, structure):
  convergdb_log("defining target attribute expressions...")
  return df.selectExpr(
    *[expression_text(c) for c in structure["attributes"]]
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

def csv_param(csv_param):
  if type(csv_param) == int:
    return chr(csv_param)
  else:
    return unichr(
      int(
        csv_param[1:], 16
      )
    )

# converts a list of dot notated expressions into a dict that can be
# converted to a spark schema.
def expressions_to_schema_dict(expressions):
  d = {}
  for e in expressions:
    append_to_schema_dict(d, e)
  return d

# used recursively to create schema dict
def append_to_schema_dict(d, k):
  print k
  keys = k.split('.', 1)
  if keys[0] in d:
    # key already exists
    None
  else:
    # key doesn't exist. add as a leaf
    d[keys[0]] = {}
  if len(keys) > 1:
    append_to_schema_dict(d[keys[0]], keys[1])

# converts a dict into a spark schema. leaf nodes are indicated by empty dict.
def dict_to_spark_schema(d):
  return StructType([spark_schema(i, d[i]) for i in d])
  
# used recursively to create schema
def spark_schema(k, v):
  if v == {}:
    return StructField(k, StringType(), True)
  else:
    return StructField(k, StructType([spark_schema(i, v[i]) for i in v]), True)