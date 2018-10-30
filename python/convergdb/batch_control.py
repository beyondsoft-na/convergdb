# !TIME AND BATCH
import time
import json

from convergdb_logging import *

from athena import *
from cloudwatch import *
from glue import *
from s3 import *
from sns import *
from spark_partitions import *
from spark import *
from functools import reduce
import re

# !UTILITIES

# generate sortable timestamp string based on UTC
def batch_id(utc_time):
  this_batch_id = time.strftime("%Y%m%d%H%M%S000", utc_time)
  convergdb_log("batch_id : " + this_batch_id)
  return this_batch_id

def sql_utc_timestamp(utc_time):
  return time.strftime("%Y-%m-%d %H:%M:%S.000", utc_time)

def add_control_file(bucket, key, body):
  convergdb_log("adding control file: " + key)
  client = boto3.client('s3')
  response = client.put_object(
    Body = body,
    Bucket = bucket,
    Key = key
  )

# !FILE SIZING AND ESTIMATES

# calculate total size for all files in the list
# list must contain a dict for each file
def file_sizing(file_dict_list):
  if len(file_dict_list) == 0:
    return 0
  else:
    return reduce(
      (lambda a,b: a + b),
      map(lambda v: v["size"], file_dict_list)
    )

# these are initialized once as a performance optimization
gz_file_re = re.compile('.*\.gz$')
gz_factor = 7
bz2_file_re = re.compile('.*\.bz2$')
bz2_factor = 10

# calculate an estimate of the total uncompressed size of a given file record
# based upon assumptions related to the filename extensions. note that
# the factors and regex are all passed in as defaults, but you can
# override them if needed.
def uncompressed_estimate(
    file_record,
    gz_file_re=gz_file_re,
    gz_factor=gz_factor,
    bz2_file_re=bz2_file_re,
    bz2_factor=bz2_factor
  ):
  if gz_file_re.match(file_record["key"]):
    return file_record["size"] * gz_factor
  elif bz2_file_re.match(file_record["key"]):
    return file_record["size"] * bz2_factor
  else:
    return file_record["size"]

# returns the total estimated uncompressed size of all files in the list
def file_estimated_sizing(file_dict_list):
  if len(file_dict_list) == 0:
    return 0
  else:
    return reduce(
      (lambda a,b: a + b),
      map(lambda v: uncompressed_estimate(v), file_dict_list)
    )

# !FILE LIST HANDLING

# converts a list of files to a list of s3a paths
def file_list_to_s3a(file_list, bucket):
  return ['s3a://' + bucket + '/' + x for x in file_list]

# wrapper for file_list_to_s3a
def diff_s3a(structure, file_diff):
  # file list needs to be in a specific string format for processing
  s3a = file_list_to_s3a(
    file_diff,
    structure["source_structure"]["storage_bucket"].split('/')[0]
  )
  return s3a

# !CONTROL TABLE NAME FUNCTIONS

# generates the database name for control table
def control_table_database_name(structure):
  return structure["control_table"].split('.')[0]

# generates the actual table name for the control table
def control_table_name(structure):
  return structure["control_table"].split('.')[1]

# !AWS API BASED INVENTORY HANDLING

# performs a diff between the control table contents and AWS S3 API queries.
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

# returns a set of available files using the AWS API
def available_files(structure, s3_function=s3_search_to_list):
  convergdb_log("searching for available files...")
  s = time.time()
  spl = structure["source_structure"]["storage_bucket"].split("/", 1)
  bucket = spl[0]
  prefix = spl[1] if len(spl) > 1 else ''
  ret = s3_function(
    bucket,
    prefix
  )
  e = time.time()
  convergdb_log("available file search took " + str(e - s) + " seconds")
  convergdb_log("available file count: " + str(len(ret)))
  return ret

# returns a dict representing the files already loaded into the target
# relation. the dict keys are the file name, while the values are none.
# dict is an optimization because it allows for faster lookups when
# comparing to the available_files.
def loaded_files(structure, athena_function=athena_query_to_list):
  convergdb_log("searching for loaded files...")
  s = time.time()
  loaded = athena_query_to_list(
    "select source_key  from " + control_table_name(structure),
    control_table_database_name(structure),
    tmp_results_location(structure),
    structure["region"]
  )
  ret = {}
  for x in loaded:
    ret[x["source_key"]] = None
  e = time.time()
  convergdb_log("loaded file search took " + str(e - s) + " seconds")
  return ret

# !CONTROL FILE HANDLING

# creates a dict with all attribues in a control table success record
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

# generates the s3 path to where the control file should be written
def control_file_key(structure, batch_id):
  return structure["deployment_id"] + "/state/" + structure["full_relation_name"] + "/control/" + str(batch_id) + ".json.gz"

# creates a list of all records that need to be written into the control table
# for this batch.
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
        ),
        sort_keys=True # added for unit testing
      )
    )
  return recs

# very stateful... writes control records to a gzipped file in S3
def write_control_records(structure, batch_id, recs):
  convergdb_log("writing " + str(len(recs)) + " control records")
  gzip_to_s3(
    structure["state_bucket"],
    control_file_key(structure, batch_id),
    "\n".join(recs)
  )

# !ATHENA QUERY BASED DIFF

# this function is optimized for the diff queries used by convergdb. it is
# expected that there are 2 columns in the file, key and size.
# first row is skipped because the header is included in the results from
# athena. the last row also requires special handling.
def control_query_diff_from_csv(csv_path, region):
  csv = get_s3_object(csv_path['bucket'], csv_path['key'])
  ret = []
  for line in csv.decode('utf-8').split("\n"):
    # this format should be command delimited after removing quotes
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

# returns the table that should be used for inventory.
# note that this function assumes that 'api' inventory is handled
# elsewhere
def inventory_table(structure):
  handling = structure["inventory_source"]
  if handling == 'default':
    if structure["source_structure"]["streaming_inventory"] == "true":
      return structure["source_structure"]["streaming_inventory_table"]
    else:
      return structure["source_structure"]["inventory_table"]
  elif handling == 'streaming':
    return structure["source_structure"]["streaming_inventory_table"]
  elif handling == 's3':
    return structure["source_structure"]["inventory_table"]

def athena_inventory_type(structure):
  if structure["inventory_source"] == 'default':
    if structure["source_structure"]["streaming_inventory"] == "true":
      return 'streaming'
    else:
      return 's3'
  else:
    return structure["inventory_source"]

# looks up the columns associated with the inventory table
def inventory_table_attributes(structure):
  inv_table = inventory_table(structure)
  convergdb_log("trace inv_table:" + inv_table)
  if (inv_table == None):
    return []
  else:
    tbl = inv_table.split('.', 1)
    inventory_table_attributes = athena_describe_table(
      tbl[0],
      tbl[1],
      structure["region"]
    )
    return inventory_table_attributes['Table']['StorageDescriptor']['Columns']

# indicates the method to use for creation of the athena sql query
def inventory_query_function(structure):
  if athena_inventory_type(structure) == 's3':
    return s3_inventory_query
  else:
    return streaming_inventory_query

# NEW version
def aws_athena_based_diff(structure):
  # use inventory athena query instead of API based search
  convergdb_log("using athena query based control diff computation...")
  # get attributes of inventory table
  s = time.time()

  # s3 based inventory tables require a synchronous
  # partition refresh in order to get the correct information
  if athena_inventory_type(structure) == 's3':
    try:
      # wrapped in a try block in case multiple ETL jobs step on each other
      # trying to refresh the partitions at the same time
      idk = run_athena_query(
        "msck repair table " + inventory_table(structure),
        'default',
        tmp_results_location(structure),
        structure["region"]
      )
    except:
      pass

  # identify the function to be used to generate the SQL
  query_function = inventory_query_function(structure)

  # this tree performs the query, and parses the results
  d = control_query_diff_from_csv(
    athena_results_csv_s3_path(
      run_athena_query(
        # dynamically uses the functions
        query_function(
          structure
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
  convergdb_log("inventory table based diff took " + str(e - s) + " seconds")
  return d

# returns the function to use for determining inventory
def diff_approaches(inventory_source_type, source_structure):
  x = {
    'api' : aws_api_based_diff,
    'streaming' : aws_athena_based_diff,
    's3' : aws_athena_based_diff,
    'default' : aws_athena_based_diff
  }
  if inventory_source_type == 'default':
    if source_structure["inventory_table"] == "":
      if source_structure["streaming_inventory"] == "false":
        return aws_api_based_diff
  return x[inventory_source_type]

# calculates the diff between available and loaded files
def file_diff(structure):
  convergdb_log("computing diff between control table and source data...")
  diff_function = diff_approaches(
    structure["inventory_source"],
    structure["source_structure"]
  )
  return diff_function(structure)

def where_clause(structure, inventory_attributes):
  predicates = []
  s = structure["source_structure"]["storage_bucket"].split('/',1)
  print(s)
  # bucket name
  predicates.append(chr(34) + "bucket" + chr(34) + "=" + chr(39) + s[0] + chr(39))
  # prefix
  if len(s) == 2:
    predicates.append(chr(34) + "key" + chr(34) + " like " + chr(39) + s[1] + '%' + chr(39))
  else:
    predicates.append(chr(34) + "key" + chr(34) + " like " + chr(39) + '%' + chr(39))
  # the following are added if they are added
  if has_attribute(inventory_attributes, 'is_latest'):
    predicates.append("is_latest=true")
  if has_attribute(inventory_attributes, 'is_delete_marker'):
    predicates.append("is_delete_marker=false")
  return predicates

# creates the sql query for use in athena when the s3 generated inventory
# is used. note the inv_table_function that is defaulted in the params.
# override this function with a stub in order to create effective unit tests.
def s3_inventory_query(structure, inv_table_function=inventory_table_attributes):
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
    inventory_table = inventory_table(structure),
    predicates = ' and '.join(
      where_clause(
        structure,
        inv_table_function(structure)
      )
    ),
    control_table = structure["control_table"]
  )

# creates the sql query for use in athena when streaming inventory
# is used. note the inv_table_function that is defaulted in the params.
# override this function with a stub in order to create effective unit tests.
def streaming_inventory_query(structure, inv_table_function=inventory_table_attributes):
  t = Template("""
with max_sequences as
(
  select
    "key",
    max(sequencer) as sequencer
  from
    $inventory_table inv
  where
    $predicates
  group by
    "key"
),
streaming_inventory as
(
  select
    "key"
    ,size
  from
    $inventory_table inv
  where
    $predicates
    and exists (
      select 1
      from
        max_sequences
      where
        max_sequences."key" = inv."key"
        and
        max_sequences.sequencer = inv.sequencer)),
control as
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
    streaming_inventory i
    left join
    control c
      on
        i.key = c.source_key
  where
    c.source_key is null)
select
  key,
  size
from
  diff;
""")
  return t.substitute(
    inventory_table = inventory_table(structure),
    predicates = ' and '.join(
      where_clause(
        structure,
        inv_table_function(structure)
      )
    ),
    control_table = structure["control_table"]
  )