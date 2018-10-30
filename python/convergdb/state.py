from convergdb_logging import *
from batch_control import *

from athena import *
from cloudwatch import *
from glue import *
from s3 import *
from sns import *
from spark_partitions import *
from spark import *

import time
import json

def sql_utc_timestamp(utc_time):
  return time.strftime("%Y-%m-%d %H:%M:%S.000", utc_time)
  
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

  for f in ret:
    convergdb_log("need deleting from failed batch " + batch_id +": " + f)

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

def state_success(batch_id, start_time, end_time, structure, state_time=sql_utc_timestamp(time.gmtime())):
  return {
    "state" : "success",
    "state_time" : state_time,
    "batch_id": batch_id,
    "start_time" : start_time,
    "end_time" : end_time,
    "structure" : structure
  }

def state_load_in_progress(structure, batch_id, start_time, source_objects, state_time=sql_utc_timestamp(time.gmtime())):
  return {
    "state" : "load_in_progress",
    "state_time" : state_time,
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
