import json
import sys
import time

from convergdb_logging import *
from batch_control import *
from locking import lock

from athena import *
from cloudwatch import *
from glue import *
from s3 import *
from sns import *
from spark_partitions import *
from spark import *
from state import *

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

def load_batch(sql_context, structure, diff, source_map_func, dpu):
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
    s3a_list = diff_s3a(structure, diff_paths)

    # only load if there are files
    if len(s3a_list) > 0:
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
        sql_context,
        structure,
        s3a_list,
        None,
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
@lock
def source_to_target(sql_context, structure_json):
  try:
    # first parse the json representation of the structure into a dict
    structure = json.loads(structure_json)
    # start time is used for batch_id
    start_time = time.gmtime()
    
    dpu = None
    # determine the number of DPUs applied to the current run
    if structure["etl_technology"] == 'aws_glue':
      dpu = current_job_dpu(
        structure["etl_job_name"],
        structure["region"]
      )
      convergdb_log("dpu for current_job: " + str(dpu))
    elif structure["etl_technology"] == 'aws_fargate':
      dpu = None
  
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
        sql_context,
        structure,
        this_diff,
        None,
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
