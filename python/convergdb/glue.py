from convergdb_logging import *

# !AWS GLUE INTERACTIONS
import json
import boto3
import time
import cStringIO
import gzip

# paginator needed
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
