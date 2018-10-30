from convergdb_logging import *
from convergdb.batch_control import *
from convergdb.athena import *
from convergdb.cloudwatch import *
from convergdb.glue import *

import sys
import json
import boto3
import time
import cStringIO
import gzip

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

# # Search for objects in S3 by bucket and prefix
# def s3_search_to_dict(bucket, prefix):
#   convergdb_log("searching s3://" + bucket + "/" + prefix + " ...")
#   s3 = boto3.client('s3')
#   response = s3.list_objects_v2(
#     Bucket=bucket,
#     Prefix=prefix
#   )
#   available = {}
#   if "Contents" in response:
#     contents = response["Contents"]
#     append_s3_search_results_to_dict(
#       available,
#       contents
#     )
#     while (response.get("NextContinuationToken",False) != False):
#       response = s3.list_objects_v2(
#         Bucket=bucket,
#         Prefix=prefix,
#         ContinuationToken=response["NextContinuationToken"]
#       )
#       if "Contents" in response:
#         contents = response["Contents"]
#         append_s3_search_results_to_dict(
#           available,
#           contents
#         )
#   convergdb_log("found " + str(len(available.keys())) + " objects")
#   return available

def s3_search_to_dict(bucket, prefix):
  convergdb_log("searching s3://" + bucket + "/" + prefix + " ...")
  # this is the return hash
  available = {}
  # connect to s3.. create a paginator.. get the pages
  s3_client = boto3.client('s3')
  paginator = s3_client.get_paginator('list_objects_v2')
  page_iterator = paginator.paginate(
    Bucket=bucket,
    Prefix=prefix
  )
  # iterate the pages
  for page in page_iterator:
    if "Contents" in page:
      # contents = response["Contents"]
      append_s3_search_results_to_dict(
        available,
        page["Contents"]
      )
  convergdb_log("found " + str(len(available.keys())) + " objects")
  return available

def s3_search_to_list(bucket, prefix):
  convergdb_log("searching s3://" + bucket + "/" + prefix + " ...")
  available = []
  s3_client = boto3.client('s3')
  paginator = s3_client.get_paginator('list_objects_v2')
  page_iterator = paginator.paginate(
    Bucket=bucket,
    Prefix=prefix
  )
  for page in page_iterator:
    if "Contents" in page:
      contents = page["Contents"]
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
