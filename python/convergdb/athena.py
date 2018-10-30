from convergdb_logging import *
from batch_control import *

from athena import *
from cloudwatch import *
from glue import *
from s3 import *
from sns import *
from spark_partitions import *
from spark import *
from state import *
from string import Template

def initiate_athena_query(query, database, s3_output, region, retries=3):
  for retry in range(0, retries):
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

def wait_for_athena_query_execution(execution_id, region, retries = 3):
  client = boto3.client('athena', region_name = region)
  st = time.time()
  for retry in range(0,retries):
    try:
      while True:
        check = client.get_query_execution(
          QueryExecutionId = execution_id
        )
        time.sleep(0.125)
        if check["QueryExecution"]["Status"]["State"] in ['SUCCEEDED','FAILED','CANCELLED']:
          if check["QueryExecution"]["Status"]["State"] == 'SUCCEEDED':
            break
          else:
            raise Exception("query for execution_id: " + execution_id + " failed")
        else:
          convergdb_log("still executing: " + execution_id)
      et = time.time()
      convergdb_log("query execution_id " + execution_id + " took " + str(et - st) +" seconds")
      return execution_id
    except:
      convergdb_log("query failed")
      if retry == retries - 1:
        raise
      convergdb_log("retrying...")

def run_athena_query(query, database, s3_output, region, retries=3):
  return wait_for_athena_query_execution(
    initiate_athena_query(
      query,
      database,
      s3_output,
      region,
      retries
    ),
    region,
    retries
  )

def run_athena_query_async(query, database, s3_output, region, retries=3):
  initiate_athena_query(
    query,
    database,
    s3_output,
    region,
    retries
  )

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
  paginator = client.get_paginator('get_query_results')
  page_iterator = paginator.paginate(
    QueryExecutionId = execution_id
  )
  for page in page_iterator:
    convergdb_log("next page")
    headers = column_headers(page)
    for row in page["ResultSet"]["Rows"]:
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

# !PARTITIONS
def msck_repair_table_async_params(structure):
  return [
    "msck repair table " + athena_table_name(structure["full_relation_name"]),
    athena_database_name(structure["full_relation_name"]),
    tmp_results_location(structure),
    structure["region"]
  ]

def msck_repair_table(structure):
  run_athena_query_async(
    "msck repair table " + athena_table_name(structure["full_relation_name"]),
    athena_database_name(structure["full_relation_name"]),
    tmp_results_location(structure),
    structure["region"]
  )
