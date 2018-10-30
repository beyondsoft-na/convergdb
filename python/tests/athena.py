from context import convergdb
from structure import *
import pytest

def test_initiate_athena_query():
  pass

def test_wait_for_athena_query_execution():
  pass

def test_msck_repair_table_async_params():
  pass

def test_run_athena_query():
  # too much... needs refactoring
  pass

def test_run_athena_query_async():
  # too much... needs refactoring
  pass

def test_athena_results_csv_s3_path():
  # requires the ability to query athena API
  pass
  
def test_column_headers():
  input = {
    "ResultSet": {
      "ResultSetMetadata": {
        "ColumnInfo":[
          {"Name": "col1", "Type": "integer"},
          {"Name": "col2", "Type": "timestamp"},
          {"Name": "col3", "Type": "varchar(20)"}
        ]
      }
    }
  }
  
  expected = [
    {"name": "col1", "type": "integer"},
    {"name": "col2", "type": "timestamp"},
    {"name": "col3", "type": "varchar(20)"}
  ]
  
  t = convergdb.column_headers(input)
  assert t == expected

def test_control_query_row_to_dict():
  test_row = {
    "Data": [
      {
        "VarCharValue": "test_key"
      },
      {
        "VarCharValue": "100"
      }
    ]
  }
  
  headers = [
    {"name": "key", "type": "varchar"},
    {"name": "size", "type": "integer"},
  ]
  
  expected = {"key": "test_key", "size": 100}
  
  t = convergdb.control_query_row_to_dict(test_row, headers)
  assert t == expected

def test_row_to_dict():
  test_row = {
    "Data": [
      {
        "VarCharValue": "test_key"
      },
      {
        "VarCharValue": "100"
      }
    ]
  }
  
  headers = [
    {"name": "key", "type": "varchar"},
    {"name": "size", "type": "integer"},
  ]
  
  expected = {"key": "test_key", "size": "100"}
  
  t = convergdb.row_to_dict(test_row, headers)
  assert t == expected

def test_athena_results_to_list():
  # needs refactoring... not functional
  pass

def test_athena_query_to_list():
  # this uses two high level functions.. but they are very stateful
  pass

def test_tmp_results_location():
  t = convergdb.tmp_results_location(structure_1())
  
  assert t == 's3://convergdb-admin-e969ca618e222a58/e969ca618e222a58/tmp/'

def test_athena_database_name():
  t = convergdb.athena_database_name('prod.dom.sch.rel')
  assert t == 'prod__dom__sch'

def test_athena_table_name():
  t = convergdb.athena_table_name('prod.dom.sch.rel')
  assert t == 'rel'

def test_athena_describe_table():
  # this function wraps an AWS API call
  pass

def test_has_attribute():
  attributes = [
    {"Name": "turkey", "Type":"varchar(20)","Comment":""},
    {"Name": "hello", "Type":"varchar(20)","Comment":""}
  ]
  
  t = convergdb.has_attribute(attributes, 'turkey')
  assert t == True
  
  t = convergdb.has_attribute(attributes, 'chicken')
  assert t == False

def test_msck_repair_table():
  # this is an asynchronous athena interaction
  # should be revised in a functional way so that we can test the parameters
  # passed in
  pass
