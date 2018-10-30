from context import convergdb
from structure import *
import pytest
import json
import time


def test_batch_id():
  t = convergdb.batch_id(
    (2018, 12, 31, 23, 59, 59, 0, 0, 0)
  )
  assert t == '20181231235959000'

def test_sql_utc_timestamp():
  t = convergdb.sql_utc_timestamp(
    (2018, 12, 31, 23, 59, 59, 0, 0, 0)
  )
  assert t == '2018-12-31 23:59:59.000'

def test_add_control_file():
  # writes to s3... no test
  pass

def test_file_sizing():
  # empty list means size 0
  t = convergdb.file_sizing(
    []
  )
  assert t == 0

  t = convergdb.file_sizing(
    [
      {
        "key": "a.gz",
        "size": 100
      },
      {
        "key": "b.gz",
        "size": 100
      },
      {
        "key": "c.gz",
        "size": 100
      }
    ]
  )
  assert t == 300

def test_uncompressed_estimate():
  t = convergdb.uncompressed_estimate(
      {
        "key": "a.gz",
        "size": 100
      }
  )
  assert t == 700

  t = convergdb.uncompressed_estimate(
      {
        "key": "a.bz2",
        "size": 100
      }
  )
  assert t == 1000

  t = convergdb.uncompressed_estimate(
      {
        "key": "a.json",
        "size": 100
      }
  )
  assert t == 100

def test_file_estimated_sizing():
  t = convergdb.file_estimated_sizing(
    [
      {
        "key": "a.json",
        "size": 100
      },
      {
        "key": "b.bz2",
        "size": 100
      },
      {
        "key": "c.gz",
        "size": 100
      }
    ]
  )
  assert t == 1800

def test_file_list_to_s3a():
  key_list = ['key1','key2','key3']
  bucket = 'some-bucket'
  t = convergdb.file_list_to_s3a(key_list, bucket)

  assert t == [
    's3a://some-bucket/key1',
    's3a://some-bucket/key2',
    's3a://some-bucket/key3'
  ]

def test_diff_s3a():
  key_list = ['key1','key2','key3']
  t = convergdb.diff_s3a(
    structure_1(),
    key_list
  )
  expected = [
    "s3a://demo-source-us-west-2.beyondsoft.us/key1",
    "s3a://demo-source-us-west-2.beyondsoft.us/key2",
    "s3a://demo-source-us-west-2.beyondsoft.us/key3"
  ]
  assert expected == t

def test_control_table_database_name():
  t = convergdb.control_table_database_name(structure_1())
  assert t == 'convergdb_control_e969ca618e222a58'

def test_control_table_name():
  t = convergdb.control_table_name(structure_1())
  assert t == 'production__ecommerce__inventory__books'

def test_aws_api_based_diff():
  # too much state
  # needs functional refactor
  pass

def test_available_files():
  # performs s3 search
  # needs functional refactoring
  pass

def test_loaded_files():
  # performs athena query
  # needs functional refactoring
  pass

def test_s3_file_loaded_record():
  t = convergdb.s3_file_loaded_record(
    structure_1(),
    'path/to/file',
    '20181231235959000',
    (2018, 12, 31, 23, 59, 59, 0, 0, 0),
    (2018, 12, 31, 23, 59, 59, 0, 0, 0)
  )

  assert t == {
    "convergdb_batch_id" : '20181231235959000',
    "batch_start_time" : '2018-12-31 23:59:59.000',
    "batch_end_time" : '2018-12-31 23:59:59.000',
    "source_type" : "s3_file",
    "source_format" : 'json',
    "source_relation" : 'production.ecommerce.inventory.books_source',
    "source_bucket" : 'demo-source-us-west-2.beyondsoft.us',
    "source_key" : 'path/to/file',
    "load_type" : "append",
    "status" : "success"
  }

def test_control_file_key():
  t = convergdb.control_file_key(
    structure_1(),
    '20181231235959000'
  )
  assert t == "e969ca618e222a58/state/production.ecommerce.inventory.books/control/20181231235959000.json.gz"

def test_file_loaded_records():
  t = convergdb.file_loaded_records(
    structure_1(),
    ['key1','key2'],
    '20181231235959000',
    (2018, 12, 31, 23, 59, 59, 0, 0, 0),
    (2018, 12, 31, 23, 59, 59, 0, 0, 0)
  )

  expected = [
	  '{"batch_end_time": "2018-12-31 23:59:59.000", "batch_start_time": "2018-12-31 23:59:59.000", "convergdb_batch_id": "20181231235959000", "load_type": "append", "source_bucket": "demo-source-us-west-2.beyondsoft.us", "source_format": "json", "source_key": "key1", "source_relation": "production.ecommerce.inventory.books_source", "source_type": "s3_file", "status": "success"}',
    '{"batch_end_time": "2018-12-31 23:59:59.000", "batch_start_time": "2018-12-31 23:59:59.000", "convergdb_batch_id": "20181231235959000", "load_type": "append", "source_bucket": "demo-source-us-west-2.beyondsoft.us", "source_format": "json", "source_key": "key2", "source_relation": "production.ecommerce.inventory.books_source", "source_type": "s3_file", "status": "success"}'
  ]

  assert expected == t

def test_write_control_records():
  # writes to s3...
  # needs functional refactoring
  pass

def test_control_query_diff_from_csv():
  pass

def test_inventory_table():
  # default, streaming_inventory = true
  test_structure1 = {
    "inventory_source": "default",
    "source_structure": {
      "inventory_table": "",
      "streaming_inventory": "true",
      "streaming_inventory_table": "inventory.table"
    }
  }
  assert "inventory.table" == convergdb.inventory_table(test_structure1)

  # s3, streaming_inventory = true (ignored)
  test_structure2 = {
    "inventory_source": "s3",
    "source_structure": {
      "inventory_table": "s3_inventory.table_name",
      "streaming_inventory": "true",
      "streaming_inventory_table": "inventory.table"
    }
  }
  assert "s3_inventory.table_name" == convergdb.inventory_table(test_structure2)

  # streaming
  test_structure3 = {
    "inventory_source": "streaming",
    "source_structure": {
      "inventory_table": "s3_inventory.table_name",
      "streaming_inventory": "true",
      "streaming_inventory_table": "inventory.table"
    }
  }
  assert "inventory.table" == convergdb.inventory_table(test_structure3)

  # default, streaming inventory = false
  test_structure4 = {
    "inventory_source": "default",
    "source_structure": {
      "inventory_table": "s3_inventory.table_name",
      "streaming_inventory": "false",
      "streaming_inventory_table": "inventory.table"
    }
  }
  assert "s3_inventory.table_name" == convergdb.inventory_table(test_structure4)

def test_athena_inventory_type():
  pass

def test_inventory_table_attributes():
  pass

def test_inventory_query_function():
  pass

def test_aws_athena_based_diff():
  pass

def test_diff_approaches():
  t = {
    "inventory_table": "",
    "streaming_inventory": "false"
  }
  
  assert convergdb.aws_api_based_diff == convergdb.diff_approaches(
    'api',
    t
  )
  assert convergdb.aws_athena_based_diff == convergdb.diff_approaches(
    's3',
    t
  )
  assert convergdb.aws_athena_based_diff == convergdb.diff_approaches(
    'streaming',
    t
  )

  # inventory table default
  t = {
    "inventory_table": "database.table",
    "streaming_inventory": "false"
  }
  assert convergdb.aws_athena_based_diff == convergdb.diff_approaches(
    'default',
    t
  )

  # streaming inventory default
  t = {
    "inventory_table": "",
    "streaming_inventory": "true"
  }
  assert convergdb.aws_athena_based_diff == convergdb.diff_approaches(
    'default',
    t
  )
  
def test_file_diff():
  pass

def test_where_clause():
  inv_attributes=[
    {"Name":"bucket","Type":"string"},
    {"Name":"key","Type":"string"},
    {"Name":"size","Type":"bigint"},
    {"Name":"last_modified_date","Type":"timestamp"},
    {"Name":"e_tag","Type":"string"},
    {"Name":"storage_class","Type":"string"},
    {"Name":"is_multipart_uploaded","Type":"boolean"},
    {"Name":"replication_status","Type":"string"},
    {"Name":"encryption_status","Type":"string"}
  ]

  t = convergdb.where_clause(structure_1(), inv_attributes)

  assert t == [
    '"bucket"=' + chr(39) + 'demo-source-us-west-2.beyondsoft.us' + chr(39),
    '"key" like ' + chr(39) + '%' + chr(39)
  ]

  # this test is for inventory tables with version tracking enabled
  inv_attributes=[
    {"Name":"bucket","Type":"string"},
    {"Name":"key","Type":"string"},
    {"Name":"size","Type":"bigint"},
    {"Name":"last_modified_date","Type":"timestamp"},
    {"Name":"e_tag","Type":"string"},
    {"Name":"storage_class","Type":"string"},
    {"Name":"is_multipart_uploaded","Type":"boolean"},
    {"Name":"replication_status","Type":"string"},
    {"Name":"encryption_status","Type":"string"},
    {"Name":"is_latest","Type":"boolean"},
    {"Name":"is_delete_marker","Type":"boolean"}
  ]

  t = convergdb.where_clause(structure_1(), inv_attributes)

  assert t == [
    '"bucket"=' + chr(39) + 'demo-source-us-west-2.beyondsoft.us' + chr(39),
    '"key" like ' + chr(39) + '%' + chr(39),
    'is_latest=true',
    'is_delete_marker=false'
  ]

# is used. note the inv_table_function that is defaulted in the params.
def test_s3_inventory_query():
  expected ="""
with i as
  (select
    key,
    size
  from
    s3_inventory.production__ecommerce__inventory__books_source
  where
    dt = (select max(dt) from s3_inventory.production__ecommerce__inventory__books_source)
    and
    "bucket"='demo-source-us-west-2.beyondsoft.us' and "key" like '%'),
c as
  (select
     source_key
   from
     convergdb_control_e969ca618e222a58.production__ecommerce__inventory__books),
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
"""
  # stub function
  def inv_attr_function_stub(a):
    return []

  t = convergdb.s3_inventory_query(
    structure_1(),
    inv_attr_function_stub
  )
  assert expected == t

def test_streaming_inventory_query():
  expected ="""
with max_sequences as
(
  select
    "key",
    max(sequencer) as sequencer
  from
    streaming_inventory_table inv
  where
    "bucket"='demo-source-us-west-2.beyondsoft.us' and "key" like '%'
  group by
    "key"
),
streaming_inventory as
(
  select
    "key"
    ,size
  from
    streaming_inventory_table inv
  where
    "bucket"='demo-source-us-west-2.beyondsoft.us' and "key" like '%'
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
     convergdb_control_e969ca618e222a58.production__ecommerce__inventory__books),
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
"""
  # stub function
  def inv_attr_function_stub(a):
    return []

  t = convergdb.streaming_inventory_query(
    structure_2(),
    inv_attr_function_stub
  )
  assert expected == t
