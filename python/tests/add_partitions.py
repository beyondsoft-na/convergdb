from context import convergdb
from structure import *
import pytest
import re

def test_regexes():
  input = [
    'part1',
    'part2'
  ]
  
  expected = [
    re.compile('part1\\=.*'),
    re.compile('part2\\=.*')
  ]
  
  assert expected == convergdb.regexes(input)

def test_convergdb_database_name():
  assert 'env__db__schema' == convergdb.convergdb_database_name(
    '12345678/env.db.schema.table/part=1/file.json'
  )

def test_convergdb_database_path():
  assert 'env.db.schema' == convergdb.convergdb_database_path(
    '12345678/env.db.schema.table/part=1/file.json'
  )

def test_convergdb_table_name():
  assert 'table' == convergdb.convergdb_table_name(
    '12345678/env.db.schema.table/part=1/file.json'
  )

def test_partition_values():
  partition_kvp = ['part1=1', 'foo=bar']
  partition_fields = ['part1','foo']
  
  assert ['1', 'bar'] == convergdb.partition_values(
    partition_kvp,
    partition_fields
  )

def test_partition_values_from_key():
  key = '12345678/env.db.schema.table/part1=1/part2=2/file.json'
  regexes = convergdb.regexes(
    ['part1', 'part2']
  )
  expected = ['part1=1', 'part2=2']
  assert expected == convergdb.partition_values_from_key(
    key,
    regexes
  )
  
def test_memoize(): # can't be tested
  pass

def test_get_table_metadata(): # NEEDS INTEGRATION TEST
  pass

def test_partition_path():
  key = '12345678/env.db.schema.table/part1=1/part2=2/file.json'
  expected = '12345678/env.db.schema.table/part1=1/part2=2/'
  assert expected == convergdb.partition_path(key, ['part1=1','part2=2'])
  
def test_analyze_s3_key(): # NEEDS INTEGRATION TEST
  pass

def test_write_partition(): # NEEDS INTEGRATION TEST
  pass

def test_s3_list_objects_for_prefix(): # NEEDS INTEGRATION TEST
  pass

def test_keys_from_s3_object_list():
  input = [
    {'Key': 'uno', 'Size': 100},
    {'Key': 'dos', 'Size': 1000}
  ]
  assert ['uno', 'dos'] == convergdb.keys_from_s3_object_list(input)

def test_key_is_valid():
  assert True  == convergdb.key_is_valid('path/is/okay')
  assert False == convergdb.key_is_valid('path/_temporary/not_okay')
  
def test_keys_are_valid():
  key_list = [
    'path/is/okay',
    'path/_temporary/not_okay'
  ]
  
  assert ['path/is/okay'] == convergdb.keys_are_valid(key_list)

def test_update_all_partitions(): # NEEDS INTEGRATION TEST
  pass