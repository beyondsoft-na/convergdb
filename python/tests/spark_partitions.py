from context import convergdb
from structure import *
from pyspark_fixtures import *
import pytest

def test_split_indices():
  expected = [(0, 1)]
  assert expected == convergdb.split_indices(1, 1)
  
  expected = [(0, 1)]
  assert expected == convergdb.split_indices(1, 2)

  expected = [(0, 1)]
  assert expected == convergdb.split_indices(1, 100)
  
  expected = [(0, 1), (1, 2)]
  assert expected == convergdb.split_indices(2, 1)
  
  expected = [(0, 2), (2, 4), (4, 5)]
  assert expected == convergdb.split_indices(5, 2)
  
def test_loadable_file_size():
  expected = 1000000
  assert expected == convergdb.loadable_file_size(
    available_files_dict(),
    ['file1']
  )

  expected = 3000000
  assert expected == convergdb.loadable_file_size(
    available_files_dict(),
    ['file1', 'file2', 'file3']
  )
  
def test_coalesce_partition_target():
  expected = 1
  assert expected == convergdb.coalesce_partition_target(
    1,
    1
  )
  
  expected = 1
  assert expected == convergdb.coalesce_partition_target(
    4,
    256*(1024**2)-1
  )

  expected = 5.0
  assert expected == convergdb.coalesce_partition_target(
    4,
    4*(1024**3)
  )

  expected = 8.0
  assert expected == convergdb.coalesce_partition_target(
    4,
    30*(1024**3)
  )
  
def test_calculate_spark_partitions():
  expected = 8
  assert expected == convergdb.calculate_spark_partitions(
    30*(1024**3),
    1,
    None
  )
  
  expected = 16
  assert expected == convergdb.calculate_spark_partitions(
    30*(1024**3),
    2,
    None
  )
  
  expected = 2
  assert expected == convergdb.calculate_spark_partitions(
    30*(1024**3),
    None,
    None
  ) 

  expected = 400
  assert expected == convergdb.calculate_spark_partitions(
    30*(1024**3),
    2,
    400
  )

def test_available_memory_in_this_cluster():
  expected = 16 * (1024**3) * 0.25
  assert expected == convergdb.available_memory_in_this_cluster(1)

  expected = 4 * (1024**3) * 0.25
  assert expected == convergdb.available_memory_in_this_cluster(None)


def test_calculate_chunk_count():
  expected = 1
  assert expected == convergdb.calculate_chunk_count(
    100,
    1,
    1
  )

  expected = 1
  assert expected == convergdb.calculate_chunk_count(
    100,
    1,
    None
  )
  
  expected = 100
  assert expected == convergdb.calculate_chunk_count(
    100 * (10**3),
    100,
    1
  )

  expected = 1000
  assert expected == convergdb.calculate_chunk_count(
    1000 * (10**3),
    1000,
    1
  )

  expected = 1000
  assert expected == convergdb.calculate_chunk_count(
    1000 * (10**3),
    1000,
    None
  )