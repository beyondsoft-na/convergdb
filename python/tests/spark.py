from __future__ import print_function

from context import convergdb
from structure import *
from pyspark_fixtures import *
import pytest
import os

from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql.functions import input_file_name

def test_data_load():
  # needs integration test
  # does too many things!
  # but... all of the components work
  pass

def test_csv_source_schema():
  from pyspark.sql.types import StructType, StructField, StringType
  expected = books_csv_schema()
  t = convergdb.csv_source_schema(
    structure_2()
  )
  assert expected == t

def test_coalesce_expression():
  assert 'a' == convergdb.coalesce_expression(
    {
      'expression': None,
      'name': 'a'
    }
  )

  assert 'a.b' == convergdb.coalesce_expression(
    {
      'expression': 'a.b',
      'name': 'a'
    }
  )

def test_nestable_source_schema():
  from pyspark.sql.types import StructType, StructField, StringType
  expected = books_json_schema()
  t = convergdb.nestable_source_schema(
    structure_1()
  )
  for i in t:
    assert i in expected

@pytest.mark.usefixtures("sql_context")
def test_casted_attribute(sql_context):
  df = books_as_row(sql_context)
  test = convergdb.casted_attribute(
    df,
    {'name': 'title','cast_type': 'varchar(100)'}
  )
  expected = df['title'].cast('varchar(100)').alias('title')
  assert df.select(test).collect() == df.select(expected).collect()

@pytest.mark.usefixtures("sql_context")
def test_json_file_to_df(sql_context):
  expected = sql_context.read.json(
    books_json_path(), 
    schema=books_json_schema()
  ).collect()
  test = convergdb.json_file_to_df(
    sql_context,
    [books_json_path()],
    books_json_schema()
  ).collect()
  assert expected == test

@pytest.mark.usefixtures("sql_context")
def test_csv_file_to_df(sql_context):
  expected = sql_context.read.json(
    books_json_path(), 
    schema=books_csv_schema()
  ).collect()
  
  test = convergdb.csv_file_to_df(
    sql_context,
    [books_csv_path()],
    books_csv_schema(),
    structure_2()
  ).collect()
  
  assert expected == test
  
@pytest.mark.usefixtures("sql_context")
def test_apply_casting(sql_context):
  # create the testable structure
  test = convergdb.apply_casting(
    books_as_row(sql_context),
    structure_1()['source_structure']
  ).collect()

  # create the expected
  # basically.. manually casting everything
  e1 = books_as_row(sql_context)
  e2 = e1.selectExpr(
    "cast(item_number as integer) item_number",
    "cast(title as varchar(100)) title",
    "cast(author as varchar(100)) author",
    "cast(price as decimal(10,2)) price",
    "cast(stock as integer) stock"
  )
  expected = e2.collect()
  
  assert test == expected
  
def test_expression_text():
  test_cases = [
    {
      "expression": "nested.json.path",
      "name": "path",
      "expected": "nested.json.path as path"
    },
    {
      "name": "path",
      "expected": "path as path",
      "expression": None
    }
  ]
  
  for test_case in test_cases:
    assert test_case["expected"] == convergdb.expression_text(test_case)

@pytest.mark.usefixtures("sql_context")
def test_apply_expressions(sql_context):
  expected = books_as_row(sql_context).selectExpr(
    "item_number as item_number",
    "title as title",
    "author as author",
    "price as price",
    "stock as stock"
  ).collect()
  
  test = convergdb.apply_expressions(
    books_as_row(sql_context),
    structure_1()["source_structure"]
  ).collect()
  
  assert test == expected

@pytest.mark.usefixtures("sql_context")
def test_source_file_name(sql_context):
  expected = books_as_row(sql_context).withColumn(
    'convergdb_source_file_name',
    input_file_name()
  ).collect()
  
  test = convergdb.source_file_name(
    books_as_row(sql_context)
  ).collect()
  
  assert test == expected
 
def test_target_partitions():
  expected = ["part_id", "convergdb_batch_id"]
  test = convergdb.target_partitions(structure_1())
  assert test == expected
  
  struc2 = structure_1()
  struc2["partitions"] == ["part_id", "convergdb_batch_id"]
  test = convergdb.target_partitions(struc2)
  assert test == expected  
  
def test_write_partitions():
  # depends on S3 - needs refactor
  pass
      
def test_reject_filter():
  expected = "stock is not null"
  test = convergdb.reject_filter(
    structure_1()["source_structure"]
  )
  assert test == expected

@pytest.mark.usefixtures("sql_context")
def test_null_reject(sql_context):
  expected = 2
  test = convergdb.null_reject(
    books_as_row(sql_context),
    structure_1()["source_structure"]
  ).count()
  assert test == expected

@pytest.mark.usefixtures("sql_context")
def test_apply_housekeeping_fields(sql_context):
  expected = books_as_row(sql_context).withColumn(
    "convergdb_batch_id",
    lit("1000")
  ).collect()
  
  test = convergdb.apply_housekeeping_fields(
    books_as_row(sql_context),
    "1000"
  ).collect()
  
  assert test == expected
    
def test_csv_param():
  tests = [
    (34, '"'),
    ('u0034', '4')
  ]
  
  for t in tests:
    assert t[1] == convergdb.csv_param(t[0])

def test_expressions_to_schema_dict():
  expressions = ['data.a', 'data.b.c']
  expected = {
    'data': {
      'a': {},
      'b': {
        'c': {}
      }
    }
  }
  
  assert expected == convergdb.expressions_to_schema_dict(expressions)
  
def test_append_to_schema_dict():
  d = {}
  expected = {'a': {}}
  convergdb.append_to_schema_dict(d, 'a')
  assert expected == d
  
  # this test is progressive
  expected = {'a': {'b': {}}}
  convergdb.append_to_schema_dict(d, 'a.b')
  assert expected == d
  
   # this test is progressive
  expected = {'a': {'b': {'c': {}}}}
  convergdb.append_to_schema_dict(d, 'a.b.c')
  assert expected == d 
  
   # this test is progressive
  expected = {'a': {'b': {'c': {}}, 'd': {}}}
  convergdb.append_to_schema_dict(d, 'a.d')
  assert expected == d 
  
def test_dict_to_spark_schema():
  from pyspark.sql.types import StructType, StructField, StringType
  d = {'a': {}}
  expected = StructType(
    [
      StructField('a', StringType(), True)
    ]
  )
  assert expected == convergdb.dict_to_spark_schema(d)
  
  d = {'a': {'b': {}}}
  expected = StructType(
    [
      StructField('a', StructType(
        [
          StructField('b', StringType(), True)
        ]
      ), True)
    ]
  )
  assert expected == convergdb.dict_to_spark_schema(d)
  
  d = {'a': {'b': {'c': {}}}}
  expected = StructType(
    [
      StructField('a', StructType(
        [
          StructField('b', StructType(
            [
              StructField('c', StringType(), True)
            ]
          ), True)
        ]
      ), True)
    ]
  )
  assert expected == convergdb.dict_to_spark_schema(d)