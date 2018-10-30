from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pytest
import os

@pytest.fixture(scope="session")
def sql_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local[3]").setAppName("pytest-pyspark-local-testing"))
    sc = SparkContext(conf=conf)
    sq = SQLContext(sc)
    request.addfinalizer(lambda: sc.stop())

    return sq

def books_json_path():
  return os.path.dirname(os.path.abspath(__file__)) + '/data/books.json'

def books_csv_path():
  return os.path.dirname(os.path.abspath(__file__)) + '/data/books.csv'
  
def books_as_row(sql_context):
  return sql_context.read.json(
    books_json_path()
  )
  
def books_as_dict(sql_context):
  return map(
    lambda x: x.asDict(),
    books_as_row(sql_context).collect()
  )

from pyspark.sql.types import StructType, StructField, StringType
def books_json_schema(): 
  return StructType(
    [ 
      StructField("item_number", StringType(), True),
      StructField("title", StringType(), True),
      StructField("author", StringType(), True),
      StructField("price", StringType(), True),
      StructField("stock", StringType(), True)
    ]
  )

def books_csv_schema(): 
  return StructType(
    [ 
      StructField("item_number", StringType(), True),
      StructField("title", StringType(), True),
      StructField("author", StringType(), True),
      StructField("publisher", StringType(), True),
      StructField("genre", StringType(), True),
      StructField("price", StringType(), True)
    ]
  )

def sorted_dict(x):
  return {z: x[z] for z in sorted(x)}
  
def rows_to_dict(rows):
  return [r.asDict() for r in rows]

def list_to_sorted_dict(l):
  return [sorted_dict(i) for i in l] 

def df_to_sorted_dict(df):
  return [sorted_dict(r) for r in rows_to_dict(df)]


def available_files_dict():
  return {
    'file1': 1000000,
    'file2': 1000000,
    'file3': 1000000
  }
