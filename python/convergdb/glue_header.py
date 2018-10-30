# glue header
import sys
import os

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from awsglue.job import Job

from convergdb_logging import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'convergdb_lock_table','aws_region'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
sq = SQLContext(sc)

os.environ['LOCK_TABLE'] = args['convergdb_lock_table']
os.environ['LOCK_ID']    = args['JOB_NAME']

# these are helpful when problems arise...
convergdb_log("python version: " + str(sys.version))
convergdb_log("spark version: " + str(sc.version))

# this is important for handling of decimal types.
# parquet needs the legacy format to work well in spectrum and athena
sq.setConf("spark.sql.parquet.writeLegacyFormat", "true")

def sql_context():
  global sq
  return sq