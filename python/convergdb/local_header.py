from __future__ import print_function
from convergdb_logging import *

# header will differ in glue
import time
import sys
import os
import boto3

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setMaster("local").setAppName("glue testing")
sc = SparkContext(conf = conf)

if os.environ.get('AWS_SESSION_TOKEN'):
  # The access key, secret key, and session token are picked up automatically
  # from the environment. We just need to override the credential provider.
  sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

# these are helpful when problems arise...
convergdb_log("python version: " + str(sys.version))
convergdb_log("spark version: " + str(sc.version))

sq = SQLContext(sc)

# this is important for handling of decimal types.
# parquet needs the legacy format to work well in spectrum and athena
sq.setConf("spark.sql.parquet.writeLegacyFormat", "true")

def sql_context():
  global sq
  return sq
