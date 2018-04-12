#!/usr/bin/env python
# -------------------------------------------------------------------------------
# Name: test_one.py
# Purpose: Test script for content in Athena
# Version: 1.0
# Date: November 21, 2017
# Author: Mark Saum
# Email: mark@saum.net
# Copyright: (c) 2017 Beyondsoft
# -------------------------------------------------------------------------------
# Requires:
#  - boto3
# -------------------------------------------------------------------------------

import argparse
import boto3
import configparser
import csv
import logging
import tempfile
from botocore.exceptions import ClientError
from deepdiff import DeepDiff
from urlparse2 import urlparse

# -------------------------------------------------------------------------------
# setup simple logging for INFO
# -------------------------------------------------------------------------------
logging.basicConfig()
STATUS = 25
logging.addLevelName(STATUS, "STATUS")
logger = logging.getLogger('athena_test')
logger.setLevel(25)

# -------------------------------------------------------------------------------
# Read program configuration information
# -------------------------------------------------------------------------------
try:
    config_file = 'athena_test.config'
    Config = configparser.ConfigParser()
    Config.read(config_file)
    try:
        s3_testruns_bucket = Config['global']['s3_testruns_bucket']
        s3_testruns_path = Config['global']['s3_testruns_path']
        s3_testcases_bucket = Config['global']['s3_testcases_bucket']
        s3_testcases_path = Config['global']['s3_testcases_path']
        s3_batch_object = Config['global']['s3_batch_object']
        region_name = Config['global']['region_name']
        profile = Config['global']['profile']
        waiter_delay = Config['global']['waiter_delay']
        waiter_attempts = Config['global']['waiter_attempts']
    except Exception as e:
        logger.debug('Failed to find a configuration value I was looking for in: ' + config_file +
                     ' configuration file because of: ' + e.message, exc_info=True)
except Exception as e:
    logger.debug('Failed to open ' + config_file + ' configuration file' + ' because of: ' + e.message, exc_info=True)

# -------------------------------------------------------------------------------
# Process Arguments
# -------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description='Convert a CSV formatted bucket object to a JSON formatted document')
parser.add_argument("--debug", "-d", help="turn on debugging output", action="store_true")
parser.add_argument("--verbose", "-v", help="turn on program status information output", action="store_true")

args = parser.parse_args()
if args.verbose:
    logger.setLevel(logging.INFO)
if args.debug:
    logger.setLevel(logging.DEBUG)

# s3_testruns_bucket
try:
    s3_testruns_bucket
except:
    logger.error("s3_testruns_bucket must be specified")
    quit(1)
# s3_testruns_path
try:
    s3_testruns_path
except:
    logger.error("s3_testruns_path must be specified")
    quit(1)
# s3_testcases_bucket
try:
    s3_testcases_bucket
except:
    logger.error("s3_testcases_bucket must be specified")
    quit(1)
# s3_testcases_path
try:
    s3_testcases_path
except:
    logger.error("s3_testcases_path must be specified")
    quit(1)
# region_name
try:
    region_name
except:
    logger.error("region_name must be specified")
    quit(1)
# profile
try:
    profile
except:
    logger.info("(AWS) profile not set, setting profile to default")
    profile = "default"
# waiter_delay
try:
    waiter_delay
except:
    logger.info("waiter_delay not set, setting waiter_delay to 5")
    waiter_delay = "5"
# waiter_attempts
try:
    waiter_attempts
except:
    logger.info("waiter_attempts not set, setting waiter_attempts to 20")
    waiter_attempts = "20"

try:
    s3_batch_object
except:
    logger.error("s3_batch_object must be specified")
    quit(1)


# -------------------------------------------------------------------------------
# Define a list of tests to run
# -------------------------------------------------------------------------------
def get_fixtures(s3_client):
    test_case = []

    with tempfile.NamedTemporaryFile(mode='w+b', delete=True) as test_fixtures_object:
        logger.info(
            "Downloading list of tests from: s3://" +
            s3_testruns_bucket + "/" +
            s3_testcases_path + "/" +
            s3_batch_object)
        try:
            s3_client.download_fileobj(s3_testruns_bucket, s3_testcases_path + '/' + s3_batch_object,
                                       test_fixtures_object)
            reader = csv.reader(test_fixtures_object)
            test_fixtures_object.seek(0)
            for row in reader:
                test_case.append({'query': row[0], 'fixture': row[1]})
                logger.debug("Found test case: " + row[0] + "," + row[1])
        except ClientError as e:
            logger.error("Failed to download S3 file object because of error: %s" % e)
            raise e
        except Exception as e:
            logger.error("Failed to download S3 file object because of error: %s" % e)
            raise e

        logger.debug('Downloaded test result using temp file: ' + test_fixtures_object.name)

    return test_case


def open_s3_connection():
    # -----------------------
    # Open S3 Connection
    # -----------------------
    try:
        s3_client = boto3.client('s3')
        logger.debug("Opened connection to S3")
    except:
        logging.error('Failed to open connection to S3.')
        raise
    return s3_client


# -------------------------------------------------------------------------------
# Test Queries
# -------------------------------------------------------------------------------


def run_test_case(s3_client, my_test_case):
    # -----------------------
    # Download test case query
    # -----------------------
    with tempfile.NamedTemporaryFile(mode='w+b', delete=True) as test_case_object:
        logger.info("Downloading test case from: " + 's3://' + s3_testcases_bucket + '/' + s3_testcases_path + '/' +
                    my_test_case['query'])
        try:
            s3_client.download_fileobj(s3_testcases_bucket, s3_testcases_path + '/' + my_test_case['query'],
                                   test_case_object)
        except Exception as e:
            logger.error("Failed to download S3 file object " + my_test_case['query'] + " because of error: %s" % e)
            raise e

        test_case_object.seek(0)
        test_query = test_case_object.read().replace('\n', '')
        logger.debug('Downloaded test case using temp file: ' + test_case_object.name)
    logger.debug('Test query: ' + test_query)

    # -----------------------
    # Download test fixture
    # -----------------------
    with tempfile.NamedTemporaryFile(mode='w+b', delete=True) as test_fixture_object:
        logger.info("Downloading test result from: " + 's3://' + s3_testcases_bucket + '/' + s3_testcases_path + '/' +
                    my_test_case['fixture'])

        try:
            s3_client.download_fileobj(s3_testcases_bucket, s3_testcases_path + '/' + my_test_case['fixture'],
                                   test_fixture_object)
        except Exception as e:
            logger.error("Failed to download S3 file object " + my_test_case['fixture'] + " because of error: %s" % e)
            raise e

        test_fixture_object.seek(0)
        test_fixture = test_fixture_object.read()
        logger.debug('Downloaded test fixture using temp file: ' + test_fixture_object.name)
    logger.debug('Test fixture: ' + test_fixture)

    # -----------------------
    # Open Athena Connection
    # -----------------------
    try:
        logger.debug("Attempting to open connection to Athena")
        athena_client = boto3.client('athena')
        logger.debug("Connection to Athena successfully opened")
    except ClientError as e:
        logger.error("Failed to connect to Athena because of error: %s" % e)
        raise e
    except Exception as e:
        logger.error("Failed to connect to Athena because of error: %s" % e)
        raise e

    # -----------------------
    # Execute Athena query
    # -----------------------
    try:
        logger.debug("Attempting to submit query to Athena")
        response = athena_client.start_query_execution(
            QueryString=test_query,
            ResultConfiguration={'OutputLocation': 's3://' + s3_testruns_bucket + '/' + s3_testruns_path}
        )
        logger.debug("Query submitted to Athena successfully")
    except ClientError as e:
        if e.response['Error']['Code'] == 'InternalServerException':
            logger.error("Query failed submission to Athena due to an InternalServerException")
            raise e
        else:
            logger.error("Query failed submission to Athena due to an unexpected error: %s" % e)
            raise e
    finally:
        # Check status and log progress
        query_id = response['QueryExecutionId']
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error("HTTP error response code: " + str(response['ResponseMetadata']['HTTPStatusCode']))
        logger.info("Query execution id: " + query_id)

    # -----------------------
    # Lookup Athena query information
    # to get exact output_location
    # -----------------------
    try:
        logger.debug("Attempting to query information about query: " + query_id)
        response = athena_client.get_query_execution(
            QueryExecutionId=query_id
        )
        logger.debug("Retrieved information about query: " + query_id)
        # Check status and log progress
        # if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        #     logger.error("HTTP error response code: " + str(response['ResponseMetadata']['HTTPStatusCode']))
        output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        logger.debug("Athena query output location: " + output_location)
        output_url = urlparse(output_location)
        output_bucket = output_url.netloc
        output_object = output_url.path.strip("/")
        logger.debug("Parsed Athena output: Bucket=" + output_bucket + " Object=" + output_object)
    except ClientError as e:
        if e.response['Error']['Code'] == 'InternalServerException':
            logger.error("Failed to retrieve information about query: " + query_id + "due to InternalServerException")
            raise e
        else:
            logger.error("Failed to retrieve information about query: " + query_id + "due to unexpected error: %s" % e)
            raise e

    # -----------------------
    # Wait for Query Execution in S3
    # -----------------------
    logger.info("Begin waiting for Bucket=" + output_bucket + " Object=" + output_object)
    try:
        logger.debug('Creating S3 Waiter client object')
        waiter = s3_client.get_waiter('object_exists')
        logger.debug('Done Creating S3 Waiter client object')
    except ClientError as e:
        logger.error("Failed to create waiter client client because of error: %s" % e)
        raise e
    except Exception as e:
        logger.error("Failed to create waiter client client because of error: %s" % e)
        raise e

    try:
        logger.debug(
            "Creating waiter for S3 Object: Bucket=" + output_bucket + " Object=" + output_object)
        waiter.wait(
            Bucket=output_bucket,
            Key=output_object,
            WaiterConfig={
                'Delay': float(waiter_delay),
                'MaxAttempts': waiter_attempts
            }
        )
    except ClientError as e:
        logger.error("Failed to create waiter because of error: %s" % e)
        raise e
    except Exception as e:
        logger.error("Failed to create waiter because of error: %s" % e)
        raise e

    logger.debug(
        "Finished waiting for S3 Object: Bucket=" + output_bucket + " Object=" + output_object)

    # -----------------------
    # Download test result
    # -----------------------
    with tempfile.NamedTemporaryFile(mode='w+b', delete=True) as test_result_object:
        logger.debug(
            "Downloading test result from:  Bucket=" + output_bucket + " Object=" + output_object)
        try:
            s3_client.download_fileobj(output_bucket, output_object, test_result_object)
            test_result_object.seek(0)
            test_result = test_result_object.read()
        except ClientError as e:
            logger.error("Failed to download S3 file object because of error: %s" % e)
            raise e
        except Exception as e:
            logger.error("Failed to download S3 file object because of error: %s" % e)
            raise e
        logger.debug('Downloaded test result using temp file: ' + test_result_object.name)
    logger.debug('Test results: ' + test_result)

    # Determine Diffs
    if test_fixture == test_result:
        logger.log(STATUS, 'Query \"' + my_test_case['query'] + "\" with fixture \"" + my_test_case[
            'fixture'] + '\" test passes validation')
        return 0
    else:
        logger.error('Query \"' + my_test_case['query'] + "\" with fixture \"" + my_test_case[
            'fixture'] + '\" test fails validation')
        deepdiff = DeepDiff(test_fixture, test_result)
        print ("------------------------  Begin Diff  ------------------------")
        print (deepdiff["values_changed"]["root"]["diff"].encode('utf-8'))
        print ("-------------------------  End Diff  -------------------------")
        return 1


def test_execute():
    result = 0
    s3_client = open_s3_connection()
    my_test_cases = get_fixtures(s3_client)
    for my_test_case in my_test_cases:
        test_result = run_test_case(s3_client, my_test_case)
        if test_result == 1:
            result = 1
    return int(result)

# -------------------------------------------------------------------------------
# Console Entry Point
# -------------------------------------------------------------------------------
if __name__ == "__main__":
    exit(test_execute())
