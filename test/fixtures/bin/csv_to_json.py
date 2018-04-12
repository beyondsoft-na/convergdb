#!/usr/bin/env python
# -------------------------------------------------------------------------------
# Name: csv_to_json.py
# Purpose: Converts a CSV formatted S3 object a JSON file, as a file of
#          one-line JSON documents.
# Version: 1.0
# Date: November 17, 2017
# Author: Mark Saum
# Email: mark@saum.net
# Copyright: (c) 2017 Beyondsoft
# -------------------------------------------------------------------------------
# Source data used to test:
# https://aws.amazon.com/public-datasets/irs-990/
# s3://irs-form-990/index_2017.csv
# -------------------------------------------------------------------------------
# Requires:
#   - smart_open
#       smart_open is a Python 2 & Python 3 library for efficient streaming of
#       very large files from/to S3, HDFS, WebHDFS, HTTP, or local (compressed)
#       files. It is well tested (using moto), well documented and sports a simple, Pythonic API
#       https://pypi.python.org/pypi/smart_open
# -------------------------------------------------------------------------------
#   - Added support for "." in S3 bucket names
#     See:
#       - https://github.com/boto/boto/issues/2836
#       - https://github.com/RaRe-Technologies/smart_open/issues/26
#       - https://github.com/RaRe-Technologies/smart_open/pull/53
# -------------------------------------------------------------------------------
# Uses S3 Multi-part upload capability
# http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
# Once you initiate a multipart upload, Amazon S3 retains all the parts until you either complete or abort the upload.
# So it is important that you manage the process to avoid orphans.
# -------------------------------------------------------------------------------
# Usage Example:
# In memory:
# ./csv_to_json.py -i s3://irs-form-990/index_2017.csv -o s3://fakedata-source.beyondsoft.us/index_2017.json --region us-west-2 --profile beyondsoft --verbose
# On disk (for large files):
# ./csv_to_json.py -i s3://irs-form-990/index_2017.csv -o s3://fakedata-source.beyondsoft.us/index_2017.json --disk --region us-west-2 --profile beyondsoft --verbose
# -------------------------------------------------------------------------------


import argparse
import cStringIO
import csv
import json
import logging
import string
import sys
import tempfile
import boto.s3.connection
import smart_open
from urlparse import urlparse
from boto.s3.key import Key

# -------------------------------------------------------------------------------
# setup simple logging for INFO
# -------------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------------------------------------------------------------------------------
# Process Arguments
# -------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description='Convert a CSV formatted bucket object to a JSON formatted document')
parser.add_argument("--debug", "-d", help="turn on debugging output", action="store_true")
parser.add_argument("--verbose", "-v", help="turn on program status information output", action="store_true")
parser.add_argument("--input", "-i", help="Input S3 bucket object i.e. s3://irs-form-990/index_2017.csv (required)",
                    type=str)
parser.add_argument("--output", "-o", help="Output S3 bucket object i.e. s3://mybucketname/index_2017.json (required)",
                    type=str)
parser.add_argument("--region", "-r", help="S3 region to connect to for the output bucket (default=us-east-1)",
                    type=str)
parser.add_argument("--profile", "-p", help="AWS Profile context (default=default)", type=str)
parser.add_argument("--tempfiles", "-t", help="Use disk-based tempfiles instead of memory for processing (default=no)",
                    action="store_true")
parser.add_argument("--chunksize", "-c", help="Chunk size for number of CSV rows per chunk (default=100000)",
                    type=int)

args = parser.parse_args()
logger.setLevel('CRITICAL')
if args.verbose:
    logger.setLevel('INFO')
if args.debug:
    logger.setLevel('DEBUG')
if not args.input:
    parser.error("You must specify an object to process with the \"-i\" argument.")
    quit()
if not args.input:
    parser.error("You must specify an object to process with the \"-o\" argument.")
    quit()
if not args.profile:
    args.profile = "default"
if not args.region:
    args.region = "us-east-1"
if args.chunksize:
    chunk_size = args.chunksize
else:
    chunk_size = 100000

if args.tempfiles:
    logging.info("Using tempfiles for chunk files")
else:
    logging.info("Using memory files for chunk files")

#################
# Banner

#################
# Create S3 Input Bucket Connection Objects
try:
    input_url = urlparse(args.input)
    logging.info("Input Bucket: s3://" + input_url.netloc + " Object: " + input_url.path)
    # Use workaround for dots in names, see: https://github.com/boto/boto/issues/2836
    s3_connection = boto.connect_s3(calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                                    profile_name=args.profile)
    input_bucket = s3_connection.get_bucket(input_url.netloc, validate=True)
    input_key = Key(input_bucket)
    input_key.key = input_url.path
    csvfile = smart_open.smart_open(input_key, "rb")
except:
    logging.error('Failed to open S3 input bucket object: ' + args.input, exc_info=True)
    raise

#################
# Create S3 Output Bucket Connection Objects
try:
    output_url = urlparse(args.output)
    logging.info("Output Bucket: s3://" + output_url.netloc + " Object: " + output_url.path)
    s3_connection = boto.s3.connect_to_region(
        args.region,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        profile_name=args.profile)
    output_bucket = s3_connection.get_bucket(output_url.netloc)
    output_key = output_bucket.new_key(output_url.path)
    jsonfile = output_bucket.initiate_multipart_upload(output_url.path)
except:
    logging.error('Failed to open S3 output bucket object: ' + args.output, exc_info=True)
    raise

#################
# Process CSV File
chunk_file = []
reader = csv.reader(csvfile)
csv_header = []
csv_data = {}
row_counter = 0
chunk = 1

try:
    for row_data in reader:
        row_counter = row_counter + 1
        # Process the CSV header row
        if row_counter == 1:
            csv_header = row_data
            if args.tempfiles == True:
                chunk_file.append(tempfile.NamedTemporaryFile())
                logging.info('Created initial temporary file: ' + chunk_file[0].name)
            else:
                chunk_file.append(cStringIO.StringIO())
                logging.info('Created initial in-memory temporary file')

        # Process a CSV data row
        else:
            column_num = 0
            for column in csv_header:
                # Remove non-printing characters from the strings and add to a JSON structure
                csv_data.update(
                    {filter(lambda x: x in string.printable, column): filter(lambda x: x in string.printable,
                                                                             row_data[column_num])})
                column_num = column_num + 1
            # Write the resulting JSON structure to S3 Output bucket
            try:
                chunk_file[chunk - 1].write(json.dumps(csv_data) + '\n')
                # When we hit a chunk boundary
                if not (row_counter) % chunk_size:
                    chunk_bytes = chunk_file[chunk - 1].tell()
                    logging.info('Writing chunk ' + str(chunk) + ' with size of ' + str(chunk_bytes) + ' bytes.')
                    chunk_file[chunk - 1].seek(0)
                    jsonfile.upload_part_from_file(fp=chunk_file[chunk - 1], part_num=chunk, size=chunk_bytes)
                    chunk = chunk + 1
                    if args.tempfiles == True:
                        chunk_file.append(tempfile.NamedTemporaryFile())
                        logging.info('Created temporary file: ' + chunk_file[chunk - 1].name)
                    else:
                        chunk_file.append(cStringIO.StringIO())
                        logging.info('Created in-memory temporary file ' + str(chunk))
            except:
                logging.error('Failed to write file chunk: ' + str(chunk))
                jsonfile.cancel_upload()
                raise
# This keeps cancelled uploads from creating orphaned multi-part upload jobs from hanging around
# http://docs.aws.amazon.com/cli/latest/reference/s3api/abort-multipart-upload.html
# aws s3api abort-multipart-upload --bucket fakedata-source.beyondsoft.us --profile beyondsoft --key index_2017.json --upload-id <myid>
#
# http://docs.aws.amazon.com/cli/latest/reference/s3api/list-multipart-uploads.html
# aws s3api list-multipart-uploads --bucket fakedata-source.beyondsoft.us --profile beyondsoft
except KeyboardInterrupt:
    logging.error('Control-C was used to stop the program...cancelling upload.')
    jsonfile.cancel_upload()
    if args.tempfiles == True:
        logging.info('Closing temporary files....')
        for fileiter in chunk_file:
            fileiter.close()
    sys.exit(1)

# Final Chunk Writer
finally:
    try:
        logging.info('Writing final chunk...')
        chunk_bytes = chunk_file[chunk - 1].tell()
        logging.info('Writing chunk ' + str(chunk) + ' with size of ' + str(chunk_bytes) + ' bytes.')
        chunk_file[chunk - 1].seek(0)
        jsonfile.upload_part_from_file(fp=chunk_file[chunk - 1], part_num=chunk, size=chunk_bytes)
    except:
        logging.error('Failed to write file chunk: ' + str(chunk))
        jsonfile.cancel_upload()
        if args.tempfiles == True:
            logging.info('Closing temporary files....')
            for fileiter in chunk_file:
                fileiter.close()
        raise
    try:
        jsonfile.complete_upload()
    except:
        logging.error('Failed to complete upload...initiating cancel...')
        jsonfile.cancel_upload()
        if args.tempfiles == True:
            logging.info('Closing temporary files....')
            for fileiter in chunk_file:
                fileiter.close()
        raise

# Close and delete physical tempfiles
if args.tempfiles == True:
    logging.info('Closing temporary files....')
    for fileiter in chunk_file:
        fileiter.close()
