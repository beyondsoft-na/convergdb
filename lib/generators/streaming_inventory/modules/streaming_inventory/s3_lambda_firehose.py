import boto3
import json
import time
import os

def lambda_handler(event, context):
    # get the value of FIREHOSE_STREAM_NAME
    try:
        firehose_stream_name = os.environ['FIREHOSE_STREAM_NAME']
    except KeyError:
        print("Please specify the value of env var FIREHOSE_STREAM_NAME"
               "in lambda function")
        raise

    # get kinesis firehose client
    firehose = boto3.client('firehose')
    # iterate the Records in event
    for e in event['Records']:
        # save one record in a dictionary
        dic = {}
        dic['last_modified_timestamp'] = e["eventTime"].replace("T"," ").replace("Z", "")
        dic['bucket'] = e["s3"]['bucket']['name']
        dic['key'] = e["s3"]['object']['key']
        dic['size'] = e["s3"]['object']['size']
        dic['e_tag'] = e["s3"]['object']['eTag']
        dic['sequencer'] = e["s3"]['object']['sequencer']

        # put the record into firehose delivery stream
        record = (json.dumps(dic) + "\n").encode()
        firehose.put_record( DeliveryStreamName = firehose_stream_name,
                             Record = {
                                'Data' : record
                             }
                           )
        print(record)
    return
