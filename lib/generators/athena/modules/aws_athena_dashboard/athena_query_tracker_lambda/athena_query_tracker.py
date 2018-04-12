# ConvergDB - DevOps for Data
# Copyright (C) 2018 Beyondsoft Consulting, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import base64
import boto3
import gzip
import logging
import json
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

def persist_query_ids(query_ids):
    for id in query_ids:
        try:
            table.put_item(
                Item={'queryId': id, 'state': 'UNKNOWN'},
                ConditionExpression='attribute_not_exists(queryId)'
            )
        except botocore.exceptions.ClientError as e:
            # Ignore the ConditionalCheckFailedException, bubble up others
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise


def process_ctl_event(ctl_event):
    query_ids = []

    for e in ctl_event.get('logEvents'):
        cloudtrail_event = json.loads(e['message'])
        event_name = cloudtrail_event.get('eventName')

        if event_name != 'StartQueryExecution':
            continue

        response = cloudtrail_event['responseElements']
        if response:
            query_id = response['queryExecutionId']
            if response and query_id:
                logger.info("queryExecutionId: %s" % query_id)
                query_ids.append(query_id)

        error_code = cloudtrail_event.get('errorCode')
        error_msg = cloudtrail_event.get('errorMessage')

        if error_code:
            logger.info("errorCode: %s" % error_code)

        if error_msg:
            logger.info("errorMessage: %s" % error_msg)

    logger.info("Gathered [%d] query_ids" % len(query_ids))
    return query_ids


def unpack_cwl_event(cwl_event):
    return json.loads(gzip.decompress(base64.b64decode(cwl_event['awslogs']['data'])))


def process(table, ctl_event):
    query_ids = process_ctl_event(ctl_event)
    persist_query_ids(table, query_ids)


def handler(cwl_event, context):
    logger.debug(json.dumps(cwl_event))
    table = dynamodb.Table(os.getenv('QUERY_TRACKING_TABLE'))
    ctl_event = unpack_cwl_event(cwl_event)
    process(table, ctl_event)
