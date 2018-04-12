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

dynamodb_resource = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
paginator = dynamodb_client.get_paginator('scan')

athena = boto3.client('athena')
cloudwatch = boto3.client('cloudwatch')

namespace = os.getenv('METRICS_NAMESPACE', '/test/convergdb/athena')

BYTES_TO_TB = 1099511627776
TB_DOLLARS = 5
METRICS_BATCH_SIZE = 20
IDS_BATCH_SIZE = 50


def bytes_to_dollars(bytes):
    return (bytes / BYTES_TO_TB) * TB_DOLLARS

def batch_metrics(metrics):
    return [metrics[i:i + METRICS_BATCH_SIZE] for i in range(0, len(metrics), METRICS_BATCH_SIZE)]

def put_metrics(metrics):
    logger.info("Posting [%d] metrics to namespace [%s]" %
                (len(metrics), namespace))

    for batch in batch_metrics(metrics):
        cloudwatch.put_metric_data(Namespace=namespace, MetricData=batch)


def build_success_metrics(timestamp, execution_time, scanned_bytes):
    # CloudWatch can't aggregate over custom metrics, so we're *not* adding a
    #  database dimension here.
    return [{'MetricName': 'EngineExecutionTime', 'Timestamp': timestamp,
             'Value': execution_time, 'Unit': 'Milliseconds'},
            {'MetricName': 'DataScanned', 'Timestamp': timestamp,
             'Value': scanned_bytes, 'Unit': 'Bytes'},
            {'MetricName': 'EstimatedCost', 'Timestamp': timestamp,
             # Use TB_DOLLARS as a dimension, so when that value changes we can
             #  cleanly separate the metrics.
             'Dimensions': [{'Name': 'DollarsPerTerabyte', 'Value': str(TB_DOLLARS)}],
             'Value': bytes_to_dollars(scanned_bytes), 'Unit': 'Count'},
            {'MetricName': 'QuerySuccesses', 'Timestamp': timestamp,
             'Value': 1, 'Unit': 'Count'}]


def build_failure_metrics(timestamp, reason):
    return [{'MetricName': 'QueryFailures', 'Timestamp': timestamp,
             'Value': 1, 'Unit': 'Count'}]


def executions_to_metrics(executions):
    metrics = []

    for execution in executions:
        status = execution.get('Status')
        timestamp = status.get('CompletionDateTime')
        state = status.get('State')
        # context = execution.get('QueryExecutionContext')
        # database = context.get('Database')

        if state == 'SUCCEEDED':
            statistics = execution.get('Statistics')
            execution_ms = statistics.get('EngineExecutionTimeInMillis')
            scanned_bytes = statistics.get('DataScannedInBytes')
            success_metrics = build_success_metrics(timestamp, execution_ms, scanned_bytes)
            metrics.extend(success_metrics)
        elif state == 'FAILED':
            reason = status.get('StateChangeReason')
            failure_metrics = build_failure_metrics(timestamp, reason)
            metrics.extend(failure_metrics)

    logger.info(metrics)
    return metrics


def errors_to_metrics(errors):
    metrics = []


def batch_ids(ids):
    return [ids[i:i + IDS_BATCH_SIZE] for i in range(0, len(ids), IDS_BATCH_SIZE)]


def filter_completed_executions(executions):
    completed_executions = []

    for execution in executions:
        status = execution.get('Status')
        state = status.get('State')
        if state in ['SUCCEEDED', 'FAILED', 'CANCELED']:
            completed_executions += [execution]

    return completed_executions


def get_query_executions(query_ids):
    executions = []

    for batch in batch_ids(query_ids):
        executions += (athena.batch_get_query_execution(QueryExecutionIds=batch).get('QueryExecutions'))

    return executions


def remove_completed(dynamodb_table, executions):
    query_ids = []

    for execution in executions:
        query_id = execution.get('QueryExecutionId')
        query_ids += [query_id]

    with dynamodb_table.batch_writer() as batch:
        for query_id in query_ids:
            batch.delete_item(Key={'queryId': query_id})


def get_query_ids(table_name):
    query_ids = []
    responses = paginator.paginate(TableName=table_name, AttributesToGet=['queryId'])
    for response in responses:
        for item in response.get('Items'):
            query_ids += [item['queryId']['S']]
    return query_ids


def process(table_name, dynamodb_table):
    query_ids = get_query_ids(table_name)

    logger.info("Found [%d] query ids in table [%s]" %
                (len(query_ids), table_name))

    executions = get_query_executions(query_ids)

    logger.info("Found [%d] query executions." %
                (len(executions)))

    completed_executions = filter_completed_executions(executions)

    logger.info("Found [%d] completed query executions." %
                (len(completed_executions)))

    metrics = executions_to_metrics(completed_executions)

    remove_completed(dynamodb_table, completed_executions)


def handler(scheduled_event, context):
    logger.debug(json.dumps(scheduled_event))
    table_name = os.getenv('QUERY_TRACKING_TABLE')
    dynamodb_table = dynamodb_resource.Table(table_name)
    process(table_name, dynamodb_table)
