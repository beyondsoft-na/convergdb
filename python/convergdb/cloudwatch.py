from convergdb_logging import *
import boto3

def put_cloudwatch_metric(region, namespace, metric, value, unit):
  try:
    metric_data = [
      {
        'MetricName': metric,
        'Value': value,
        'Unit': unit
      }
    ]
    convergdb_log("publishing cloudwatch metric: " + str(metric_data) + "to namespace: " + namespace)
    client = boto3.client('cloudwatch', region_name = region)
    response = client.put_metric_data(
      Namespace = namespace,
      MetricData = metric_data
    )
  except:
    convergdb_log("failed to publish cloudwatch metric: " + str(metric_data) + "to namespace: " + namespace)

