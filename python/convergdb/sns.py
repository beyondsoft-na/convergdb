from convergdb_logging import *

import boto3

def publish_sns(region, topic_arn, subject, message):
  try:
    convergdb_log("publishing sns subject: " + str(subject) + " to topic: " + str(topic_arn) + "...")
    client = boto3.client('sns', region_name = region)
    response = client.publish(
      TopicArn = topic_arn,
      Subject = subject,
      Message = message
    )
    convergdb_log("sns message sent successfully!")
  except:
    pass