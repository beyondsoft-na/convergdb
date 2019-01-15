import boto3
import os
import time
import uuid

from convergdb_logging import *
from functools import wraps

def dynamodb_client():
  if os.environ.has_key('AWS_GLUE_REGION'):
    return boto3.client('dynamodb', region_name=os.environ['AWS_GLUE_REGION'])
  else:
    return boto3.client('dynamodb')

lock_table = os.environ['LOCK_TABLE']
lock_id = os.environ['LOCK_ID']

def acquire_lock(owner_id):
    put_params = {
        'TableName': lock_table,
        'Item': {
            'LockID': {
                'S': lock_id
            },
            'OwnerID': {
                'S': owner_id
            }
        },
        'ConditionExpression': 'attribute_not_exists(LockID)'
    }

    # Will raise an exception if the item already exists.
    # Otherwise, catches 'AccessDeniedException' and retry
    convergdb_log("Attempting conditional put: lock_id: [" + lock_id  + "], owner_id: [" + owner_id + "]")
    dynamodb_client().put_item(**put_params)
    convergdb_log("Lock acquired: [" + lock_id + "]")


def release_lock(owner_id):
    delete_params = {
        'TableName': lock_table,
        'ConditionExpression': 'OwnerID = :OwnerID',
        'ExpressionAttributeValues': {
            ':OwnerID': {
                'S': owner_id
            }
        },
        'Key': {
            'LockID': {
                'S': lock_id
            }
        }
    }

    # No exceptions raised if condition is not met.
    convergdb_log("Attempting conditional delete: lock_id: [" + lock_id  + "], owner_id: [" + owner_id + "]")
    dynamodb_client().delete_item(**delete_params)
    convergdb_log("Lock released: [" + lock_id + "]")

def lock(function):

    @wraps(function)
    def wrapper(*args, **kwargs):

        # Validate environment variables exist
        assert lock_table != None
        assert lock_id != None

        owner_id = str(uuid.uuid4())
        response = {}
        
        err = None
        try:
            acquire_lock(owner_id)
            # LOCKED at this point. Single execution in progress here.
            # - Business logic should be resilient to partial or subsequent
            #   execution (but not concurrent execution).
            response = function(*args, **kwargs)
        except Exception as e:
            # catch the exception
            err = e
        finally:
            # release the lock
            # if this fails.. it will raise an exception
            release_lock(owner_id)
            
            # raise previous exception
            if err:
              raise err 
        return response

    return wrapper
