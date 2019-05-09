import boto3
import re
import functools
from multiprocessing import Pool

# returns a list of regexes to be used.
def regexes(partition_fields):
  return [
    re.compile(p + '\\=.*') for p in partition_fields
  ]

def convergdb_database_name(convergdb_s3_key):
  return '__'.join(convergdb_s3_key.split('/')[1].split('.')[0:3])

def convergdb_database_path(convergdb_s3_key):
  return '.'.join(convergdb_s3_key.split('/')[1].split('.')[0:3])
  
def convergdb_table_name(convergdb_s3_key):
  return convergdb_s3_key.split('/')[1].split('.')[-1]

def partition_values(partition_kvp, partition_fields):
  retval = []
  for i in range(len(partition_fields)):
    retval.append(partition_kvp[i].split(partition_fields[i] + '=')[1])
  return retval

def partition_values_from_key(key, partition_field_regexes):
  parts = key.split('/')
  retval = []
  for r in partition_field_regexes:
    for k in parts:
      if r.match(k):
        retval.append(k)
  return retval

def memoize(func):
  cache = func.cache = {}
  @functools.wraps(func)
  def memoized_func(*args, **kwargs):
    key = str(args) + str(kwargs)
    if key not in cache:
      cache[key] = func(*args, **kwargs)
    return cache[key]
  return memoized_func
    
# this function is memoized to preven too many aws calls
@memoize
def get_table_metadata(database_name, table_name, region):
  print "getting metadata for table " + database_name + '.' + table_name
  client = boto3.client('glue', region_name=region)
  table_metadata = client.get_table(
    DatabaseName=database_name,
    Name=table_name
  )
  return table_metadata

def partition_path(s3_key, partition_values):
  parts = s3_key.split('/')
  i = parts.index(partition_values[-1])
  return '/'.join(parts[0:(i+1)]) + '/'

def analyze_s3_key(bucket, s3_key, region):
  if convergdb_database_name(s3_key) and convergdb_table_name(s3_key):
    table_metadata = get_table_metadata(
      convergdb_database_name(s3_key),
      convergdb_table_name(s3_key),
      region
    )
    
    partition_keys = list(
      map(
        lambda part_key: str(part_key['Name']),
        table_metadata['Table']['PartitionKeys']
      )
    )
    database_name = convergdb_database_name(s3_key)
    table_name = convergdb_table_name(s3_key)
    
    raw_partition_values = partition_values_from_key(
      s3_key,
      regexes(
        partition_keys
      )
    )
    
    values = partition_values(
      raw_partition_values,
      partition_keys
    )
    
    path = partition_path(
      s3_key,
      raw_partition_values
    )
    
    return {
      'partition': database_name + '/' + table_name + '/' + '/'.join(raw_partition_values),
      'database': database_name,
      'table': table_name,
      'values': values,
      'location': 's3://' + bucket + '/' + path
    }
    
def write_partition(params):
  try:
    database = params['database']
    table    = params['table']  
    values   = params['values']    
    location = params['location']     
    region   = params['region']  

    table_metadata = get_table_metadata(
      database,
      table,
      region
    )
    
    storage_descriptor = table_metadata['Table']['StorageDescriptor']
    storage_descriptor['Location'] = location
    client = boto3.client('glue', region_name=region)
    response = client.create_partition(
        DatabaseName=database,
        TableName=table,
        PartitionInput={
            'Values': values,
            'StorageDescriptor': storage_descriptor
        }
    )
    return response
  except Exception as error:
    if hasattr(error, 'response'):
      if error.response['Error']['Code'] == 'AlreadyExistsException':
        return {'response' : 'AlreadyExistsException', 'params': params}
    else:
      return {'response' : str(error) ,'params': params}

def s3_list_objects_for_prefix(bucket, prefix, region):
  print('searching for prefix ' + prefix + ' in bucket ' + bucket)
  ret = []
  client = boto3.client('s3', region_name = region)
  paginator = client.get_paginator('list_objects_v2')
  page_iterator = paginator.paginate(
    Bucket = bucket,
    Prefix = prefix
  )
  for page in page_iterator:
    if 'Contents' in page:
      ret += page['Contents']
  return ret

def keys_from_s3_object_list(s3_object_list):
  return list(
    map(
      lambda o: o['Key'],
      s3_object_list
    )
  )

def key_is_valid(s3_key):
  temporary = re.compile(r'.*\/\_temporary\/.*')
  if temporary.match(s3_key):
    return False
  else:
    return True

def keys_are_valid(s3_keys):
  return list(
    filter(
      lambda k: key_is_valid(k),
      s3_keys
    )
  )

def update_all_partitions(bucket, prefix, region):
  print 'updating partition information...'

  keys = keys_are_valid(
    keys_from_s3_object_list(
      s3_list_objects_for_prefix(
        bucket,
        prefix,
        region
      )
    )
  )
  print 'found ' + str(len(keys)) + ' objects'
  h = {}
  
  client = boto3.client('glue', region_name=region)

  for k in keys:
    try:
      a = analyze_s3_key(
        bucket,
        k,
        region
      )
      if a['partition'] not in h.keys():
        h[a['partition']] = a
    except Exception as error:
      pass
  
  print 'found ' + str(len(h.keys())) + ' partitions'
  
  partitions = []
  for k in h.keys():
    partitions.append(
      {
        'database': h[k]['database'],
        'table': h[k]['table'],
        'values': h[k]['values'],
        'location': h[k]['location'],
        'region': region
      }
    )
  p = Pool(16)
  results = p.map(write_partition, partitions)
  for i in results:
    print i

