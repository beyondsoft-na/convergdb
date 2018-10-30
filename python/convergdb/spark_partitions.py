from convergdb_logging import *

import time

# !SPARK PARTITION HANDLING

def cores_per_dpu(dpu):
  return dpu * 4

def memory_per_dpu(dpu):
  return dpu * (10**3)
  
# creates a list of tuples representing the list slice ranges
# for each chunk.
def split_indices(count, grouping):
  # creating a set of tuples
  ret = []
  if (grouping == 1) and (count == 1):
    ret.append((0, 1))
    return ret
  elif grouping >= count:
    # full range provided as one group (tuple)
    ret.append((0, count))
    return ret
  else:
    # number of groups to create
    grouping_range = 0
    if count % grouping == 0:
      # grouping is a factor of count
      grouping_range = int(count / grouping)
    else:
      # add a group for the remainder
      grouping_range = int(count / grouping) + 1
    # will execute at least once
    for i in range(grouping_range):
      lo = i * grouping
      hi = min(lo + grouping, count)
      ret.append((lo, hi))
  return ret

# calculates the total size in bytes for the list of files in diff...
# as looked up by the value stored in available dict
def loadable_file_size(available, diff):
  convergdb_log("calculating total size of " + str(len(diff)) + " files...")
  st = time.time()
  size = 0
  for d in diff:
    size += available[d]
  et = time.time()
  convergdb_log("total size calculation took " + str(et - st) + " seconds")
  convergdb_log("source data total " + str(size) + " bytes")
  return size

def coalesce_partition_target(core_count, estimated_source_bytes):
  max_partitions_per_core = 2
  target_compression_factor = 3
  target_file_size = 256*(1024**2)
  max_partitions = core_count * max_partitions_per_core
  partition_count = round(float(estimated_source_bytes) / float(target_compression_factor) / float(target_file_size))
  partition_count = min([max([partition_count, 1]), max_partitions])
  convergdb_log("calculated partition target: " + str(partition_count))
  return partition_count

def calculate_spark_partitions(total_bytes, dpu):
  partitions = None
  if dpu:
    convergdb_log("AWS Glue DPU from current run_id: " + str(dpu))
    partitions = coalesce_partition_target(
      cores_per_dpu(dpu),
      total_bytes
    )
  else:
    convergdb_log("assuming single CPU core in AWS Fargate")
    partitions = coalesce_partition_target(
      1,
      total_bytes
    )
  return int(partitions)

def available_memory_in_this_cluster(dpu):
  usable_portion = 0.25
  if dpu:
    bytes_per_dpu = 16 * (1024**3) # 16GB
    return int(dpu * bytes_per_dpu * usable_portion)
  else:
    return int(usable_portion * 4 * (1024**3))

def calculate_chunk_count(batch_uncompressed_bytes, batch_file_count, dpu):
  return batch_file_count / int(
    max(
      1,
      batch_uncompressed_bytes / available_memory_in_this_cluster(dpu)
    )
  )
