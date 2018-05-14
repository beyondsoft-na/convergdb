variable "region" {}

# source bucket
variable "source_bucket" {}

# destination bucket for firehose
variable "destination_bucket" {}

# prefix for writing output files
variable "destination_prefix" {}

# kinesis firehose stream name
variable "firehose_stream_name" {}

# provide lambda name
variable "lambda_name" {}
