# Copyright © 2020 Beyondsoft Consulting, Inc.

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the “Software”), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
# BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

module ConvergDB
  # version number for this gem
  VERSION = '0.0.6.17'
  
  # commit associated with this version of convergdb
  TERRAFORM_MODULES_COMMIT_ID = '2e4c987798bc5f1c19f9f41f0e6bc0f3bc364e6b'

  TERRAFORM_MODULES = {
    aws_athena_database: "github.com/beyondsoft-na/convergdb-terraform//aws_athena_database?ref=#{ConvergDB::TERRAFORM_MODULES_COMMIT_ID}",
    aws_athena_relations: "github.com/beyondsoft-na/convergdb-terraform//aws_athena_relations?ref=#{ConvergDB::TERRAFORM_MODULES_COMMIT_ID}",
    aws_glue_table: "github.com/beyondsoft-na/convergdb-terraform//aws_glue_table?ref=#{ConvergDB::TERRAFORM_MODULES_COMMIT_ID}",
    aws_glue_database: "github.com/beyondsoft-na/convergdb-terraform//aws_glue_database?ref=#{ConvergDB::TERRAFORM_MODULES_COMMIT_ID}",
    aws_glue_etl_job: "github.com/beyondsoft-na/convergdb-terraform//aws_glue_etl_job?ref=#{ConvergDB::TERRAFORM_MODULES_COMMIT_ID}",
    aws_fargate_etl_job: "github.com/beyondsoft-na/convergdb-terraform//aws_fargate_etl_job?ref=#{ConvergDB::TERRAFORM_MODULES_COMMIT_ID}",
    aws_s3_streaming_inventory: "github.com/beyondsoft-na/convergdb-terraform//aws_s3_streaming_inventory?ref=#{ConvergDB::TERRAFORM_MODULES_COMMIT_ID}",
    convergdb_bootstrap: "github.com/beyondsoft-na/convergdb-terraform//convergdb_bootstrap?ref=#{ConvergDB::TERRAFORM_MODULES_COMMIT_ID}"
  }

  DOCKERHUB_IMAGE_NAME = 'beyondsoftna/convergdb'

  DOCKERHUB_IMAGE_SHA256 = 'sha256:c40f79636467f08391dacd04c90964816dbab56a789ae0f4df0006d9cba5a69c'
end
