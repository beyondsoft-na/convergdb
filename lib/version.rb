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

module ConvergDB
  # version number for this gem
  VERSION = '0.0.6.16'
  
  # commit associated with this version of convergdb
  TERRAFORM_MODULES_COMMIT_ID = '3e96a517915008f8ed965a498a24864b71d8531e'

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
