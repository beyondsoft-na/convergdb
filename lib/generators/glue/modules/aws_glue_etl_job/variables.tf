variable "region" {}
variable "job_name" {}

variable "service_role" {
  default = ""
}

variable "stack_name" {}
variable "local_script" {}
variable "local_pyspark_library" {}
variable "script_bucket" {}
variable "script_key" {}
variable "pyspark_library_key" {}
variable "schedule" {}
variable "dpu" {}
variable "deployment_id" {}
variable "admin_bucket" {}
variable "data_bucket" {}
variable "cloudwatch_namespace" {}
variable "sns_topic" {}
