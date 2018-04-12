variable "region" {}

variable "trail_name" {
  default = "convergdb_trail"
}

variable "bucket_name" {
  default = "convergdb-cloudtrail.beyondsoft.us"
}

variable "dashboard_name" {
  default = "ConvergDB-Athena"
}

variable "metrics_namespace" {
  default = "/test/convergdb/athena"
}

variable "deployment_id" {}
