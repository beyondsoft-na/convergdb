provider "aws" {
  alias  = "myregion"
  region = "${var.region}"
}

# unique identifier for this module
resource "random_id" "module_id" {
  byte_length = 8
}

data "aws_caller_identity" "current" {
  provider = "aws.myregion"
}
