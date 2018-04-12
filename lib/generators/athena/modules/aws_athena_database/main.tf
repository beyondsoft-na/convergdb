# unique identifier for this module
resource "random_id" "module_id" {
  byte_length = 8
}

provider "aws" {
  alias  = "myregion"
  region = "${var.region}"
}

resource "aws_cloudformation_stack" "athena_database_stack" {
  name          = "convergdb-tf-db-${var.deployment_id}-${random_id.module_id.dec}"
  template_body = "${var.stack}"
  provider      = "aws.myregion"

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

output "database_stack_id" {
  value = "${aws_cloudformation_stack.athena_database_stack.id}"
}
