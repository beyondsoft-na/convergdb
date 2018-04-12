# unique identifier for this module
resource "random_id" "module_id" {
  byte_length = 8
}

provider "aws" {
  alias  = "myregion"
  region = "${var.region}"
}

# template file is used because there may be variables inside
# the stack definition which need to be resolved.
data "template_file" "stack" {
  template = "${file("${var.local_stack_file_path}")}"

  vars {
    admin_bucket = "${var.admin_bucket}"
    data_bucket = "${var.data_bucket}"
    deployment_id = "${var.deployment_id}"
    database_stack_id = "${var.database_stack_id}"
    aws_account_id = "${var.aws_account_id}"
  }
}

# stack file is pushed to S3 because it may be too large for inline use
resource "aws_s3_bucket_object" "stack" {
  bucket = "${var.admin_bucket}"
  key = "${var.s3_stack_key}"
  content = "${data.template_file.stack.rendered}"
  etag = "${md5("${data.template_file.stack.rendered}")}"
}

resource "aws_cloudformation_stack" "athena_relation_stack" {
  name          = "convergdb-${var.stack_name}-${var.deployment_id}"
  template_url = "https://s3.amazonaws.com/${var.admin_bucket}/${var.s3_stack_key}"
  provider      = "aws.myregion"

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
  depends_on = ["aws_s3_bucket_object.stack"]
}
