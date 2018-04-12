provider "aws" {
  alias  = "myregion"
  region = "${var.region}"
}

# unique identifier for this module
resource "random_id" "module_id" {
  byte_length = 8
}

output "module_id" {
  value = "${random_id.module_id.dec}"
}

resource "aws_cloudformation_stack" "glue_etl_stack" {
  name = "${var.stack_name}-${random_id.module_id.dec}"

  template_body = <<STACK
AWSTemplateFormatVersion: "2010-09-09"
Description: Glue ETL Job and Schedule

Resources:

  GlueJob:
    Type: "AWS::Glue::Job"
    Properties:
      Name: "${var.job_name}"
      Role: ${coalesce(var.service_role,aws_iam_role.glue_service_role.name)}
      DefaultArguments:
        "--extra-py-files": "s3://${var.script_bucket}/${var.pyspark_library_key}"
        "--convergdb_deployment_id": ${var.deployment_id}
        "--conf": "spark.yarn.executor.memoryOverhead=1024"
      Command:
        Name: glueetl
        ScriptLocation: "s3://${var.script_bucket}/${var.script_key}"
      AllocatedCapacity: ${var.dpu}

  ScheduledGlueTrigger:
    Type: "AWS::Glue::Trigger"
    Properties:
      Type: SCHEDULED
      Name: "convergdb-${var.job_name}"
      Schedule: "${var.schedule}"
      Actions:
        - JobName: !Ref GlueJob
STACK

  depends_on = [
    "aws_s3_bucket_object.script_object",
    "aws_s3_bucket_object.library_object",
    "aws_iam_role_policy_attachment.attach_glue_service",
    "aws_iam_role_policy.s3_access",
  ]

  provider = "aws.myregion"

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_iam_role" "glue_service_role" {
  name = "convergdb-${var.job_name}-${random_id.module_id.dec}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

  provider = "aws.myregion"
}

resource "aws_iam_role_policy_attachment" "attach_glue_service" {
  role       = "${aws_iam_role.glue_service_role.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  provider   = "aws.myregion"
}

resource "aws_iam_role_policy" "s3_access" {
  name = "convergdb-${var.job_name}-s3-access-policy-${random_id.module_id.dec}"
  role = "${aws_iam_role.glue_service_role.name}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:*"
      ],
      "Effect": "Allow",
      "Resource": "*"
    },
    {
      "Action": [
        "athena:*"
      ],
      "Effect": "Allow",
      "Resource": "*"
    },
    {
      "Action": [
        "sns:Publish"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:sns:::convergdb*"
    },
    {
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }
  ]
}
EOF

  provider = "aws.myregion"
}

data "template_file" "script_object_source" {
  template = "${file("${var.local_script}")}"

  vars {
    admin_bucket = "${var.admin_bucket}"
    data_bucket = "${var.data_bucket}"
    deployment_id = "${var.deployment_id}"
    region = "${var.region}"
    sns_topic = "${var.sns_topic}"
    cloudwatch_namespace = "${var.cloudwatch_namespace}"
  }
}

resource "aws_s3_bucket_object" "script_object" {
  bucket   = "${var.script_bucket}"
  key      = "${var.script_key}"
  content  = "${data.template_file.script_object_source.rendered}"
  etag     = "${md5("${data.template_file.script_object_source.rendered}")}"
  provider = "aws.myregion"

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_s3_bucket_object" "library_object" {
  bucket   = "${var.script_bucket}"
  key      = "${var.pyspark_library_key}"
  source   = "${var.local_pyspark_library}"
  etag     = "${md5(file("${var.local_pyspark_library}"))}"
  provider = "aws.myregion"

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}
