# TODO: Allow users to specify an existing CloudTrail
resource "aws_cloudtrail" "trail" {
  provider                      = "aws.myregion"
  name                          = "${var.trail_name}"
  s3_bucket_name                = "${aws_s3_bucket.bucket.id}"
  include_global_service_events = true
  cloud_watch_logs_role_arn     = "${aws_iam_role.cloudwatch_logs_role.arn}"
  cloud_watch_logs_group_arn    = "${aws_cloudwatch_log_group.log_group.arn}"
  is_multi_region_trail         = true

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_cloudwatch_log_group" "log_group" {
  provider          = "aws.myregion"
  name              = "/test/convergdb/${var.trail_name}"
  retention_in_days = 30

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_cloudwatch_log_subscription_filter" "lambdafunction_logfilter" {
  provider        = "aws.myregion"
  name            = "${var.trail_name}_logfilter"
  log_group_name  = "${aws_cloudwatch_log_group.log_group.name}"
  filter_pattern  = "{ $.eventSource = \"athena.amazonaws.com\" && $.eventName = \"StartQueryExecution\" }"
  destination_arn = "${aws_lambda_function.tracker_lambda.arn}"
}

resource "aws_s3_bucket" "bucket" {
  provider = "aws.myregion"
  bucket   = "${var.bucket_name}"

  policy = <<BUCKET_POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AWSCloudTrailAclCheck",
            "Effect": "Allow",
            "Principal": {
              "Service": "cloudtrail.amazonaws.com"
            },
            "Action": "s3:GetBucketAcl",
            "Resource": "arn:aws:s3:::${var.bucket_name}"
        },
        {
            "Sid": "AWSCloudTrailWrite",
            "Effect": "Allow",
            "Principal": {
              "Service": "cloudtrail.amazonaws.com"
            },
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::${var.bucket_name}/*",
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-acl": "bucket-owner-full-control"
                }
            }
        }
    ]
}
BUCKET_POLICY

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_iam_role_policy" "cloudwatch_logs_role_policy" {
  provider = "aws.myregion"
  role     = "${aws_iam_role.cloudwatch_logs_role.id}"
  policy   = "${data.aws_iam_policy_document.cloudwatch_logs_policy_document.json}"
}

data "aws_iam_policy_document" "cloudwatch_logs_policy_document" {
  provider = "aws.myregion"

  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents",
    ]

    resources = [
      "${aws_cloudwatch_log_group.log_group.arn}:*",
    ]
  }
}

resource "aws_iam_role" "cloudwatch_logs_role" {
  provider           = "aws.myregion"
  assume_role_policy = "${data.aws_iam_policy_document.cloudwatch_logs_assume_role_policy_document.json}"
}

data "aws_iam_policy_document" "cloudwatch_logs_assume_role_policy_document" {
  provider = "aws.myregion"

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }
  }
}
