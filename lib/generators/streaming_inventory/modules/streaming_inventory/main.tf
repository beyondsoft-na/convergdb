provider "aws" {
  region = "${var.region}"
}

resource "aws_kinesis_firehose_delivery_stream" "convergdb_firehose" {
  name        = "${var.firehose_stream_name}"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = "${aws_iam_role.firehose_role.arn}"
    bucket_arn = "arn:aws:s3:::${var.destination_bucket}"
    prefix     = "${var.destination_prefix}"
    buffer_size = 100
    buffer_interval = 60
    compression_format = "GZIP"
  }
}

resource "aws_iam_role" "firehose_role" {
  name = "${var.firehose_stream_name}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "firehose.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "firehose_policy" {
  role   = "${aws_iam_role.firehose_role.name}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Action": [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
      ],
      "Resource": [
          "arn:aws:s3:::${var.destination_bucket}",
          "arn:aws:s3:::${var.destination_bucket}/*"
      ]
    }
  ]
}
EOF
}

resource "aws_lambda_permission" "s3_trigger" {
  statement_id = "AllowExecutionFromS3Changes"
  action = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.convergdb_firehose_lambda.function_name}"
  principal = "s3.amazonaws.com"
  source_arn = "arn:aws:s3:::${var.source_bucket}"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = "${var.source_bucket}"

  lambda_function {
    lambda_function_arn = "${aws_lambda_function.convergdb_firehose_lambda.arn}"
    events              = ["s3:ObjectCreated:*"]
  }
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/s3_lambda_firehose.py"
  output_path = "${path.module}/files/s3_lambda_firehose.zip"
}

resource "aws_lambda_function" "convergdb_firehose_lambda" {
  filename         = "${data.archive_file.lambda_zip.output_path}"
  function_name    = "${var.lambda_name}"
  description      = "streaming inventory for bucket ${var.source_bucket}"
  handler          = "s3_lambda_firehose.lambda_handler"
  source_code_hash = "${data.archive_file.lambda_zip.output_base64sha256}"
  runtime          = "python3.6"
  timeout          = "300"
  role             = "${aws_iam_role.convergdb_firehose_lambda_role.arn}"
  environment {
    variables = {
      FIREHOSE_STREAM_NAME = "${var.firehose_stream_name}"
    }
  }
}

resource "aws_iam_role" "convergdb_firehose_lambda_role" {
  name = "${var.lambda_name}-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "lambda_policy" {
  role   = "${aws_iam_role.convergdb_firehose_lambda_role.name}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
        "Effect": "Allow",
        "Action": [
            "firehose:PutRecord",
            "firehose:PutRecordBatch"
        ],
        "Resource": [
            "${aws_kinesis_firehose_delivery_stream.convergdb_firehose.arn}"
        ]
    }

  ]
}
EOF
}
