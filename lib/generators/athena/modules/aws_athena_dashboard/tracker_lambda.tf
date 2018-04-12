data "archive_file" "tracker_lambda_zip" {
  type        = "zip"
  output_path = "athena_query_reporter.zip"
  source_file = "athena_query_reporter_lambda/athena_query_reporter.py"
}

resource "aws_lambda_function" "tracker_lambda" {
  provider         = "aws.myregion"
  filename         = "${data.archive_file.tracker_lambda_zip.output_path}"
  function_name    = "athena_query_tracker"
  role             = "${aws_iam_role.tracker_lambda_role.arn}"
  handler          = "athena_query_tracker.handler"
  source_code_hash = "${data.archive_file.tracker_lambda_zip.output_base64sha256}"
  runtime          = "python3.6"

  environment {
    variables {
      QUERY_TRACKING_TABLE = "${aws_dynamodb_table.query_tracker_table.name}"
    }
  }

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_iam_role_policy" "tracker_lambda_policy" {
  provider = "aws.myregion"
  role     = "${aws_iam_role.tracker_lambda_role.id}"
  policy   = "${data.aws_iam_policy_document.tracker_lambda_policy_document.json}"
}

data "aws_iam_policy_document" "tracker_lambda_policy_document" {
  provider = "aws.myregion"

  statement {
    actions = [
      "logs:CreateLogGroup",
    ]

    resources = [
      "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:*",
    ]
  }

  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = [
      "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*:*",
    ]
  }

  statement {
    actions = [
      "dynamodb:PutItem",
    ]

    resources = [
      "${aws_dynamodb_table.query_tracker_table.arn}",
    ]
  }
}

resource "aws_iam_role" "tracker_lambda_role" {
  provider           = "aws.myregion"
  assume_role_policy = "${data.aws_iam_policy_document.tracker_lambda_assume_role_policy_document.json}"
}

data "aws_iam_policy_document" "tracker_lambda_assume_role_policy_document" {
  provider = "aws.myregion"

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_lambda_permission" "allow_cloudwatch_tracker" {
  provider      = "aws.myregion"
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.tracker_lambda.function_name}"
  principal     = "logs.${var.region}.amazonaws.com"
}

resource "aws_dynamodb_table" "query_tracker_table" {
  provider = "aws.myregion"

  name           = "convergdb_query_tracker_${var.deployment_id}"
  read_capacity  = 5
  write_capacity = 5
  hash_key       = "queryId"

  attribute {
    name = "queryId"
    type = "S"
  }
}
