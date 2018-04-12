data "archive_file" "reporter_lambda_zip" {
  type        = "zip"
  output_path = "athena_query_reporter.zip"
  source_file = "athena_query_reporter_lambda/athena_query_reporter.py"
}

resource "aws_lambda_function" "reporter_lambda" {
  provider         = "aws.myregion"
  filename         = "${data.archive_file.reporter_lambda_zip.output_path}"
  function_name    = "athena_query_reporter"
  role             = "${aws_iam_role.reporter_lambda_role.arn}"
  handler          = "athena_query_reporter.handler"
  source_code_hash = "${data.archive_file.reporter_lambda_zip.output_base64sha256}"
  runtime          = "python3.6"

  environment {
    variables {
      METRICS_NAMESPACE    = "${var.metrics_namespace}"
      QUERY_TRACKING_TABLE = "${aws_dynamodb_table.query_tracker_table.name}"
    }
  }

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_cloudwatch_event_rule" "every_five_minutes" {
  provider            = "aws.myregion"
  name                = "every-five-minutes"
  description         = "Fires every five minutes"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "report_every_five_minutes" {
  provider  = "aws.myregion"
  rule      = "${aws_cloudwatch_event_rule.every_five_minutes.name}"
  target_id = "athena_reporter"
  arn       = "${aws_lambda_function.reporter_lambda.arn}"
}

resource "aws_iam_role_policy" "reporter_lambda_policy" {
  provider = "aws.myregion"
  role     = "${aws_iam_role.reporter_lambda_role.id}"
  policy   = "${data.aws_iam_policy_document.reporter_lambda_policy_document.json}"
}

data "aws_iam_policy_document" "reporter_lambda_policy_document" {
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
      "dynamodb:Scan",
      "dynamodb:DeleteItem",
      "dynamodb:BatchWriteItem",
    ]

    resources = [
      "${aws_dynamodb_table.query_tracker_table.arn}",
    ]
  }

  statement {
    actions = [
      "athena:BatchGetQueryExecution",
    ]

    resources = [
      "*",
    ]
  }

  statement {
    actions = [
      "cloudwatch:putMetricData",
    ]

    resources = [
      "*",
    ]
  }
}

resource "aws_iam_role" "reporter_lambda_role" {
  provider           = "aws.myregion"
  assume_role_policy = "${data.aws_iam_policy_document.reporter_lambda_assume_role_policy_document.json}"
}

data "aws_iam_policy_document" "reporter_lambda_assume_role_policy_document" {
  provider = "aws.myregion"

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_lambda_permission" "allow_cloudwatch_reporter" {
  provider      = "aws.myregion"
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.reporter_lambda.function_name}"
  principal     = "events.amazonaws.com"
  source_arn    = "${aws_cloudwatch_event_rule.every_five_minutes.arn}"
}
