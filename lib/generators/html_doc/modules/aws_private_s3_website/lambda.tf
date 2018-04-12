data "archive_file" "lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/basic_auth.zip"
  source_file = "${path.module}/basic_auth_lambda/basic_auth.js"
}

resource "aws_lambda_function" "basic_auth_lambda" {
  provider         = "aws.east"
  filename         = "${data.archive_file.lambda_zip.output_path}"
  function_name    = "${var.lambda_name}"
  role             = "${aws_iam_role.lambda_role.arn}"
  handler          = "basic_auth.handler"
  source_code_hash = "${data.archive_file.lambda_zip.output_base64sha256}"
  runtime          = "nodejs6.10"
  publish          = true

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_iam_role_policy" "lambda_policy" {
  provider = "aws.east"
  role     = "${aws_iam_role.lambda_role.id}"
  policy   = "${data.aws_iam_policy_document.lambda_policy_document.json}"
}

data "aws_caller_identity" "current" {
  provider = "aws.east"
}

data "aws_iam_policy_document" "lambda_policy_document" {
  provider = "aws.east"

  statement {
    actions = [
      "logs:CreateLogGroup",
    ]

    resources = [
      "arn:aws:logs:us-east-1:${data.aws_caller_identity.current.account_id}:*",
    ]
  }

  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = [
      "arn:aws:logs:us-east-1:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*:*",
    ]
  }
}

resource "aws_iam_role" "lambda_role" {
  provider           = "aws.east"
  assume_role_policy = "${data.aws_iam_policy_document.lambda_assume_role_policy_document.json}"
}

data "aws_iam_policy_document" "lambda_assume_role_policy_document" {
  provider = "aws.east"

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com", "edgelambda.amazonaws.com"]
    }
  }
}
