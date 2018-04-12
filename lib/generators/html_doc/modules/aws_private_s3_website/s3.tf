locals {
  bucket_name = "${replace(var.site_name, "_", "-")}"
}

resource "aws_s3_bucket" "bucket" {
  provider = "aws.east"
  bucket   = "${local.bucket_name}"

  website {
    index_document = "index.html"
    error_document = "error.html"
  }

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_s3_bucket_object" "index_html" {
  provider     = "aws.east"
  bucket       = "${aws_s3_bucket.bucket.bucket}"
  key          = "index.html"
  source       = "${path.module}/index.html"
  etag         = "${md5(file("index.html"))}"
  content_type = "text/html"

  tags {
    "convergdb:deployment" = "${var.deployment_id}"
    "convergdb:module"     = "${random_id.module_id.dec}"
  }
}

resource "aws_s3_bucket_policy" "bucket_policy" {
  provider = "aws.east"
  bucket   = "${aws_s3_bucket.bucket.id}"
  policy   = "${data.aws_iam_policy_document.s3_policy.json}"
}

resource "aws_cloudfront_origin_access_identity" "origin_access_identity" {
  provider = "aws.east"
}

data "aws_iam_policy_document" "s3_policy" {
  provider = "aws.east"

  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.bucket.arn}/*"]

    principals {
      type        = "AWS"
      identifiers = ["${aws_cloudfront_origin_access_identity.origin_access_identity.iam_arn}"]
    }
  }

  statement {
    actions   = ["s3:ListBucket"]
    resources = ["${aws_s3_bucket.bucket.arn}"]

    principals {
      type        = "AWS"
      identifiers = ["${aws_cloudfront_origin_access_identity.origin_access_identity.iam_arn}"]
    }
  }
}
