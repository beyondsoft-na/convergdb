provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

resource "aws_sns_topic" "convergdb-notifications" {
  name = "convergdb-${var.deployment_id}"
}

resource "aws_cloudwatch_dashboard" "dashboard" {
  provider       = aws
  dashboard_name = "convergdb-${var.deployment_id}"

  dashboard_body = <<BODY
{
    "widgets": [
        {
            "type": "metric",
            "x": 0,
            "y": 0,
            "width": 21,
            "height": 3,
            "properties": {
                "view": "singleValue",
                "stacked": false,
                "metrics": [
                    [ "convergdb/${var.deployment_id}", "batch_failure", { "stat": "Sum", "period": 86400 } ],
                    [ ".", "batch_success", { "stat": "Sum", "period": 86400 } ],
                    [ ".", "source_data_processed", { "period": 86400, "stat": "Sum" } ],
                    [ ".", "source_data_processed_uncompressed_estimate", { "period": 86400, "stat": "Sum" } ],
                    [ ".", "source_files_processed", { "period": 86400, "stat": "Sum" } ]
                ],
                "region": "${var.region}",
                "title": "Summary"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 3,
            "width": 21,
            "height": 6,
            "properties": {
                "view": "timeSeries",
                "stacked": false,
                "metrics": [
                    [ "convergdb/${var.deployment_id}", "source_data_processed", { "period": 21600, "stat": "Sum" } ],
                    [ ".", "source_data_processed_uncompressed_estimate", { "period": 21600, "stat": "Sum" } ]
                ],
                "region": "${var.region}",
                "title": "Data Volume"
            }
        }
    ]
}
BODY
}