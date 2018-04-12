resource "aws_cloudwatch_dashboard" "dashboard" {
  provider       = "aws.myregion"
  dashboard_name = "${var.dashboard_name}"

  dashboard_body = <<BODY
    { "widgets": [
      {
        "type": "metric",
        "width": 12,
        "height": 6,
        "properties": {
          "metrics": [
            ["${var.metrics_namespace}", "QuerySuccesses" ]
          ],
          "region": "${var.region}",
          "period": 300,
          "view": "timeSeries",
          "stat": "Sum",
          "title": "Query Successes",
          "yAxis": {
            "left": {
              "min": 0
            }
          }
        }
      },
      {
        "type": "metric",
        "width": 12,
        "height": 6,
        "properties": {
          "metrics": [
            ["${var.metrics_namespace}", "QueryFailures"]
          ],
          "region": "${var.region}",
          "period": 300,
          "view": "timeSeries",
          "stat": "Sum",
          "title": "Query Failures",
          "yAxis": {
            "left": {
              "min": 0
            }
          }
        }
      },
      {
        "type": "metric",
        "width": 8,
        "height": 6,
        "properties": {
          "metrics": [
            ["${var.metrics_namespace}", "EstimatedCost", "DollarsPerTerabyte", "5" ]
          ],
          "region": "${var.region}",
          "period": 300,
          "view": "timeSeries",
          "stat": "Sum",
          "title": "Scan Cost",
          "yAxis": {
            "left": {
              "min": 0
            }
          }
        }
      },
      {
        "type": "metric",
        "width": 8,
        "height": 6,
        "properties": {
          "metrics": [
            ["${var.metrics_namespace}", "DataScanned" ]
          ],
          "region": "${var.region}",
          "period": 300,
          "view": "timeSeries",
          "stat": "Sum",
          "title": "Bytes Scanned",
          "yAxis": {
            "left": {
              "min": 0
            }
          }
        }
      },
      {
        "type": "metric",
        "width": 8,
        "height": 6,
        "properties": {
          "metrics": [
            ["${var.metrics_namespace}", "EngineExecutionTime" ]
          ],
          "region": "${var.region}",
          "period": 300,
          "view": "timeSeries",
          "stat": "Sum",
          "title": "Query Execution Time",
          "yAxis": {
            "left": {
              "min": 0
            }
          }
        }
      }]}
BODY
}
