# Monitor 1: Job Failed
resource "datadog_monitor" "job_failed" {
  name    = "[Data Pipeline POC] Pipeline Job Failed"
  type    = "log alert"
  message = <<-EOT
    ## Pipeline Job Failed

    A job in the market-d1 pipeline has failed.

    **Job:** {{job_name.name}}
    **Business Date:** {{business_date.name}}
    **Run ID:** {{run_id.name}}

    Check DJM for details and downstream impact.

    ${var.notification_channel}
  EOT

  query = "logs(\"service:controlm-sim status:error @job.status:failed env:demo\").index(\"*\").rollup(\"count\").by(\"@job.name\").last(\"5m\") > 0"

  monitor_thresholds {
    critical = 0
  }

  tags = ["env:demo", "team:data-platform", "domain:capital-markets", "pipeline:market-d1"]

  priority = 2
}

# Monitor 2: Job SLA Miss
resource "datadog_monitor" "sla_miss" {
  name    = "[Data Pipeline POC] Pipeline Job SLA Miss"
  type    = "log alert"
  message = <<-EOT
    ## Pipeline Job SLA Miss

    A job exceeded its SLA window.

    **Job:** {{job_name.name}}
    **Business Date:** {{business_date.name}}

    This may delay downstream reports and file exports.

    ${var.notification_channel}
  EOT

  query = "logs(\"service:controlm-sim @sla_miss:true env:demo\").index(\"*\").rollup(\"count\").by(\"@job.name\").last(\"10m\") > 0"

  monitor_thresholds {
    critical = 0
  }

  tags = ["env:demo", "team:data-platform", "domain:capital-markets", "pipeline:market-d1"]

  priority = 2
}

# Monitor 3: Quality Gate Critical
resource "datadog_monitor" "quality_gate_critical" {
  name    = "[Data Pipeline POC] Data Quality Gate - Critical Failure"
  type    = "log alert"
  message = <<-EOT
    ## Data Quality Gate Failed

    Critical quality checks have failed for the D+1 pipeline.

    **Business Date:** {{business_date.name}}
    **Failed Checks:** {{checks_failed.name}}

    Downstream reports will NOT be published until quality issues are resolved.
    Check DJM lineage to see impacted datasets.

    ${var.notification_channel}
  EOT

  query = "logs(\"service:controlm-sim @gate_status:FAIL @job.name:quality_gate_d1 env:demo\").index(\"*\").rollup(\"count\").last(\"10m\") > 0"

  monitor_thresholds {
    critical = 0
  }

  tags = ["env:demo", "team:data-platform", "domain:capital-markets", "pipeline:market-d1", "quality:critical"]

  priority = 1
}

# Monitor 4: Slow Reconciliation Query
resource "datadog_monitor" "slow_query" {
  name    = "[Data Pipeline POC] MySQL - Slow Reconciliation Query"
  type    = "query alert"
  message = <<-EOT
    ## Slow Query Detected on MySQL

    The reconciliation query p95 latency has exceeded the threshold.

    This typically correlates with job delays in the D+1 pipeline.
    Check DBM for query details and execution plan.

    ${var.notification_channel}
  EOT

  query = "avg(last_5m):avg:mysql.queries.avg_time{env:demo,domain:capital-markets} > 5"

  monitor_thresholds {
    critical = 5
    warning  = 2
  }

  tags = ["env:demo", "team:data-platform", "domain:capital-markets", "dbm:mysql"]

  priority = 3
}

# Monitor 5: No D+1 Export
resource "datadog_monitor" "no_export" {
  name    = "[Data Pipeline POC] No D+1 Export File Generated"
  type    = "log alert"
  message = <<-EOT
    ## No D+1 Export

    The daily settlement report and exposure files have not been generated.

    This means downstream consumers (risk, compliance, back-office) have no data.

    Check the pipeline status in DJM and quality gate results.

    ${var.notification_channel}
  EOT

  query = "logs(\"service:controlm-sim @job.name:publish_d1_reports @job.status:success env:demo\").index(\"*\").rollup(\"count\").last(\"30m\") < 1"

  monitor_thresholds {
    critical = 1
  }

  tags = ["env:demo", "team:data-platform", "domain:capital-markets", "pipeline:market-d1"]

  priority = 1
}
