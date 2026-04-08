resource "datadog_dashboard_json" "data_quality" {
  dashboard = jsonencode({
    title       = "Exchange - Data Lineage and Quality"
    description = "Data quality gate results and lineage tracking for D+1 pipeline"
    layout_type = "ordered"
    tags        = ["env:demo", "team:data-platform", "domain:capital-markets"]

    widgets = [
      # Row 1: Quality Gate Summary
      {
        definition = {
          title       = "Quality Gate Summary"
          type        = "group"
          layout_type = "ordered"
          widgets = [
            {
              definition = {
                title   = "Gate Status (Last Run)"
                type    = "query_value"
                requests = [{
                  queries = [{
                    data_source = "logs"
                    name        = "query1"
                    search      = { query = "service:controlm-sim @job.name:quality_gate_d1 @gate_status:FAIL env:demo" }
                    indexes     = ["*"]
                    compute     = { aggregation = "count" }
                  }]
                  formulas = [{ formula = "query1" }]
                }]
                autoscale = true
                precision = 0
              }
            },
            {
              definition = {
                title   = "Critical Failures"
                type    = "query_value"
                requests = [{
                  queries = [{
                    data_source = "logs"
                    name        = "query1"
                    search      = { query = "service:controlm-sim @job.name:quality_gate_d1 env:demo" }
                    indexes     = ["*"]
                    compute     = { aggregation = "avg", metric = "@critical_failures" }
                  }]
                  formulas = [{ formula = "query1" }]
                }]
                autoscale = true
                precision = 0
              }
            },
            {
              definition = {
                title   = "Checks Passed Rate"
                type    = "query_value"
                requests = [{
                  queries = [{
                    data_source = "logs"
                    name        = "total"
                    search      = { query = "service:controlm-sim @job.name:quality_gate_d1 env:demo" }
                    indexes     = ["*"]
                    compute     = { aggregation = "avg", metric = "@checks_run" }
                  },
                  {
                    data_source = "logs"
                    name        = "failed"
                    search      = { query = "service:controlm-sim @job.name:quality_gate_d1 env:demo" }
                    indexes     = ["*"]
                    compute     = { aggregation = "avg", metric = "@checks_failed" }
                  }]
                  formulas = [{ formula = "((total - failed) / total) * 100" }]
                }]
                autoscale = true
                precision = 1
                custom_unit = "%"
              }
            }
          ]
        }
      },
      # Row 2: Quality Results Detail
      {
        definition = {
          title = "Quality Check Results (Last Runs)"
          type  = "log_stream"
          query = "service:pipeline-runner @check_name:* env:demo"
          columns = ["@check_name", "@check_type", "@target_table", "@severity", "@passed", "@actual_value", "@details"]
          indexes = ["*"]
          message_display = "inline"
          sort = { column = "time", order = "desc" }
        }
      },
      # Row 3: Quality Trend
      {
        definition = {
          title = "Quality Failures Over Time"
          type  = "timeseries"
          requests = [{
            queries = [{
              data_source = "logs"
              name        = "query1"
              search      = { query = "service:pipeline-runner @check_name:* @passed:false env:demo" }
              indexes     = ["*"]
              compute     = { aggregation = "count" }
              group_by = [{
                facet = "@check_type"
                limit = 10
                sort  = { aggregation = "count", order = "desc" }
              }]
            }]
            formulas     = [{ formula = "query1" }]
            display_type = "bars"
          }]
        }
      },
      # Row 4: Affected Datasets
      {
        definition = {
          title            = "Datasets Affected by Quality Failures"
          type             = "note"
          content          = <<-EOT
## Data Lineage Path

```
raw.trades → close_market_eod → raw.close_prices → curated.market_close_snapshot
raw.trades → reconcile_d1_positions → staging.settlement_recon → quality_gate_d1 → publish_d1_reports → d1_settlement_report.parquet
raw.participant_positions → reconcile_d1_positions → staging.position_recon → quality_gate_d1 → publish_d1_reports → participant_exposure_report.csv
```

**Use DJM Lineage view** to see the live graph and trace impact of quality failures to downstream datasets.
EOT
          background_color = "yellow"
          font_size        = "14"
          text_align       = "left"
          show_tick        = false
          tick_edge        = "left"
          tick_pos         = "50%"
        }
      }
    ]
  })
}
