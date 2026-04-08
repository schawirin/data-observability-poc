resource "datadog_dashboard_json" "pipeline_d1" {
  dashboard = jsonencode({
    title       = "Exchange - Pipeline D+1"
    description = "Monitoring the market D+1 pipeline: job status, duration, SLA, and errors"
    layout_type = "ordered"
    tags        = ["env:demo", "team:data-platform", "domain:capital-markets"]

    widgets = [
      # Row 1: Summary
      {
        definition = {
          title = "Pipeline Overview"
          type  = "group"
          layout_type = "ordered"
          widgets = [
            {
              definition = {
                title   = "Job Executions (Last 24h)"
                type    = "query_value"
                requests = [{
                  queries = [{
                    data_source = "logs"
                    name        = "query1"
                    search      = { query = "service:controlm-sim @job.status:* env:demo" }
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
                title   = "Failed Jobs (Last 24h)"
                type    = "query_value"
                requests = [{
                  queries = [{
                    data_source = "logs"
                    name        = "query1"
                    search      = { query = "service:controlm-sim @job.status:failed env:demo" }
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
                title   = "SLA Misses (Last 24h)"
                type    = "query_value"
                requests = [{
                  queries = [{
                    data_source = "logs"
                    name        = "query1"
                    search      = { query = "service:controlm-sim @sla_miss:true env:demo" }
                    indexes     = ["*"]
                    compute     = { aggregation = "count" }
                  }]
                  formulas = [{ formula = "query1" }]
                }]
                autoscale = true
                precision = 0
              }
            }
          ]
        }
      },
      # Row 2: Job Status Timeline
      {
        definition = {
          title = "Job Status by Execution"
          type  = "log_stream"
          query = "service:controlm-sim @job.status:* env:demo"
          columns = ["@job.name", "@job.status", "@duration_seconds", "@business_date", "@sla_miss", "@retries"]
          indexes = ["*"]
          message_display = "inline"
          sort = { column = "time", order = "desc" }
        }
      },
      # Row 3: Duration over time
      {
        definition = {
          title = "Job Duration by Name"
          type  = "timeseries"
          requests = [{
            queries = [{
              data_source = "logs"
              name        = "query1"
              search      = { query = "service:controlm-sim @job.status:success env:demo" }
              indexes     = ["*"]
              compute     = { aggregation = "avg", metric = "@duration_seconds" }
              group_by = [{
                facet = "@job.name"
                limit = 10
                sort  = { aggregation = "avg", metric = "@duration_seconds", order = "desc" }
              }]
            }]
            formulas     = [{ formula = "query1" }]
            display_type = "bars"
          }]
        }
      },
      # Row 4: Errors
      {
        definition = {
          title = "Recent Errors"
          type  = "log_stream"
          query = "service:controlm-sim status:error env:demo"
          columns = ["@job.name", "@error", "@business_date", "@run_id"]
          indexes = ["*"]
          message_display = "expanded-md"
          sort = { column = "time", order = "desc" }
        }
      }
    ]
  })
}
