resource "datadog_dashboard_json" "mysql_dbm" {
  dashboard = jsonencode({
    title       = "Exchange - MySQL DBM"
    description = "Database monitoring for Data Pipeline POC MySQL instance - queries, latency, locks, and throughput"
    layout_type = "ordered"
    tags        = ["env:demo", "team:data-platform", "domain:capital-markets"]

    widgets = [
      # Row 1: Overview
      {
        definition = {
          title       = "MySQL Overview"
          type        = "group"
          layout_type = "ordered"
          widgets = [
            {
              definition = {
                title   = "Avg Query Latency"
                type    = "query_value"
                requests = [{
                  queries = [{
                    data_source = "metrics"
                    name        = "query1"
                    query       = "avg:mysql.queries.avg_time{env:demo,domain:capital-markets}"
                  }]
                  formulas = [{ formula = "query1" }]
                }]
                autoscale   = true
                precision   = 2
                custom_unit = "s"
              }
            },
            {
              definition = {
                title   = "Queries/sec"
                type    = "query_value"
                requests = [{
                  queries = [{
                    data_source = "metrics"
                    name        = "query1"
                    query       = "avg:mysql.queries.rate{env:demo,domain:capital-markets}"
                  }]
                  formulas = [{ formula = "query1" }]
                }]
                autoscale   = true
                precision   = 1
                custom_unit = "q/s"
              }
            },
            {
              definition = {
                title   = "Active Connections"
                type    = "query_value"
                requests = [{
                  queries = [{
                    data_source = "metrics"
                    name        = "query1"
                    query       = "avg:mysql.performance.threads_running{env:demo,domain:capital-markets}"
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
      # Row 2: Query Latency Over Time
      {
        definition = {
          title = "Query Latency Over Time"
          type  = "timeseries"
          requests = [{
            queries = [{
              data_source = "metrics"
              name        = "query1"
              query       = "avg:mysql.queries.avg_time{env:demo,domain:capital-markets}"
            }]
            formulas     = [{ formula = "query1" }]
            display_type = "line"
          }]
        }
      },
      # Row 3: Top Queries (note pointing to DBM)
      {
        definition = {
          title            = "Top Queries"
          type             = "note"
          content          = <<-EOT
## Top Queries

Use **Datadog DBM → Query Samples** to see:
- The slow reconciliation JOIN (`raw_trades` ↔ `raw_settlement_instructions`)
- Full table scans on `raw_settlement_instructions` (missing index on `trade_id`)
- Heavy aggregations during `close_market_eod`

**Direct link:** Navigate to Database Monitoring → MySQL → demo-mysql in Datadog.
EOT
          background_color = "white"
          font_size        = "14"
          text_align       = "left"
          show_tick        = false
          tick_edge        = "left"
          tick_pos         = "50%"
        }
      },
      # Row 4: Throughput
      {
        definition = {
          title = "MySQL Throughput"
          type  = "timeseries"
          requests = [
            {
              queries = [{
                data_source = "metrics"
                name        = "reads"
                query       = "avg:mysql.innodb.data_reads{env:demo,domain:capital-markets}"
              }]
              formulas     = [{ formula = "reads", alias = "Reads" }]
              display_type = "line"
            },
            {
              queries = [{
                data_source = "metrics"
                name        = "writes"
                query       = "avg:mysql.innodb.data_writes{env:demo,domain:capital-markets}"
              }]
              formulas     = [{ formula = "writes", alias = "Writes" }]
              display_type = "line"
            }
          ]
        }
      },
      # Row 5: Locks
      {
        definition = {
          title = "InnoDB Row Lock Waits"
          type  = "timeseries"
          requests = [{
            queries = [{
              data_source = "metrics"
              name        = "query1"
              query       = "avg:mysql.innodb.row_lock_waits{env:demo,domain:capital-markets}"
            }]
            formulas     = [{ formula = "query1" }]
            display_type = "bars"
          }]
        }
      }
    ]
  })
}
