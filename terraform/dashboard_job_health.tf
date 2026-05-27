resource "datadog_dashboard_json" "job_health" {
  dashboard = jsonencode({
    title       = "Exchange — Job Health (Custom View)"
    description = "Correlates Control-M job execution with DBM activity, locks/deadlocks, logs and SLO compliance. One panel per signal, filtered by job and business date."
    layout_type = "ordered"
    tags        = ["team:data-platform"]

    template_variables = [
      {
        name    = "env"
        prefix  = "env"
        default = "demo"
      },
      {
        name    = "ctm_job"
        prefix  = "ctm_job"
        default = "*"
      },
      {
        name    = "business_date"
        prefix  = "business_date"
        default = "*"
      }
    ]

    widgets = [
      # ════════════════ 1. STATUS + DBM TOP QUERIES ════════════════
      {
        definition = {
          title       = "1 · Job Execution Status + DBM Top Queries"
          type        = "group"
          layout_type = "ordered"
          widgets = [
            {
              definition = {
                title = "Job runs — OK vs FAIL by job"
                type  = "timeseries"
                requests = [
                  { q = "sum:controlm.job.ended_ok.count{$env,$ctm_job} by {ctm_job}.as_count()",     display_type = "bars", style = { palette = "green" } },
                  { q = "sum:controlm.job.ended_not_ok.count{$env,$ctm_job} by {ctm_job}.as_count()", display_type = "bars", style = { palette = "red"   } }
                ]
              }
            },
            {
              definition = {
                title = "Job duration p95 (last 24h)"
                type  = "timeseries"
                requests = [
                  { q = "avg:controlm.job.duration_seconds{$env,$ctm_job} by {ctm_job}", display_type = "line", style = { palette = "cool" } }
                ]
              }
            },
            {
              definition = {
                title = "Retries by job"
                type  = "toplist"
                requests = [
                  { q = "top(sum:controlm.job.retries{$env,$ctm_job} by {ctm_job}.as_count(), 10, 'sum', 'desc')" }
                ]
              }
            },
            {
              definition = {
                title = "DBM top queries — Oracle (elapsed time)"
                type  = "toplist"
                requests = [
                  { q = "top(sum:oracle.query.elapsed_time{$env} by {query_signature}.as_count(), 10, 'sum', 'desc')" }
                ]
              }
            },
            {
              definition = {
                title = "DBM top queries — SQL Server (elapsed time)"
                type  = "toplist"
                requests = [
                  { q = "top(sum:sqlserver.query.total_elapsed_time{$env} by {query_signature}.as_count(), 10, 'sum', 'desc')" }
                ]
              }
            },
            {
              definition = {
                title = "DBM top queries — MySQL (latency)"
                type  = "toplist"
                requests = [
                  { q = "top(avg:mysql.queries.time{$env} by {query_signature}, 10, 'mean', 'desc')" }
                ]
              }
            },
            {
              definition = {
                title = "DBM top queries — Postgres (latency)"
                type  = "toplist"
                requests = [
                  { q = "top(avg:postgresql.queries.time{$env} by {query_signature}, 10, 'mean', 'desc')" }
                ]
              }
            }
          ]
        }
      },

      # ════════════════ 2. LOCKS / BLOCKING / DEADLOCKS ════════════════
      {
        definition = {
          title       = "2 · Locks · Blocking · Deadlocks"
          type        = "group"
          layout_type = "ordered"
          widgets = [
            {
              definition = {
                title = "SQL Server — locks waits/sec"
                type  = "timeseries"
                requests = [
                  { q = "avg:sqlserver.access.page_splits{$env}.as_rate()",                       display_type = "line", style = { palette = "warm" } },
                  { q = "avg:sqlserver.stats.lock_waits{$env}.as_count()",                       display_type = "line", style = { palette = "orange" } },
                  { q = "avg:sqlserver.stats.deadlocks{$env}.as_count()",                        display_type = "bars", style = { palette = "red" } }
                ]
              }
            },
            {
              definition = {
                title = "Oracle — wait events (top 5)"
                type  = "toplist"
                requests = [
                  { q = "top(avg:oracle.wait_time{$env} by {event}, 5, 'mean', 'desc')" }
                ]
              }
            },
            {
              definition = {
                title = "MySQL — locks/waits"
                type  = "timeseries"
                requests = [
                  { q = "avg:mysql.innodb.row_lock_waits{$env}.as_rate()",         display_type = "line", style = { palette = "warm" } },
                  { q = "avg:mysql.innodb.row_lock_time{$env}.as_rate()",          display_type = "line", style = { palette = "orange" } },
                  { q = "avg:mysql.innodb.row_lock_current_waits{$env}",           display_type = "bars", style = { palette = "red" } }
                ]
              }
            },
            {
              definition = {
                title = "Postgres — locks (count by mode)"
                type  = "timeseries"
                requests = [{
                  q = "sum:postgresql.locks{$env} by {mode}.as_count()"
                  display_type = "bars"
                  style = { palette = "purple" }
                }]
              }
            }
          ]
        }
      },

      # ════════════════ 3. LOGS CORRELATED BY JOB ════════════════
      {
        definition = {
          title       = "3 · Logs Correlated by Job"
          type        = "group"
          layout_type = "ordered"
          widgets = [
            {
              definition = {
                title       = "Live log stream — ctm_job:$ctm_job"
                type        = "log_stream"
                indexes     = ["*"]
                query       = "$env $ctm_job"
                sort        = { column = "time", order = "desc" }
                columns     = ["host", "service", "ctm_job", "status"]
                message_display = "expanded-md"
                show_date_column   = true
                show_message_column = true
              }
            },
            {
              definition = {
                title = "Log status breakdown — by job and status"
                type  = "toplist"
                requests = [{
                  response_format = "scalar"
                  queries = [{
                    name        = "cnt"
                    data_source = "logs"
                    compute     = { aggregation = "count" }
                    search      = { query = "$env $ctm_job" }
                    group_by    = [
                      { facet = "@ctm_job", limit = 10, sort = { aggregation = "count", order = "desc" } }
                    ]
                    indexes     = ["*"]
                  }]
                  formulas = [{ formula = "cnt" }]
                }]
              }
            }
          ]
        }
      },

      # ════════════════ 4. ROW COUNT (SRC vs TGT) + SLO ════════════════
      {
        definition = {
          title       = "4 · Row Count Reconciliation (Source vs DW) + SLO"
          type        = "group"
          layout_type = "ordered"
          widgets = [
            {
              definition = {
                title = "Oracle source vs SQL Server DW — derivatives"
                type  = "timeseries"
                requests = [
                  { q = "avg:oracle.exchange.oracle.trade_mvmt_source_count{$env}",       display_type = "line", style = { palette = "cool" } },
                  { q = "avg:exchange.dw.trade_movements_loaded{$env}",                   display_type = "line", style = { palette = "warm" } }
                ]
                yaxis = { include_zero = true }
                markers = [
                  { value = "y = 0", display_type = "info dashed" }
                ]
              }
            },
            {
              definition = {
                title = "Oracle source vs SQL Server DW — positions"
                type  = "timeseries"
                requests = [
                  { q = "avg:oracle.exchange.oracle.derivative_position_source_count{$env}",  display_type = "line", style = { palette = "cool" } },
                  { q = "avg:exchange.dw.deriv_positions_loaded{$env}",                       display_type = "line", style = { palette = "warm" } },
                  { q = "avg:oracle.exchange.oracle.cash_position_source_count{$env}",        display_type = "line", style = { palette = "purple" } },
                  { q = "avg:exchange.dw.cash_positions_loaded{$env}",                        display_type = "line", style = { palette = "orange" } }
                ]
              }
            },
            {
              definition = {
                title = "SLA misses by job (last 24h)"
                type  = "query_value"
                requests = [{
                  q          = "sum:controlm.job.sla_miss.count{$env,$ctm_job}.as_count()"
                  aggregator = "sum"
                  conditional_formats = [
                    { comparator = "=", value = 0, palette = "white_on_green" },
                    { comparator = ">", value = 0, palette = "white_on_red" }
                  ]
                }]
                autoscale = true
                precision = 0
              }
            },
            {
              definition = {
                title = "Pipeline final job — publish_d1_reports completions"
                type  = "query_value"
                requests = [{
                  q          = "sum:controlm.job.ended_ok.count{$env,ctm_job:publish_d1_reports}.as_count()"
                  aggregator = "sum"
                  conditional_formats = [
                    { comparator = ">", value = 0, palette = "white_on_green" },
                    { comparator = "=", value = 0, palette = "white_on_yellow" }
                  ]
                }]
                autoscale = true
                precision = 0
              }
            },
            {
              definition = {
                title = "DQ Gate — checks passed vs failed"
                type  = "timeseries"
                requests = [
                  { q = "sum:exchange.dq.checks_passed{$env}.as_count()", display_type = "bars", style = { palette = "green" } },
                  { q = "sum:exchange.dq.checks_failed{$env}.as_count()", display_type = "bars", style = { palette = "red" } }
                ]
              }
            }
          ]
        }
      }
    ]
  })
}

output "dashboard_job_health_url" {
  description = "URL of the Job Health dashboard"
  value       = "https://app.datadoghq.com/dashboard/${datadog_dashboard_json.job_health.id}"
}
