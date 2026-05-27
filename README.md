# B3 Control-M Observability POC

Proof of Concept for a B3-style D+1 batch pipeline orchestrated by a Control-M-like wrapper and observed with Datadog.

The goal is operational diagnosis: from one dashboard, identify whether the pipeline ran, which job failed, how long it took, which database/query was involved, and which logs belong to the same Python execution trace.

## What This Demo Shows

- Control-M-style job orchestration in Python.
- Oracle source tables (`ASTA*`) loaded into SQL Server DW tables (`ADWPM*`).
- Data Quality checks for duplicate trades, null settlement prices, zero-sum positions, and row-count parity.
- Exports to S3-compatible MinIO.
- Datadog telemetry from the job wrapper:
  - Control-M metrics via DogStatsD.
  - APM traces from Python jobs.
  - Structured logs correlated with traces through `dd.trace_id` and `dd.span_id`.
  - DBM for Oracle, SQL Server, MySQL, and PostgreSQL.
  - File-based log collection from Control-M job executions and database logs.

OpenLineage emitters exist in the repo, but the main demo path intentionally focuses on metrics, APM, logs, and DBM because those signals are more reliable for a short customer demo.

## Architecture

```text
Market data generator
        |
        v
Oracle XE source tables
  XEPDB1.DEMOPOC.ASTADRVT_TRADE_MVMT
  XEPDB1.DEMOPOC.ASTANO_FGBE_DRVT_PSTN
  XEPDB1.DEMOPOC.ASTACASH_MRKT_PSTN
        |
        v
Control-M simulator / Python wrapper
  market_data_ingest
  close_market_eod
  reconcile_d1_positions
  quality_gate_d1
  publish_d1_reports
        |
        v
SQL Server DW tables
  demopoc.dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
  demopoc.dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
  demopoc.dbo.ADWPM_POSICAO_MERCADO_A_VISTA
  demopoc.dbo.ADWPM_DQ_RESULTS
        |
        v
MinIO / S3 exports
        |
        v
Datadog Agent
  Metrics + APM + Logs + DBM
```

## Services

| Service | Purpose |
|---|---|
| `demo-controlm-sim` | Control-M-style UI, Python job wrapper, metrics, traces, logs |
| `demo-datadog-agent` | Datadog Agent with APM, DogStatsD, logs, DBM |
| `demo-oracle` | Oracle XE source database |
| `demo-sqlserver` | SQL Server DW target |
| `demo-minio` | S3-compatible export target |
| `demo-market-mock` | Market data and fault generator |
| `demo-cron` | Optional scenario rotator to keep Datadog populated |
| `demo-mysql` / `demo-postgres` | Auxiliary DBM examples |
| `demo-controlm-workbench` | Optional BMC Control-M Workbench profile |

## Prerequisites

- Podman Desktop or Podman machine.
- 8 GB+ RAM allocated to the container VM.
- `make`.
- Datadog API key.
- Datadog application key if you want to update dashboards via API or Terraform.

## Datadog Environment

Create `.env` from the template:

```bash
cp .env.example .env
```

Edit `.env`:

```bash
DD_API_KEY=<your_datadog_api_key>
DD_APP_KEY=<your_datadog_application_key>
DD_SITE=datadoghq.com
MYSQL_ROOT_PASSWORD=demopoc2026
```

`DD_API_KEY` is required for the Agent to send metrics, logs, traces, events, and DBM telemetry.

`DD_APP_KEY` is not required for ingestion, but it is recommended for this POC because the helper scripts can update dashboards and query Datadog APIs.

Never commit `.env`. It is ignored by `.gitignore`.

## Quick Start

```bash
git clone https://github.com/schawirin/data-observability-poc.git
cd data-observability-poc

cp .env.example .env
# edit .env with DD_API_KEY, DD_APP_KEY, DD_SITE

podman compose up -d --build
podman compose ps
```

Equivalent Make target:

```bash
make up
make status
```

Useful local URLs:

| UI | URL |
|---|---|
| Control-M simulator | http://localhost:5000 |
| MinIO Console | http://localhost:9001 |
| Adminer | http://localhost:8080 |
| Optional Control-M Workbench | https://localhost:8443 |

MinIO credentials:

```text
minioadmin / minioadmin
```

## Validate Datadog Agent

```bash
make dd-agent-status
```

Look for:

- `APM Agent: Running`
- `DogStatsD`
- `Logs Agent`
- DBM checks for SQL Server, Oracle, MySQL, PostgreSQL

You can also run a small smoke sample:

```bash
make dd-smoke
```

## Run the Demo

For a quick video, disable artificial job padding:

```bash
podman exec -e JOB_PADDING_FACTOR=0 demo-controlm-sim \
  python main.py run-pipeline --business-date $(date +%F)
```

For a production-like run with longer jobs, omit `JOB_PADDING_FACTOR=0`:

```bash
make run-d1 BUSINESS_DATE=$(date +%F)
```

## Run a DQ Failure

Inject all Data Quality faults in one run:

```bash
podman exec -e JOB_PADDING_FACTOR=0 demo-controlm-sim \
  python main.py run-pipeline --business-date $(date +%F) --inject-fault all
```

Expected result:

- `quality_gate_d1` runs and reports DQ failures.
- Dashboard shows:
  - `DQ Failed = 3`
  - duplicate trades
  - null settlement prices
  - zero-sum positions
- Logs contain the same `ctm_run_id`, `ctm_job`, `dd.trace_id`, and `dd.span_id` as the APM trace.

## Run Hard Failures

These scenarios make a job fail, not only the DQ gate:

```bash
make demo-hard-oracle-timeout BUSINESS_DATE=$(date +%F)
make demo-hard-gate-fail BUSINESS_DATE=$(date +%F)
make demo-hard-s3-down BUSINESS_DATE=$(date +%F)
make demo-db-blocking BUSINESS_DATE=$(date +%F)
make demo-db-deadlock BUSINESS_DATE=$(date +%F)
```

## Datadog Dashboard

Main dashboard:

```text
https://app.datadoghq.com/dashboard/6ug-s4c-mu3/b3-control-m-pipeline-operations?fromUser=true&refresh_mode=sliding&from_ts=1779881715239&to_ts=1779896115239&live=true
```

Local dashboard definition:

```text
datadog/dashboards/b3_controlm_pipeline_ops.json
```

The dashboard is scoped to:

```text
env:demo
service:controlm-sim
```

## What to Show in Datadog

### Dashboard

Show:

- Pipelines OK / FAILED.
- Jobs OK / NOT OK.
- Job duration.
- Data Quality cards.
- DQ details by check.
- Row-count parity between Oracle and SQL Server.

### APM

Search:

```text
service:controlm-sim env:demo
```

Open a trace for one job. The waterfall should show child spans for:

- `demo-oracle`
- `demo-sqlserver`
- `mock-exchange`

This demonstrates Python calling Oracle, writing to SQL Server, and publishing to S3/MinIO.

### Logs

Search:

```text
service:controlm-sim env:demo
```

For one exact run:

```text
service:controlm-sim ctm_run_id:<run_id>
```

For one exact trace:

```text
dd.trace_id:<trace_id>
```

Structured job logs include:

- `ctm_job`
- `ctm_order_date`
- `ctm_run_id`
- `status`
- `duration_seconds`
- `output_rows`
- `dd.trace_id`
- `dd.span_id`

### DBM

Use DBM to show:

- SQL Server query activity.
- Oracle source queries.
- Slow query, blocking, or deadlock scenarios.
- Query timing alongside Control-M job duration.

## Telemetry Model

### Metrics

The Python wrapper emits Control-M metrics through DogStatsD:

```text
controlm.job.duration_seconds
controlm.job.ended_ok
controlm.job.ended_not_ok
controlm.job.output_rows
controlm.job.retries
controlm.folder.duration
controlm.folder.ended_ok
controlm.folder.ended_not_ok
controlm.dq.check_failed
controlm.dq.check_ran
controlm.dq.actual_value
```

Important tags:

```text
env:demo
service:controlm-sim
version:2.0.0
ctm_job:<job_name>
ctm_order_date:<business_date>
ctm_run_id:<run_id>
ctm_status:<status>
```

### Traces

Each job creates an APM trace with spans for:

- Control-M wrapper execution.
- Oracle connection and SQL.
- SQL Server connection and SQL.
- MinIO/S3 put object.

### Logs

The Agent tails:

```text
/data/logs/controlm-sim.log
/data/logs/jobs.jsonl
/data/runs.jsonl
logs/demo-cron.log
```

Configured in:

```text
datadog/conf.d/controlm_logs.d/conf.yaml
```

Trace/log correlation uses:

```text
dd.trace_id
dd.span_id
```

### Metrics vs Trace Correlation

Metrics are aggregated time series, so they do not carry a unique trace ID per point. Datadog pivots from a metric to related traces/logs using shared tags such as:

```text
env
service
version
ctm_job
ctm_order_date
```

For exact run-level correlation, use the trace or log field:

```text
ctm_run_id:<run_id>
dd.trace_id:<trace_id>
```

## Control-M Workbench

Workbench is optional and runs behind a Compose profile:

```bash
make up-workbench
```

Deploy generated job variants:

```bash
make ctm-generate
make ctm-deploy-all
```

The generator reads:

```text
controlm/jobs/manifest.yaml
```

and produces Script, Command, Embedded Script, and Database job variants.

## Cron Sidecar

Start the cron scenario rotator:

```bash
podman compose --profile cron up -d --build demo-cron
```

It rotates success, DQ failure, hard failure, and DBM scenarios. Logs are written to:

```text
logs/demo-cron.log
```

and tailed by the Datadog Agent.

## Project Structure

```text
.
├── docker-compose.yml
├── Makefile
├── .env.example
├── datadog/
│   ├── Dockerfile
│   ├── datadog.yaml
│   ├── conf.d/
│   └── dashboards/
├── services/
│   ├── controlm-sim/
│   ├── pipeline-runner/
│   ├── market-mock/
│   └── cron-sidecar/
├── controlm/
│   ├── jobs/
│   ├── scripts/
│   └── generate.py
├── sql/
│   ├── oracle/
│   ├── sqlserver/
│   ├── postgres/
│   ├── schema/
│   └── seeds/
├── terraform/
└── docs/
```

## Common Commands

| Command | Description |
|---|---|
| `make up` | Start the full stack |
| `make down` | Stop services |
| `make clean` | Stop services and remove volumes |
| `make status` | Show container status |
| `make logs` | Tail compose logs |
| `make dd-agent-status` | Show Datadog Agent status |
| `make dd-smoke` | Emit one smoke metric/trace |
| `make generate-day` | Generate market data for a business date |
| `make run-d1` | Run the full D+1 pipeline |
| `make run-job JOB=quality_gate_d1` | Run one job |
| `make demo-hard-oracle-timeout` | Simulate Oracle timeout |
| `make demo-hard-gate-fail` | Simulate hard DQ gate failure |
| `make demo-hard-s3-down` | Simulate S3/MinIO failure |
| `make demo-db-blocking` | Simulate DB blocking |
| `make demo-db-deadlock` | Simulate DB deadlock |

## Troubleshooting

### No Data in Datadog

Check `.env`:

```bash
cat .env
```

Then check Agent status:

```bash
make dd-agent-status
```

### Logs Do Not Appear in APM Trace

Open a new trace generated after the latest code is running. Old traces do not gain new log correlation retroactively.

Check logs directly:

```text
service:controlm-sim dd.trace_id:<trace_id>
```

### Dashboard Shows No Data

Use `Past 30 Minutes` or `Past 1 Hour`, then run:

```bash
podman exec -e JOB_PADDING_FACTOR=0 demo-controlm-sim \
  python main.py run-pipeline --business-date $(date +%F)
```

### Agent Cannot Read Logs

Check the file tail integrations:

```bash
podman exec demo-datadog-agent agent status
```

Look for the `controlm_logs` section.

## Video Demo Flow

1. Explain the challenge: Control-M jobs are hard to diagnose from status alone.
2. Explain the strategy: metrics for overview, APM for execution path, logs for evidence, DBM for database diagnosis.
3. Clone the repo and configure `.env`.
4. Start with `podman compose up -d --build`.
5. Run a happy path.
6. Run a DQ failure.
7. Show Datadog dashboard.
8. Open a trace and show child spans for Oracle, SQL Server, and S3.
9. Open logs from the trace and show `dd.trace_id`.
10. Open DBM for SQL Server/Oracle query evidence.

## License

Proof of Concept for demonstration purposes.
