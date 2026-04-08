# Data Observability POC

Proof of Concept demonstrating **end-to-end data observability** for capital markets pipelines using Datadog.

Simulates a realistic exchange-style batch processing flow — from market close through D+1 reconciliation — with full visibility into **Data Jobs Monitoring (DJM)**, **Data Lineage**, **Database Monitoring (DBM)**, and **Data Quality** checks.

---

## Architecture

```
                    ┌─────────────┐
                    │  Oracle XE   │  Source (derivatives, positions)
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ Control-M   │  Job orchestration (simulated)
                    │   Sim       │  OpenLineage events → Datadog
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │  MySQL   │ │ Postgres │ │SQL Server│
        │  (raw →  │ │  (dbt    │ │  (DW     │
        │ staging →│ │ transforms│ │ target)  │
        │ curated) │ │          │ │          │
        └──────────┘ └──────────┘ └──────────┘
              │
              ▼
        ┌──────────┐     ┌───────────────┐
        │  MinIO   │     │ Datadog Agent │
        │ (exports)│     │ APM · DBM ·   │
        └──────────┘     │ Logs · DJM    │
                         └───────────────┘
```

### Services

| Service | Description |
|---|---|
| **oracle** | Oracle XE 21 — source tables (derivatives trades, positions) |
| **sqlserver** | SQL Server 2022 — DW destination |
| **mysql** | MySQL 8.0 — raw/staging/curated schemas + DBM |
| **postgres** | PostgreSQL 16 — dbt transformations |
| **minio** | S3-compatible storage — CSV/Parquet exports |
| **datadog-agent** | Agent 7 — APM, DBM, Logs, OpenLineage proxy |
| **controlm-sim** | Control-M simulator — DAG orchestration via OpenLineage |
| **pipeline-runner** | ETL jobs — reconciliation, quality gates, publishing |
| **market-mock** | Market data generator — realistic trades, prices, positions |
| **dbt-runner** | dbt transformations on Postgres |
| **controlm-workbench** | BMC Control-M Workbench (optional) |
| **adminer** | DB admin UI |

---

## Quick Start

### Prerequisites

- Docker Desktop (4GB+ RAM recommended)
- Datadog API Key ([get one here](https://app.datadoghq.com/organization-settings/api-keys))
- `make`

### Setup

```bash
# 1. Clone and configure
git clone https://github.com/schawirin/data-observability-poc.git
cd data-observability-poc

# 2. Create .env with your Datadog API key
cp .env.example .env
# Edit .env and set DD_API_KEY=<your-key>

# 3. Start the full stack
make up

# 4. Seed reference data
make seed
```

### Verify

- **Adminer**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Control-M Sim**: http://localhost:5000
- **Control-M Workbench**: https://localhost:8443 (workbench/workbench)

---

## Demo Scenarios

### Scenario A: Happy Path

Full pipeline execution — all jobs succeed, data flows end-to-end.

```bash
make demo-happy BUSINESS_DATE=2026-03-16
```

**What to see in Datadog:**
- **DJM** → 4 jobs in sequence: `close_market_eod` → `reconcile_d1_positions` → `quality_gate_d1` → `publish_d1_reports`
- **Lineage** → `raw_trades` → staging → curated → `d1_settlement_report.parquet`
- **DBM** → Normal query latency on MySQL

---

### Scenario B: Data Quality Failure (Duplicate Trades)

Injects duplicate `trade_id` records, quality gate catches the issue.

```bash
make demo-fail BUSINESS_DATE=2026-03-17
```

**What to see in Datadog:**
- **DJM** → `quality_gate_d1` = FAILED, `publish_d1_reports` = BLOCKED
- **Monitor** → "Quality Gate Critical" alert fires
- **Lineage** → Trace the issue from `raw_trades` through to the missing export file

---

### Scenario C: Database Bottleneck

Injects a slow reconciliation query (full table scan, no index).

```bash
make demo-slow BUSINESS_DATE=2026-03-18
```

**What to see in Datadog:**
- **DBM** → Slow query detected with explain plan
- **DJM** → `reconcile_d1_positions` with elevated duration
- **Correlation** → Job delay ↔ slow query ↔ downstream dataset delay

---

### Advanced Scenarios (Oracle → SQL Server)

These scenarios use production-like table names and data patterns:

| Scenario | Command | Tables |
|---|---|---|
| **Caso 1**: Duplicate derivatives trades | `make demo-caso1` | `ASTADRVT_TRADE_MVMT` → `ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO` |
| **Caso 2**: Null settlement price | `make demo-caso2` | `ASTANO_FGBE_DRVT_PSTN` → `ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL` |
| **Caso 3**: Zero-sum positions | `make demo-caso3` | `ASTACASH_MRKT_PSTN` → `ADWPM_POSICAO_MERCADO_A_VISTA` |
| **Overflow**: Decimal precision exceeded | `make demo-overflow` | `ASTADRVT_TRADE_MVMT` |

---

### Full Demo (all scenarios sequentially)

```bash
make demo-full
```

---

## Pipeline Jobs

The D+1 pipeline consists of 4 chained jobs:

```
close_market_eod → reconcile_d1_positions → quality_gate_d1 → publish_d1_reports
```

| Job | Input | Output | Purpose |
|---|---|---|---|
| `close_market_eod` | `raw.trades`, `raw.orders` | `raw.close_prices`, `curated.market_close_snapshot` | End-of-day market consolidation |
| `reconcile_d1_positions` | `raw.trades`, `raw.participant_positions`, `raw.settlement_instructions` | `staging.settlement_recon`, `staging.position_recon` | D+1 reconciliation (heavy SQL) |
| `quality_gate_d1` | `staging.*` | `ops.quality_results` | Completeness, uniqueness, consistency, reasonability |
| `publish_d1_reports` | Approved curated data | Parquet/CSV in MinIO | Final export for downstream consumers |

---

## Data Quality Checks

| Rule | Description |
|---|---|
| **Completeness** | `closing_price` must not be NULL |
| **Uniqueness** | `trade_id` must be unique per `business_date` |
| **Consistency** | Every settlement instruction must reference a valid trade |
| **Reasonability** | `price > 0`, `quantity > 0`, volume within expected range |
| **Timeliness** | D+1 pipeline must complete before SLA deadline |

---

## Fault Injection

Inject specific failures to demonstrate observability:

```bash
make inject-duplicates          # Duplicate trade IDs
make inject-null-prices         # NULL closing prices
make inject-slow-query          # Slow reconciliation query (DBM)
make inject-caso1               # Duplicate derivatives (Oracle tables)
make inject-caso2               # Null settlement price (Oracle tables)
make inject-caso3               # Zero-sum positions (Oracle tables)
make inject-overflow            # Decimal overflow
make inject-row-count-diff      # Source/DW row count mismatch
```

---

## Observability Stack

### Data Jobs Monitoring (DJM)
- Job status, duration, retries, SLA tracking
- Pipeline-level and job-level views
- Emitted via **OpenLineage** protocol through the Datadog Agent

### Data Lineage
- Dataset-to-job and job-to-dataset tracing
- Impact analysis: trace a bad record from source to affected exports
- 3-layer visibility: raw → staging → curated/export

### Database Monitoring (DBM)
- Query performance, explain plans, wait events
- Top queries, lock analysis, throughput
- Correlation with pipeline job execution

### Monitors (Terraform)
- Job failure alerts
- SLA miss detection
- Quality gate threshold breaches
- Slow query detection

---

## Terraform (IaC)

Dashboards and monitors are defined as code in [`terraform/`](terraform/):

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Set dd_api_key and dd_app_key
terraform init && terraform apply
```

| Resource | File |
|---|---|
| Pipeline Dashboard | `dashboard_pipeline.tf` |
| Data Quality Dashboard | `dashboard_quality.tf` |
| DBM Dashboard | `dashboard_dbm.tf` |
| Monitors | `monitors.tf` |

---

## Project Structure

```
exchange/
├── docker-compose.yml          # Full stack definition
├── Makefile                    # All demo commands
├── .env.example                # Environment template
├── datadog/
│   ├── datadog.yaml            # Agent config (OpenLineage proxy)
│   └── conf.d/                 # Integration configs (MySQL, Oracle, SQL Server)
├── services/
│   ├── controlm-sim/           # Control-M simulator + OpenLineage emitter
│   ├── pipeline-runner/        # ETL jobs + quality gate
│   ├── market-mock/            # Market data generator + fault injection
│   └── dbt-project/            # dbt transformations (Postgres)
├── sql/
│   ├── oracle/                 # Oracle source schema
│   ├── sqlserver/              # SQL Server DW schema
│   ├── postgres/               # PostgreSQL init
│   ├── schema/                 # MySQL schemas (raw, staging, curated, ops)
│   └── seeds/                  # Reference data
├── controlm/                   # Control-M Workbench setup scripts
├── terraform/                  # Datadog dashboards & monitors as code
└── docs/
    └── demo-script.md          # Step-by-step demo guide
```

---

## Makefile Reference

| Command | Description |
|---|---|
| `make up` | Start the full stack |
| `make down` | Stop all services |
| `make clean` | Stop + remove volumes |
| `make status` | Show container status |
| `make logs` | Tail all logs |
| `make seed` | Load reference data |
| `make generate-day` | Generate market data for a date |
| `make demo-happy` | Run happy path scenario |
| `make demo-fail` | Run duplicate trade failure |
| `make demo-slow` | Run slow query scenario |
| `make demo-caso1` | Duplicate derivatives (real tables) |
| `make demo-caso2` | Null settlement price (real tables) |
| `make demo-caso3` | Zero-sum positions (real tables) |
| `make demo-full` | Run all scenarios sequentially |

---

## Tags

All telemetry is tagged consistently:

```
env:demo
team:data-platform
domain:capital-markets
pipeline:market-d1
orchestrator:controlm-sim
business_date:YYYY-MM-DD
stage:raw|staging|curated
```

---

## License

This project is a Proof of Concept for demonstration purposes.
