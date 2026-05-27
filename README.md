# POC B3 de Observabilidade para Control-M

Prova de conceito de uma pipeline batch D+1 no estilo B3, orquestrada por um wrapper similar ao Control-M e observada com Datadog.

O objetivo da demo é diagnóstico operacional: a partir de um dashboard, identificar se a pipeline rodou, qual job falhou, quanto tempo levou, qual banco/query esteve envolvido e quais logs pertencem ao mesmo trace Python.

## O Que Esta Demo Mostra

- Orquestração de jobs estilo Control-M em Python.
- Tabelas fonte Oracle (`ASTA*`) carregadas em tabelas DW SQL Server (`ADWPM*`).
- Validações de Data Quality para trades duplicados, preço de liquidação nulo, posições com soma zero e paridade de contagem de linhas.
- Exportação para MinIO compatível com S3.
- Telemetria Datadog gerada pelo wrapper dos jobs:
  - Métricas Control-M via DogStatsD.
  - Traces APM dos jobs Python.
  - Logs estruturados correlacionados com traces via `dd.trace_id` e `dd.span_id`.
  - DBM para Oracle, SQL Server, MySQL e PostgreSQL.
  - Coleta de logs em arquivo das execuções Control-M e dos bancos.

Os emissores OpenLineage continuam no repositório, mas o caminho principal da demo foca em métricas, APM, logs e DBM porque estes sinais são mais previsíveis para uma demonstração curta.

## Arquitetura

```text
Gerador de dados de mercado
        |
        v
Tabelas fonte Oracle XE
  XEPDB1.DEMOPOC.ASTADRVT_TRADE_MVMT
  XEPDB1.DEMOPOC.ASTANO_FGBE_DRVT_PSTN
  XEPDB1.DEMOPOC.ASTACASH_MRKT_PSTN
        |
        v
Simulador Control-M / wrapper Python
  market_data_ingest
  close_market_eod
  reconcile_d1_positions
  quality_gate_d1
  publish_d1_reports
        |
        v
Tabelas DW SQL Server
  demopoc.dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
  demopoc.dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
  demopoc.dbo.ADWPM_POSICAO_MERCADO_A_VISTA
  demopoc.dbo.ADWPM_DQ_RESULTS
        |
        v
Exports MinIO / S3
        |
        v
Datadog Agent
  Métricas + APM + Logs + DBM
```

## Jobs da Pipeline

| Job | O Que Faz | Principal Sinal na Demo |
|---|---|---|
| `market_data_ingest` | Gera e carrega os dados fonte de fechamento de mercado no Oracle XE. Representa a ingestão upstream antes do batch D+1. | Mostra o primeiro job Python, volume de dados fonte e início da correlação entre trace e logs. |
| `close_market_eod` | Lê movimentos de trades derivativos no Oracle (`ASTADRVT_TRADE_MVMT`) e carrega a tabela DW SQL Server (`ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO`). | Mostra duração do ETL Oracle para SQL Server, contagem de linhas, retries e tratamento de falha. |
| `reconcile_d1_positions` | Reconcilia posições D+1 derivativas e cash do Oracle (`ASTANO_*`, `ASTACASH_*`) para tabelas de posição no SQL Server. | Mostra a etapa mais pesada do batch, maior duração, queries no DBM e cenários de SQL lento ou lock. |
| `quality_gate_d1` | Executa checagens de Data Quality nos outputs DW: duplicidade, nulos, soma zero, paridade de linhas e overflow. | Mostra métricas de DQ, `DQ Failed`, falhas por tipo de check e comportamento de gate pass/fail. |
| `publish_d1_reports` | Publica os outputs curados do SQL Server no MinIO/S3 como arquivos de relatório para consumidores downstream. | Mostra a etapa final de exportação, falhas de S3, linhas de output, logs e status de conclusão. |

## Serviços

| Serviço | Papel |
|---|---|
| `demo-controlm-sim` | UI estilo Control-M, wrapper Python, métricas, traces e logs |
| `demo-datadog-agent` | Datadog Agent com APM, DogStatsD, logs e DBM |
| `demo-oracle` | Banco fonte Oracle XE |
| `demo-sqlserver` | Banco destino SQL Server DW |
| `demo-minio` | Destino de exportação compatível com S3 |
| `demo-market-mock` | Gerador de dados de mercado e injeção de falhas |
| `demo-cron` | Rotacionador opcional de cenários para manter o Datadog populado |
| `demo-mysql` / `demo-postgres` | Exemplos auxiliares para DBM |
| `demo-controlm-workbench` | Perfil opcional do BMC Control-M Workbench |

## Pré-requisitos

- Podman Desktop ou Podman machine.
- 8 GB ou mais de RAM alocados para a VM de containers.
- `make`.
- API key do Datadog.
- Application key do Datadog se quiser atualizar dashboards via API ou Terraform.

## Ambiente Datadog

Crie o arquivo `.env` a partir do template:

```bash
cp .env.example .env
```

Edite o `.env`:

```bash
DD_API_KEY=<your_datadog_api_key>
DD_APP_KEY=<your_datadog_application_key>
DD_SITE=datadoghq.com
MYSQL_ROOT_PASSWORD=demopoc2026
```

`DD_API_KEY` é obrigatória para o Agent enviar métricas, logs, traces, eventos e telemetria DBM.

`DD_APP_KEY` não é obrigatória para ingestão, mas é recomendada nesta POC porque os scripts auxiliares podem atualizar dashboards e consultar APIs do Datadog.

Nunca commite `.env`. O arquivo está ignorado no `.gitignore`.

## Subida Rápida

```bash
git clone https://github.com/schawirin/data-observability-poc.git
cd data-observability-poc

cp .env.example .env
# edite .env com DD_API_KEY, DD_APP_KEY e DD_SITE

podman compose up -d --build
podman compose ps
```

Target equivalente no Makefile:

```bash
make up
make status
```

URLs locais úteis:

| UI | URL |
|---|---|
| Simulador Control-M | http://localhost:5000 |
| Console MinIO | http://localhost:9001 |
| Adminer | http://localhost:8080 |
| Control-M Workbench opcional | https://localhost:8443 |

Credenciais do MinIO:

```text
minioadmin / minioadmin
```

## Validar o Datadog Agent

```bash
make dd-agent-status
```

Procure por:

- `APM Agent: Running`
- `DogStatsD`
- `Logs Agent`
- Checks DBM para SQL Server, Oracle, MySQL e PostgreSQL

Também é possível enviar uma amostra simples:

```bash
make dd-smoke
```

## Rodar a Demo

Para gravar um vídeo curto, desative o padding artificial dos jobs:

```bash
podman exec -e JOB_PADDING_FACTOR=0 demo-controlm-sim \
  python main.py run-pipeline --business-date $(date +%F)
```

Para uma execução mais parecida com produção, com jobs mais longos, omita `JOB_PADDING_FACTOR=0`:

```bash
make run-d1 BUSINESS_DATE=$(date +%F)
```

## Rodar uma Falha de Data Quality

Injete todas as falhas de Data Quality em uma única execução:

```bash
podman exec -e JOB_PADDING_FACTOR=0 demo-controlm-sim \
  python main.py run-pipeline --business-date $(date +%F) --inject-fault all
```

Resultado esperado:

- `quality_gate_d1` executa e reporta falhas de DQ.
- O dashboard mostra:
  - `DQ Failed = 3`
  - trades duplicados
  - preços de liquidação nulos
  - posições com soma zero
- Os logs contêm o mesmo `ctm_run_id`, `ctm_job`, `dd.trace_id` e `dd.span_id` do trace APM.

## Rodar Falhas Hard

Estes cenários fazem um job falhar, não apenas o gate de DQ:

```bash
make demo-hard-oracle-timeout BUSINESS_DATE=$(date +%F)
make demo-hard-gate-fail BUSINESS_DATE=$(date +%F)
make demo-hard-s3-down BUSINESS_DATE=$(date +%F)
make demo-db-blocking BUSINESS_DATE=$(date +%F)
make demo-db-deadlock BUSINESS_DATE=$(date +%F)
```

## Dashboard Datadog

Dashboard principal:

```text
https://app.datadoghq.com/dashboard/6ug-s4c-mu3/b3-control-m-pipeline-operations?fromUser=true&refresh_mode=sliding&from_ts=1779881715239&to_ts=1779896115239&live=true
```

Definição local do dashboard:

```text
datadog/dashboards/b3_controlm_pipeline_ops.json
```

O dashboard está escopado para:

```text
env:demo
service:controlm-sim
```

## O Que Mostrar no Datadog

### Dashboard

Mostre:

- Pipelines OK / FAILED.
- Jobs OK / NOT OK.
- Duração dos jobs.
- Cards de Data Quality.
- Detalhes de DQ por check.
- Paridade de contagem de linhas entre Oracle e SQL Server.

### APM

Pesquise:

```text
service:controlm-sim env:demo
```

Abra um trace de um job. O waterfall deve mostrar spans filhos para:

- `demo-oracle`
- `demo-sqlserver`
- `mock-exchange`

Isso demonstra o Python chamando Oracle, gravando no SQL Server e publicando no S3/MinIO.

### Logs

Pesquise:

```text
service:controlm-sim env:demo
```

Para uma execução específica:

```text
service:controlm-sim ctm_run_id:<run_id>
```

Para um trace específico:

```text
dd.trace_id:<trace_id>
```

Logs estruturados de job incluem:

- `ctm_job`
- `ctm_order_date`
- `ctm_run_id`
- `status`
- `duration_seconds`
- `output_rows`
- `dd.trace_id`
- `dd.span_id`

### DBM

Use DBM para mostrar:

- Atividade de queries no SQL Server.
- Queries fonte no Oracle.
- Cenários de query lenta, blocking ou deadlock.
- Tempo de query junto da duração do job Control-M.

## Modelo de Telemetria

### Métricas

O wrapper Python emite métricas Control-M via DogStatsD:

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

Tags importantes:

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

Cada job cria um trace APM com spans para:

- Execução do wrapper Control-M.
- Conexão e SQL no Oracle.
- Conexão e SQL no SQL Server.
- `put object` no MinIO/S3.

### Logs

O Agent faz tail destes arquivos:

```text
/data/logs/controlm-sim.log
/data/logs/jobs.jsonl
/data/runs.jsonl
logs/demo-cron.log
```

Configurado em:

```text
datadog/conf.d/controlm_logs.d/conf.yaml
```

A correlação trace/log usa:

```text
dd.trace_id
dd.span_id
```

### Correlação entre Métricas e Traces

Métricas são séries temporais agregadas, então não carregam um trace ID único por ponto. O Datadog faz o pivot de uma métrica para traces/logs relacionados usando tags compartilhadas como:

```text
env
service
version
ctm_job
ctm_order_date
```

Para correlação exata por execução, use campos de trace ou log:

```text
ctm_run_id:<run_id>
dd.trace_id:<trace_id>
```

## Control-M Workbench

O Workbench é opcional e roda atrás de um profile do Compose:

```bash
make up-workbench
```

Deploy das variantes de jobs geradas:

```bash
make ctm-generate
make ctm-deploy-all
```

O gerador lê:

```text
controlm/jobs/manifest.yaml
```

e produz variantes Script, Command, Embedded Script e Database.

## Cron Sidecar

Inicie o rotacionador de cenários:

```bash
podman compose --profile cron up -d --build demo-cron
```

Ele alterna cenários de sucesso, falha de DQ, hard failure e DBM. Os logs são escritos em:

```text
logs/demo-cron.log
```

e coletados pelo Datadog Agent.

## Estrutura do Projeto

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

## Comandos Comuns

| Comando | Descrição |
|---|---|
| `make up` | Sobe a stack completa |
| `make down` | Para os serviços |
| `make clean` | Para os serviços e remove volumes |
| `make status` | Mostra o status dos containers |
| `make logs` | Acompanha logs do Compose |
| `make dd-agent-status` | Mostra o status do Datadog Agent |
| `make dd-smoke` | Emite uma métrica/trace de smoke |
| `make generate-day` | Gera dados de mercado para uma data |
| `make run-d1` | Roda a pipeline D+1 completa |
| `make run-job JOB=quality_gate_d1` | Roda um job específico |
| `make demo-hard-oracle-timeout` | Simula timeout no Oracle |
| `make demo-hard-gate-fail` | Simula falha hard no gate de DQ |
| `make demo-hard-s3-down` | Simula falha no S3/MinIO |
| `make demo-db-blocking` | Simula blocking no banco |
| `make demo-db-deadlock` | Simula deadlock no banco |

## Troubleshooting

### Sem Dados no Datadog

Confira o `.env`:

```bash
cat .env
```

Depois confira o Agent:

```bash
make dd-agent-status
```

### Logs Não Aparecem no Trace APM

Abra um trace novo, gerado depois da última versão do código estar rodando. Traces antigos não recebem correlação de logs retroativamente.

Confira os logs diretamente:

```text
service:controlm-sim dd.trace_id:<trace_id>
```

### Dashboard Sem Dados

Use `Past 30 Minutes` ou `Past 1 Hour`, depois rode:

```bash
podman exec -e JOB_PADDING_FACTOR=0 demo-controlm-sim \
  python main.py run-pipeline --business-date $(date +%F)
```

### Agent Não Consegue Ler Logs

Confira as integrações de tail de arquivo:

```bash
podman exec demo-datadog-agent agent status
```

Procure pela seção `controlm_logs`.

## Fluxo Sugerido Para Video

1. Explique o desafio: jobs Control-M são difíceis de diagnosticar apenas pelo status.
2. Explique a estratégia: métricas para visão geral, APM para caminho de execução, logs para evidência e DBM para diagnóstico de banco.
3. Clone o repositório e configure `.env`.
4. Suba com `podman compose up -d --build`.
5. Rode um happy path.
6. Rode uma falha de DQ.
7. Mostre o dashboard Datadog.
8. Abra um trace e mostre spans filhos para Oracle, SQL Server e S3.
9. Abra logs a partir do trace e mostre `dd.trace_id`.
10. Abra DBM para evidências de queries no SQL Server/Oracle.

## Licença

Prova de conceito para fins de demonstração.

---

# English Version

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

## Pipeline Jobs

| Job | What It Does | Main Signal in the Demo |
|---|---|---|
| `market_data_ingest` | Generates and loads the market close source data into Oracle XE. It represents the upstream market data ingestion step before the D+1 batch starts. | Shows the first Python job, source data volume, and the beginning of the trace/log correlation. |
| `close_market_eod` | Reads derivative trade movement data from Oracle (`ASTADRVT_TRADE_MVMT`) and loads the SQL Server DW trade table (`ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO`). | Shows Oracle-to-SQL Server ETL duration, row counts, retries, and failure handling. |
| `reconcile_d1_positions` | Reconciles D+1 derivative and cash positions from Oracle (`ASTANO_*`, `ASTACASH_*`) into SQL Server position tables. | Shows the heavier batch step, longer duration, DBM queries, and where slow SQL or locking issues are visible. |
| `quality_gate_d1` | Runs Data Quality checks on the DW outputs: duplicates, null settlement price, zero-sum positions, row-count parity, and overflow conditions. | Shows DQ metrics, `DQ Failed`, failed checks by type, and gate pass/fail behavior. |
| `publish_d1_reports` | Publishes the curated SQL Server outputs to MinIO/S3 as report files for downstream consumers. | Shows the final export step, S3 publish failures, output rows, logs, and job completion status. |

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
