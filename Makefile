.PHONY: up down seed run-eod run-d1 inject-fault demo-happy demo-fail logs status clean \
        ctm-deploy ctm-run-d1 ctm-run-eod ctm-status ctm-login \
        ctm-generate ctm-deploy-script ctm-deploy-command ctm-deploy-embedded \
        ctm-deploy-database ctm-deploy-all ctm-test-all-variants ctm-add-job-help \
        demo-caso1 demo-caso2 demo-caso3 demo-overflow demo-row-count-diff \
        demo-hard-oracle-timeout demo-hard-gate-fail demo-hard-s3-down demo-hard-all \
        demo-db-blocking demo-db-deadlock \
        up-workbench dd-agent-status dd-smoke \
        dind-up dind-run dind-logs dind-down

# Default business date: today
BUSINESS_DATE ?= $(shell date +%Y-%m-%d)
COMPOSE ?= podman compose
CONTAINER ?= podman

# ─── Control-M Workbench settings ───────────────────────────────
CTM_HOST ?= localhost
CTM_PORT ?= 8443
CTM_USER ?= workbench
CTM_PASS ?= workbench
CTM_BASE  = https://$(CTM_HOST):$(CTM_PORT)/automation-api

# ─── Infrastructure ──────────────────────────────────────────────
up:
	@echo "▶ Starting Data Pipeline POC stack..."
	$(COMPOSE) up -d --build
	@echo "✓ Stack is up. Control-M Sim: localhost:5000 | MinIO: localhost:9001 | Adminer: localhost:8080"

up-workbench:
	@echo "▶ Starting optional Control-M Workbench profile..."
	$(COMPOSE) --profile workbench up -d --build controlm-workbench

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v
	@echo "✓ Volumes removed"

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=50

dd-agent-status:
	$(CONTAINER) exec demo-datadog-agent agent status

dd-smoke:
	@echo "▶ Emitting one Control-M metric/APM smoke sample..."
	$(COMPOSE) up -d --build datadog-agent
	@until $(CONTAINER) exec demo-datadog-agent agent health >/dev/null 2>&1; do sleep 2; done
	$(COMPOSE) build controlm-sim
	$(COMPOSE) run --rm --no-deps controlm-sim python main.py emit-smoke-metrics --business-date $(BUSINESS_DATE)
	@echo "✓ Check Datadog metric: sum:controlm.job.executions{env:demo,ctm_job:leitura_dados}.as_count()"
	@echo "✓ Existing trace widget can use: sum:trace.controlm.job.leitura_dados.hits{env:demo}"

# ─── Docker-in-Docker CI/CD demo ─────────────────────────────────
dind-up:
	@echo "▶ Starting optional Docker-in-Docker daemon for CI/CD testing..."
	$(COMPOSE) --profile dind up -d datadog-agent ci-dind
	@echo "✓ DinD daemon ready target: tcp://ci-dind:2375"

dind-run:
	@echo "▶ Running Jenkins-like Docker-in-Docker CI job..."
	@mkdir -p logs
	$(COMPOSE) --profile dind up -d datadog-agent ci-dind
	$(COMPOSE) --profile dind up --force-recreate ci-jenkins-job
	@echo "✓ CI job finished. Datadog logs query: service:ci-jenkins-job env:demo"
	@echo "✓ Local log: logs/dind-ci.log"

dind-logs:
	$(COMPOSE) --profile dind logs -f --tail=100 ci-dind ci-jenkins-job

dind-down:
	$(COMPOSE) --profile dind rm -sf ci-jenkins-job
	$(COMPOSE) --profile dind stop ci-dind

# ─── Data ────────────────────────────────────────────────────────
seed:
	@echo "▶ Seeding reference data..."
	$(CONTAINER) exec -i demo-mysql mysql -u root -p$${MYSQL_ROOT_PASSWORD:-demopoc2026} exchange < sql/seeds/seed_participants.sql
	@echo "✓ Seed complete"

generate-day:
	@echo "▶ Generating market data for $(BUSINESS_DATE)..."
	$(CONTAINER) exec demo-market-mock python main.py generate-day --business-date $(BUSINESS_DATE)
	@echo "✓ Market data generated"

# ─── Control-M Automation API ───────────────────────────────────
ctm-login:
	@echo "▶ Authenticating to Control-M Workbench..."
	@curl -sk -X POST "$(CTM_BASE)/session/login" \
	  -H "Content-Type: application/json" \
	  -d '{"username":"$(CTM_USER)","password":"$(CTM_PASS)"}' | python3 -m json.tool

ctm-deploy:
	@echo "▶ Deploying B3_D1_Pipeline to Control-M Workbench..."
	@bash controlm/setup.sh --host $(CTM_HOST) --port $(CTM_PORT)

# ─── Variant generator + per-variant deploys ────────────────────
# Edit controlm/jobs/manifest.yaml then run `make ctm-generate` to regenerate
# the four Job:Type variants and the _job_io_generated.py table.
GENERATOR_VENV := .venv-generator
GENERATOR_PY   := $(GENERATOR_VENV)/bin/python

$(GENERATOR_PY):
	@echo "▶ Creating one-shot venv for the manifest generator..."
	@python3 -m venv $(GENERATOR_VENV)
	@$(GENERATOR_VENV)/bin/pip install --quiet pyyaml

ctm-generate: $(GENERATOR_PY)
	@$(GENERATOR_PY) controlm/generate.py

ctm-deploy-script: ctm-generate
	@bash controlm/setup.sh --variant script   --host $(CTM_HOST) --port $(CTM_PORT)

ctm-deploy-command: ctm-generate
	@bash controlm/setup.sh --variant command  --host $(CTM_HOST) --port $(CTM_PORT)

ctm-deploy-embedded: ctm-generate
	@bash controlm/setup.sh --variant embedded --host $(CTM_HOST) --port $(CTM_PORT)

ctm-deploy-database: ctm-generate
	@bash controlm/setup.sh --variant database --host $(CTM_HOST) --port $(CTM_PORT)

ctm-deploy-all: ctm-generate
	@bash controlm/setup.sh --variant all      --host $(CTM_HOST) --port $(CTM_PORT)

ctm-test-all-variants: ctm-deploy-all
	@echo "═══════════════════════════════════════════════"
	@echo "  Running all 4 variants sequentially for $(BUSINESS_DATE)"
	@echo "═══════════════════════════════════════════════"
	@for v in script command embedded database; do \
	  echo "▶ Variant $$v"; \
	  $(MAKE) ctm-run-d1 BUSINESS_DATE=$(BUSINESS_DATE) || echo "  variant $$v failed"; \
	  sleep 2; \
	  $(MAKE) ctm-status; \
	done
	@echo "✓ Validate in Datadog: 4 pipeline runs in DJM, lineage events from each Python variant."

ctm-add-job-help:
	@echo "To add a new job to the pipeline:"
	@echo "  1. Edit  controlm/jobs/manifest.yaml  — append a new entry under 'jobs:'"
	@echo "  2. Run   make ctm-generate            — regenerates the 4 JSON variants + JOB_IO"
	@echo "  3. Run   make ctm-deploy-all          — deploys all variants to Workbench/Helix"
	@echo ""
	@echo "Minimal new-job template (paste under 'jobs:' in manifest.yaml):"
	@echo "  - name: my_new_job"
	@echo "    sla: \"00:10\""
	@echo "    depends_on: publish_d1_reports   # or null"
	@echo "    description: \"What this job does\""
	@echo "    inputs:"
	@echo "      - {ns: sqlserver, name: \"dbo.SOME_TABLE\"}"
	@echo "    outputs:"
	@echo "      - {ns: minio, name: \"exports/some_output\"}"
	@echo "    # Optional — only if you also want this job in the database variant:"
	@echo "    sql:"
	@echo "      type: mssql"
	@echo "      query: |"
	@echo "        SELECT COUNT(*) FROM dbo.SOME_TABLE WHERE business_date = ?"

ctm-run-d1:
	@echo "▶ Triggering B3_D1_Pipeline for $(BUSINESS_DATE) via Control-M..."
	$(eval TOKEN := $(shell curl -sk -X POST "$(CTM_BASE)/session/login" \
	  -H "Content-Type: application/json" \
	  -d '{"username":"$(CTM_USER)","password":"$(CTM_PASS)"}' \
	  | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))"))
	@curl -sk -X POST "$(CTM_BASE)/run" \
	  -H "Authorization: Bearer $(TOKEN)" \
	  -H "Content-Type: application/json" \
	  -d '{"folder":"B3_D1_Pipeline","variables":[{"ODATE":"$(BUSINESS_DATE)"}]}' \
	  | python3 -m json.tool

ctm-run-eod:
	@echo "▶ Triggering B3_close_market_eod for $(BUSINESS_DATE) via Control-M..."
	$(eval TOKEN := $(shell curl -sk -X POST "$(CTM_BASE)/session/login" \
	  -H "Content-Type: application/json" \
	  -d '{"username":"$(CTM_USER)","password":"$(CTM_PASS)"}' \
	  | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))"))
	@curl -sk -X POST "$(CTM_BASE)/run" \
	  -H "Authorization: Bearer $(TOKEN)" \
	  -H "Content-Type: application/json" \
	  -d '{"jobs":"B3_close_market_eod","variables":[{"ODATE":"$(BUSINESS_DATE)"}]}' \
	  | python3 -m json.tool

ctm-status:
	@echo "▶ Checking Control-M job status..."
	$(eval TOKEN := $(shell curl -sk -X POST "$(CTM_BASE)/session/login" \
	  -H "Content-Type: application/json" \
	  -d '{"username":"$(CTM_USER)","password":"$(CTM_PASS)"}' \
	  | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))"))
	@curl -sk -X GET "$(CTM_BASE)/run/jobs/status?folder=B3_D1_Pipeline" \
	  -H "Authorization: Bearer $(TOKEN)" \
	  | python3 -m json.tool

# ─── Pipeline Execution ─────────────────────────────────────────
run-eod:
	$(CONTAINER) exec demo-controlm-sim python main.py run-job --job-name close_market_eod --business-date $(BUSINESS_DATE)

run-d1:
	$(CONTAINER) exec demo-controlm-sim python main.py run-pipeline --business-date $(BUSINESS_DATE)

run-job:
	@echo "▶ Running job $(JOB) for $(BUSINESS_DATE) via wrapper..."
	$(CONTAINER) exec demo-controlm-sim python main.py run-job --job-name $(JOB) --business-date $(BUSINESS_DATE)

# ─── Fault Injection ────────────────────────────────────────────
inject-fault:
	@echo "▶ Injecting fault: $(FAULT) for $(BUSINESS_DATE)..."
	$(CONTAINER) exec demo-market-mock python main.py inject-fault --fault-type $(FAULT) --business-date $(BUSINESS_DATE)

inject-duplicates:
	$(MAKE) inject-fault FAULT=duplicate_trade_id

inject-null-prices:
	$(MAKE) inject-fault FAULT=null_closing_price

inject-slow-query:
	$(MAKE) inject-fault FAULT=slow_reconciliation_query

# ─── Real Exchange Fault Injection ────────────────────────────────────
inject-caso1:
	$(MAKE) inject-fault FAULT=duplicate_trade_mvmt

inject-caso2:
	$(MAKE) inject-fault FAULT=null_settlement_price

inject-caso3:
	$(MAKE) inject-fault FAULT=zero_sum_position

inject-overflow:
	$(MAKE) inject-fault FAULT=overflow

inject-row-count-diff:
	$(MAKE) inject-fault FAULT=row_count_diff

# ─── HARD FAIL Demo Scenarios (jobs individuais falham, não só quality_gate) ──
# Para a B3 ver pinpoint de falha por job no DJM + Lineage com edges FAIL
demo-hard-oracle-timeout:  ## close_market_eod CRASHA com ORA-12170 TNS timeout
	@echo "═══ HARD FAIL: ORA-12170 em close_market_eod ═══"
	$(CONTAINER) exec demo-controlm-sim python main.py run-pipeline --business-date $(BUSINESS_DATE) --inject-fault oracle_timeout

demo-hard-gate-fail:  ## quality_gate_d1 aborta o pipeline (critical failures)
	@echo "═══ HARD FAIL: quality_gate_d1 aborta o pipeline ═══"
	$(CONTAINER) exec demo-controlm-sim python main.py run-pipeline --business-date $(BUSINESS_DATE) --inject-fault gate_fail_hard

demo-hard-s3-down:  ## publish_d1_reports falha com S3/MinIO connection refused
	@echo "═══ HARD FAIL: S3 down em publish_d1_reports ═══"
	$(CONTAINER) exec demo-controlm-sim python main.py run-pipeline --business-date $(BUSINESS_DATE) --inject-fault s3_down

demo-hard-all:  ## Todos os 3 DQ + gate hard fail = pipeline ENDED NOT OK
	@echo "═══ HARD FAIL: pipeline inteiro falha ═══"
	$(CONTAINER) exec demo-controlm-sim python main.py run-pipeline --business-date $(BUSINESS_DATE) --inject-fault all_hard

demo-db-blocking:  ## Blocking query (exclusive lock 10s em ADWPM_DQ_RESULTS)
	@echo "═══ DB: Blocking query ═══"
	$(CONTAINER) exec demo-controlm-sim python main.py run-pipeline --business-date $(BUSINESS_DATE) --inject-fault db_blocking

demo-db-deadlock:  ## Deadlock entre ADWPM_MOVIMENTO e ADWPM_POSICAO
	@echo "═══ DB: Deadlock ═══"
	$(CONTAINER) exec demo-controlm-sim python main.py run-pipeline --business-date $(BUSINESS_DATE) --inject-fault db_deadlock

# ─── Real Exchange Demo Scenarios ─────────────────────────────────────
demo-caso1:  ## Caso 1: Injects 4 duplicate rows in ASTADRVT_TRADE_MVMT + DW, runs pipeline, quality_gate fires
	@echo "═══════════════════════════════════════════════"
	@echo "  Caso 1: Duplicate Derivatives Trades"
	@echo "  ASTADRVT_TRADE_MVMT → ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO"
	@echo "═══════════════════════════════════════════════"
	$(MAKE) generate-day
	$(MAKE) run-d1
	$(MAKE) inject-caso1 BUSINESS_DATE=$(BUSINESS_DATE)
	$(MAKE) run-job JOB=quality_gate_d1
	@echo ""
	@echo "Check Datadog:"
	@echo "  - DJM: quality_gate_d1 should FAIL"
	@echo "  - Log Monitor: DQ - Duplicate Derivatives Trades [Caso 1]"
	@echo "  - Lineage: ASTADRVT_TRADE_MVMT → ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO"

demo-caso2:  ## Caso 2: Injects null settlement_price in ASTANO_FGBE_DRVT_PSTN + DW, runs pipeline
	@echo "═══════════════════════════════════════════════"
	@echo "  Caso 2: Null Settlement Price"
	@echo "  ASTANO_FGBE_DRVT_PSTN → ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL"
	@echo "═══════════════════════════════════════════════"
	$(MAKE) generate-day
	$(MAKE) run-d1
	$(MAKE) inject-caso2 BUSINESS_DATE=$(BUSINESS_DATE)
	$(MAKE) run-job JOB=quality_gate_d1
	@echo ""
	@echo "Check Datadog:"
	@echo "  - DJM: quality_gate_d1 should FAIL"
	@echo "  - Log Monitor: DQ - Null Settlement Price [Caso 2]"
	@echo "  - Lineage: ASTANO_FGBE_DRVT_PSTN → ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL"

demo-caso3:  ## Caso 3: Injects zero-sum positions in ASTACASH_MRKT_PSTN + DW, runs pipeline
	@echo "═══════════════════════════════════════════════"
	@echo "  Caso 3: Zero Sum Position (long_value + short_value = 0)"
	@echo "  ASTACASH_MRKT_PSTN → ADWPM_POSICAO_MERCADO_A_VISTA"
	@echo "═══════════════════════════════════════════════"
	$(MAKE) generate-day
	$(MAKE) run-d1
	$(MAKE) inject-caso3 BUSINESS_DATE=$(BUSINESS_DATE)
	$(MAKE) run-job JOB=quality_gate_d1
	@echo ""
	@echo "Check Datadog:"
	@echo "  - DJM: quality_gate_d1 should FAIL"
	@echo "  - Log Monitor: DQ - Zero Sum Position [Caso 3]"
	@echo "  - Lineage: ASTACASH_MRKT_PSTN → ADWPM_POSICAO_MERCADO_A_VISTA"

demo-overflow:  ## Overflow: Injects a value exceeding column precision in trade/position tables
	@echo "═══════════════════════════════════════════════"
	@echo "  Overflow: Decimal precision exceeded in notional column"
	@echo "  ASTADRVT_TRADE_MVMT / ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO"
	@echo "═══════════════════════════════════════════════"
	$(MAKE) generate-day
	$(MAKE) inject-overflow BUSINESS_DATE=$(BUSINESS_DATE)
	$(MAKE) run-d1
	@echo ""
	@echo "Check Datadog:"
	@echo "  - DJM: close_market_eod may log insert errors"
	@echo "  - DBM: arithmetic overflow query error"

# ─── Demo Scenarios ─────────────────────────────────────────────
demo-happy:
	@echo "═══════════════════════════════════════════════"
	@echo "  Scenario A: Happy Path"
	@echo "═══════════════════════════════════════════════"
	$(MAKE) generate-day
	$(MAKE) run-d1
	@echo ""
	@echo "✓ Happy path complete. Check Datadog:"
	@echo "  - DJM: 4 jobs should show as SUCCESS"
	@echo "  - Lineage: raw → staging → curated → MinIO"
	@echo "  - DBM: normal query latency"

demo-fail:
	@echo "═══════════════════════════════════════════════"
	@echo "  Scenario B: Duplicate Trade IDs"
	@echo "═══════════════════════════════════════════════"
	$(MAKE) generate-day
	$(MAKE) inject-duplicates
	$(MAKE) run-d1
	@echo ""
	@echo "✓ Failure scenario complete. Check Datadog:"
	@echo "  - DJM: quality_gate_d1 should FAIL"
	@echo "  - Monitor: Quality Gate Critical alert"
	@echo "  - Lineage: trace impact to downstream datasets"

demo-slow:
	@echo "═══════════════════════════════════════════════"
	@echo "  Scenario C: Database Bottleneck"
	@echo "═══════════════════════════════════════════════"
	$(MAKE) generate-day
	$(MAKE) inject-slow-query
	$(MAKE) run-d1
	@echo ""
	@echo "✓ Slow query scenario complete. Check Datadog:"
	@echo "  - DBM: slow reconciliation query detected"
	@echo "  - DJM: reconcile_d1_positions with longer duration"
	@echo "  - Correlation: job delay → slow query → dataset delay"

# ─── Full Demo ──────────────────────────────────────────────────
demo-full: up seed
	@sleep 10
	@echo ""
	$(MAKE) demo-happy BUSINESS_DATE=2026-03-16
	@echo ""
	@sleep 5
	$(MAKE) demo-fail BUSINESS_DATE=2026-03-17
	@echo ""
	@sleep 5
	$(MAKE) demo-slow BUSINESS_DATE=2026-03-18
