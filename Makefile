.PHONY: up down seed run-eod run-d1 inject-fault demo-happy demo-fail logs status clean \
        ctm-deploy ctm-run-d1 ctm-run-eod ctm-status ctm-login \
        demo-caso1 demo-caso2 demo-caso3 demo-overflow demo-row-count-diff

# Default business date: today
BUSINESS_DATE ?= $(shell date +%Y-%m-%d)

# ─── Control-M Workbench settings ───────────────────────────────
CTM_HOST ?= localhost
CTM_PORT ?= 8443
CTM_USER ?= workbench
CTM_PASS ?= workbench
CTM_BASE  = https://$(CTM_HOST):$(CTM_PORT)/automation-api

# ─── Infrastructure ──────────────────────────────────────────────
up:
	@echo "▶ Starting Data Pipeline POC stack..."
	docker compose up -d --build
	@echo "✓ Stack is up. MySQL: localhost:3306 | MinIO: localhost:9001 | Adminer: localhost:8080"

down:
	docker compose down

clean:
	docker compose down -v
	@echo "✓ Volumes removed"

status:
	docker compose ps

logs:
	docker compose logs -f --tail=50

# ─── Data ────────────────────────────────────────────────────────
seed:
	@echo "▶ Seeding reference data..."
	docker exec demo-mysql mysql -u root -p$${MYSQL_ROOT_PASSWORD:-demopoc2026} exchange < sql/seeds/seed_participants.sql
	@echo "✓ Seed complete"

generate-day:
	@echo "▶ Generating market data for $(BUSINESS_DATE)..."
	docker exec demo-market-mock python main.py generate-day --business-date $(BUSINESS_DATE)
	@echo "✓ Market data generated"

# ─── Control-M Automation API ───────────────────────────────────
ctm-login:
	@echo "▶ Authenticating to Control-M Workbench..."
	@curl -sk -X POST "$(CTM_BASE)/session/login" \
	  -H "Content-Type: application/json" \
	  -d '{"username":"$(CTM_USER)","password":"$(CTM_PASS)"}' | python3 -m json.tool

ctm-deploy:
	@echo "▶ Deploying Market_D1_Pipeline to Control-M Workbench..."
	@bash controlm/setup.sh $(CTM_HOST) $(CTM_PORT)

ctm-run-d1:
	@echo "▶ Triggering Market_D1_Pipeline for $(BUSINESS_DATE) via Control-M..."
	$(eval TOKEN := $(shell curl -sk -X POST "$(CTM_BASE)/session/login" \
	  -H "Content-Type: application/json" \
	  -d '{"username":"$(CTM_USER)","password":"$(CTM_PASS)"}' \
	  | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))"))
	@curl -sk -X POST "$(CTM_BASE)/run" \
	  -H "Authorization: Bearer $(TOKEN)" \
	  -H "Content-Type: application/json" \
	  -d '{"folder":"Market_D1_Pipeline","variables":[{"ODATE":"$(BUSINESS_DATE)"}]}' \
	  | python3 -m json.tool

ctm-run-eod:
	@echo "▶ Triggering Market_close_market_eod for $(BUSINESS_DATE) via Control-M..."
	$(eval TOKEN := $(shell curl -sk -X POST "$(CTM_BASE)/session/login" \
	  -H "Content-Type: application/json" \
	  -d '{"username":"$(CTM_USER)","password":"$(CTM_PASS)"}' \
	  | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))"))
	@curl -sk -X POST "$(CTM_BASE)/run" \
	  -H "Authorization: Bearer $(TOKEN)" \
	  -H "Content-Type: application/json" \
	  -d '{"jobs":"Market_close_market_eod","variables":[{"ODATE":"$(BUSINESS_DATE)"}]}' \
	  | python3 -m json.tool

ctm-status:
	@echo "▶ Checking Control-M job status..."
	$(eval TOKEN := $(shell curl -sk -X POST "$(CTM_BASE)/session/login" \
	  -H "Content-Type: application/json" \
	  -d '{"username":"$(CTM_USER)","password":"$(CTM_PASS)"}' \
	  | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))"))
	@curl -sk -X GET "$(CTM_BASE)/run/jobs/status?folder=Market_D1_Pipeline" \
	  -H "Authorization: Bearer $(TOKEN)" \
	  | python3 -m json.tool

# ─── Pipeline Execution ─────────────────────────────────────────
run-eod: ctm-run-eod

run-d1: ctm-run-d1

run-job:
	@echo "▶ Running job $(JOB) for $(BUSINESS_DATE) via wrapper..."
	docker exec demo-controlm-workbench python /scripts/run_job.py --job $(JOB) --date $(BUSINESS_DATE)

# ─── Fault Injection ────────────────────────────────────────────
inject-fault:
	@echo "▶ Injecting fault: $(FAULT) for $(BUSINESS_DATE)..."
	docker exec demo-market-mock python main.py inject-fault --fault-type $(FAULT) --business-date $(BUSINESS_DATE)

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
