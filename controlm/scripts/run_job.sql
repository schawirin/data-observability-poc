-- run_job.sql -- Pure SQL DQ checks for the Job:Database variant.
--
-- Use when B3 agents have NO Python and they prefer to run SQL directly via
-- Control-M Database connection. Datadog observability comes from:
--   * DBM capturing the query (latency, plan, errors)
--   * Control-M Automation API exposing job status + output as logs
--   * No OpenLineage emission (lineage edge is inferred from DBM table touches)
--
-- The Job:Database variant in market_d1_database.json embeds equivalent
-- inline SQL per job (generated from manifest.yaml `sql:` blocks). This file
-- exists as a reference implementation that B3's DBA can copy/adapt.
--
-- Parameter: business_date in 'YYYY-MM-DD' (passed by Control-M as the first
-- parameter binding when the SQL is parameterized, or substituted via
-- %%ODATE%% if running as a raw template).

-- ── Caso 1: Duplicate derivatives trades ────────────────────────────────────
SELECT
  'CASO_1_DUPLICATES' AS check_name,
  COUNT(*) - COUNT(DISTINCT trade_id) AS violation_count
FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
WHERE business_date = ?;

-- ── Caso 2: Null settlement price ───────────────────────────────────────────
SELECT
  'CASO_2_NULL_SETTLEMENT_PRICE' AS check_name,
  COUNT(*) AS violation_count
FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
WHERE business_date = ?
  AND settlement_price IS NULL;

-- ── Caso 3: Zero-sum positions ──────────────────────────────────────────────
SELECT
  'CASO_3_ZERO_SUM' AS check_name,
  COUNT(*) AS violation_count
FROM dbo.ADWPM_POSICAO_MERCADO_A_VISTA
WHERE business_date = ?
  AND (long_value + short_value) = 0;

-- ── Row count parity Oracle vs SQL Server (advisory) ────────────────────────
-- Run this on each side separately; Control-M can compare via downstream job.
SELECT
  'ROW_COUNT_SOURCE' AS check_name,
  COUNT(*) AS row_count
FROM DEMOPOC.ASTADRVT_TRADE_MVMT
WHERE business_date = ?;
