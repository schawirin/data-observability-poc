-- Job 3: quality_gate_d1 - Data quality checks
-- Mirrors: pipeline-runner/quality/checks.py

WITH completeness_closing_price AS (
    SELECT
        'completeness' AS check_type,
        'closing_price_not_null' AS check_name,
        'critical' AS severity,
        (SELECT COUNT(*) FROM {{ ref('close_prices') }} WHERE closing_price IS NULL) AS failures,
        (SELECT COUNT(*) FROM {{ ref('close_prices') }}) AS total_rows
),
uniqueness_trade_id AS (
    SELECT
        'uniqueness' AS check_type,
        'trade_id_unique' AS check_name,
        'critical' AS severity,
        (SELECT COUNT(*) - COUNT(DISTINCT trade_id)
         FROM {{ source('raw', 'raw_trades') }}) AS failures,
        (SELECT COUNT(*) FROM {{ source('raw', 'raw_trades') }}) AS total_rows
),
consistency_settlement AS (
    SELECT
        'consistency' AS check_type,
        'settlement_has_trade' AS check_name,
        'critical' AS severity,
        (SELECT COUNT(*) FROM {{ ref('settlement_recon') }}
         WHERE recon_status = 'MISSING_INSTRUCTION') AS failures,
        (SELECT COUNT(*) FROM {{ ref('settlement_recon') }}) AS total_rows
),
reasonableness_price AS (
    SELECT
        'reasonableness' AS check_type,
        'price_positive' AS check_name,
        'critical' AS severity,
        (SELECT COUNT(*) FROM {{ source('raw', 'raw_trades') }} WHERE price <= 0) AS failures,
        (SELECT COUNT(*) FROM {{ source('raw', 'raw_trades') }}) AS total_rows
),
reasonableness_quantity AS (
    SELECT
        'reasonableness' AS check_type,
        'quantity_positive' AS check_name,
        'critical' AS severity,
        (SELECT COUNT(*) FROM {{ source('raw', 'raw_trades') }} WHERE quantity <= 0) AS failures,
        (SELECT COUNT(*) FROM {{ source('raw', 'raw_trades') }}) AS total_rows
)
SELECT
    check_type,
    check_name,
    severity,
    failures,
    total_rows,
    CASE WHEN failures = 0 THEN TRUE ELSE FALSE END AS passed,
    CASE WHEN total_rows > 0
        THEN ROUND((1.0 - failures::NUMERIC / total_rows) * 100, 2)
        ELSE 100.0
    END AS pass_rate,
    NOW() AS checked_at
FROM (
    SELECT * FROM completeness_closing_price
    UNION ALL SELECT * FROM uniqueness_trade_id
    UNION ALL SELECT * FROM consistency_settlement
    UNION ALL SELECT * FROM reasonableness_price
    UNION ALL SELECT * FROM reasonableness_quantity
) checks
