-- Job 4a: publish_d1_reports - Settlement report
-- Aggregated metrics from reconciliation

SELECT
    business_date,
    COUNT(*) AS total_trades,
    SUM(CASE WHEN recon_status = 'MATCHED' THEN 1 ELSE 0 END) AS matched_count,
    SUM(CASE WHEN recon_status = 'BREAK' THEN 1 ELSE 0 END) AS break_count,
    SUM(CASE WHEN recon_status = 'MISSING_INSTRUCTION' THEN 1 ELSE 0 END) AS missing_count,
    ROUND(
        SUM(CASE WHEN recon_status = 'MATCHED' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    ) AS match_rate_pct,
    SUM(CASE WHEN recon_status = 'BREAK' THEN notional_value ELSE 0 END) AS break_notional,
    SUM(notional_value) AS total_notional,
    NOW() AS report_generated_at
FROM {{ ref('settlement_recon') }}
GROUP BY business_date
