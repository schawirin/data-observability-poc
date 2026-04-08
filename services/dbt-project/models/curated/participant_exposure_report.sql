-- Job 4b: publish_d1_reports - Participant exposure report
-- Mark-to-market exposure per participant per ticker

SELECT
    pr.participant_id,
    pr.ticker,
    pr.business_date,
    pr.declared_quantity,
    pr.traded_net_qty,
    pr.quantity_diff,
    pr.recon_status,
    cp.closing_price,
    pr.declared_quantity * COALESCE(cp.closing_price, 0) AS mark_to_market,
    pr.quantity_diff * COALESCE(cp.closing_price, 0) AS exposure_diff,
    NOW() AS report_generated_at
FROM {{ ref('position_recon') }} pr
LEFT JOIN {{ ref('close_prices') }} cp
    ON pr.ticker = cp.ticker
    AND pr.business_date = cp.business_date
