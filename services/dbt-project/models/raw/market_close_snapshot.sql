-- Job 1b: curated market close snapshot
-- Mirrors: curated_market_close_snapshot from close_market_eod.py

SELECT
    ticker,
    business_date,
    closing_price,
    daily_volume,
    vwap,
    trade_count,
    total_notional,
    CASE
        WHEN daily_volume > 1000000 THEN 'HIGH'
        WHEN daily_volume > 100000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS volume_category,
    NOW() AS snapshot_time
FROM {{ ref('close_prices') }}
