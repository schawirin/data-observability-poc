-- Job 1a: close_market_eod - Calculate closing prices from trades
-- Mirrors: pipeline-runner/jobs/close_market_eod.py

WITH trade_stats AS (
    SELECT
        ticker,
        business_date,
        COUNT(*) AS trade_count,
        SUM(quantity) AS daily_volume,
        SUM(price * quantity) / NULLIF(SUM(quantity), 0) AS vwap,
        SUM(price * quantity) AS total_notional
    FROM {{ source('raw', 'raw_trades') }}
    GROUP BY ticker, business_date
),
last_trade AS (
    SELECT DISTINCT ON (ticker, business_date)
        ticker,
        business_date,
        price AS closing_price
    FROM {{ source('raw', 'raw_trades') }}
    ORDER BY ticker, business_date, trade_time DESC
)
SELECT
    lt.ticker,
    lt.business_date,
    lt.closing_price,
    ts.daily_volume,
    ts.vwap,
    ts.trade_count,
    ts.total_notional,
    FALSE AS auction_flag,
    NOW() AS created_at
FROM last_trade lt
JOIN trade_stats ts ON lt.ticker = ts.ticker AND lt.business_date = ts.business_date
