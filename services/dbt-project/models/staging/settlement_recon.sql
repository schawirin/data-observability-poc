-- Job 2a: reconcile_d1_positions - Settlement reconciliation
-- Mirrors: pipeline-runner/jobs/reconcile_d1_positions.py
-- LEFT JOIN trades with settlement instructions to find breaks

SELECT
    t.trade_id,
    t.ticker,
    t.business_date,
    t.price,
    t.quantity,
    t.buyer_participant,
    t.seller_participant,
    si.settlement_id,
    si.settlement_date,
    si.status AS settlement_status,
    CASE
        WHEN si.trade_id IS NOT NULL AND si.status = 'MATCHED' THEN 'MATCHED'
        WHEN si.trade_id IS NOT NULL AND si.status != 'MATCHED' THEN 'BREAK'
        ELSE 'MISSING_INSTRUCTION'
    END AS recon_status,
    t.price * t.quantity AS notional_value,
    NOW() AS reconciled_at
FROM {{ source('raw', 'raw_trades') }} t
LEFT JOIN {{ source('raw', 'raw_settlement_instructions') }} si
    ON t.trade_id = si.trade_id
