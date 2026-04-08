-- Job 2b: reconcile_d1_positions - Position reconciliation
-- Compares declared positions with net traded quantities

WITH net_traded AS (
    SELECT
        business_date,
        buyer_participant AS participant_id,
        ticker,
        SUM(quantity) AS net_quantity
    FROM {{ source('raw', 'raw_trades') }}
    GROUP BY business_date, buyer_participant, ticker

    UNION ALL

    SELECT
        business_date,
        seller_participant AS participant_id,
        ticker,
        -SUM(quantity) AS net_quantity
    FROM {{ source('raw', 'raw_trades') }}
    GROUP BY business_date, seller_participant, ticker
),
net_positions AS (
    SELECT
        business_date,
        participant_id,
        ticker,
        SUM(net_quantity) AS traded_net_qty
    FROM net_traded
    GROUP BY business_date, participant_id, ticker
)
SELECT
    pp.participant_id,
    pp.ticker,
    pp.business_date,
    pp.net_quantity AS declared_quantity,
    COALESCE(np.traded_net_qty, 0) AS traded_net_qty,
    pp.net_quantity - COALESCE(np.traded_net_qty, 0) AS quantity_diff,
    CASE
        WHEN ABS(pp.net_quantity - COALESCE(np.traded_net_qty, 0)) < 1 THEN 'MATCHED'
        ELSE 'BREAK'
    END AS recon_status,
    NOW() AS reconciled_at
FROM {{ source('raw', 'raw_participant_positions') }} pp
LEFT JOIN net_positions np
    ON pp.participant_id = np.participant_id
    AND pp.ticker = np.ticker
    AND pp.business_date = np.business_date
