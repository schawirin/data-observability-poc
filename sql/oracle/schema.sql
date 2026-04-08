-- Oracle 23c Free — Data Pipeline POC source schema (ASTA* tables)
-- Run as: system / OracleDemo123!
-- These tables simulate what the exchange's transactional Oracle DB would expose
-- as source datasets for the Data Observability pipeline.
--
-- Table naming follows exchange convention:
--   ASTA* = source (application / transactional layer)
--
-- Oracle 23c Free supports CREATE TABLE IF NOT EXISTS natively.

-- ── ASTADRVT_TRADE_MVMT ──────────────────────────────────────────────────────
-- Derivatives trade movement from the exchange matching engine.
-- Source for close_market_eod → ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO (DW).
-- Caso 1 (duplicates): 4 duplicate rows may be injected here to survive ETL.
CREATE TABLE IF NOT EXISTS ASTADRVT_TRADE_MVMT (
    trade_id         VARCHAR2(36)    NOT NULL,
    ticker           VARCHAR2(20)    NOT NULL,
    notional         NUMBER(18,4)    NOT NULL,
    quantity         NUMBER(10)      NOT NULL,
    trade_dt         DATE            NOT NULL,
    settlement_dt    DATE,
    counterparty_id  VARCHAR2(20),
    trader_id        VARCHAR2(20),
    desk_code        VARCHAR2(10),
    status           VARCHAR2(20)    DEFAULT 'EXECUTED',
    created_at       TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_astadrvt_trade_mvmt PRIMARY KEY (trade_id)
);

CREATE INDEX IF NOT EXISTS idx_astadrvt_trade_dt
    ON ASTADRVT_TRADE_MVMT (trade_dt);

CREATE INDEX IF NOT EXISTS idx_astadrvt_ticker
    ON ASTADRVT_TRADE_MVMT (ticker);

-- ── ASTANO_FGBE_DRVT_PSTN ────────────────────────────────────────────────────
-- Non-fungible derivative position (FGBE = futures, options, etc.).
-- Source for reconcile_d1_positions → ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL.
-- Caso 2 (null column): settlement_price may be NULL for some rows.
CREATE TABLE IF NOT EXISTS ASTANO_FGBE_DRVT_PSTN (
    position_id      VARCHAR2(36)    NOT NULL,
    instrument_id    VARCHAR2(36)    NOT NULL,
    participant_code VARCHAR2(20)    NOT NULL,
    position_date    DATE            NOT NULL,
    long_qty         NUMBER(14,4)    DEFAULT 0,
    short_qty        NUMBER(14,4)    DEFAULT 0,
    net_qty          NUMBER(14,4)    DEFAULT 0,
    settlement_price NUMBER(18,6),              -- NULL is valid upstream, flagged in DW
    margin_value     NUMBER(18,4),
    created_at       TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_astano_fgbe_drvt_pstn PRIMARY KEY (position_id)
);

CREATE INDEX IF NOT EXISTS idx_astano_fgbe_posdt
    ON ASTANO_FGBE_DRVT_PSTN (position_date);

CREATE INDEX IF NOT EXISTS idx_astano_fgbe_participant
    ON ASTANO_FGBE_DRVT_PSTN (participant_code);

-- ── ASTACASH_MRKT_PSTN ───────────────────────────────────────────────────────
-- Spot market position per participant/ticker.
-- Source for reconcile_d1_positions → ADWPM_POSICAO_MERCADO_A_VISTA.
-- Caso 3 (sum = zero): rows where long_value + short_value = 0 indicate a fault.
CREATE TABLE IF NOT EXISTS ASTACASH_MRKT_PSTN (
    position_id      VARCHAR2(36)    NOT NULL,
    ticker           VARCHAR2(20)    NOT NULL,
    participant_code VARCHAR2(20)    NOT NULL,
    position_date    DATE            NOT NULL,
    long_value       NUMBER(18,4)    DEFAULT 0,
    short_value      NUMBER(18,4)    DEFAULT 0,
    net_value        NUMBER(18,4)    DEFAULT 0,   -- long_value + short_value (non-zero expected)
    created_at       TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_astacash_mrkt_pstn PRIMARY KEY (position_id)
);

CREATE INDEX IF NOT EXISTS idx_astacash_posdt
    ON ASTACASH_MRKT_PSTN (position_date);

CREATE INDEX IF NOT EXISTS idx_astacash_ticker
    ON ASTACASH_MRKT_PSTN (ticker);
