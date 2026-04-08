-- SQL Server 2022 — Data Pipeline POC Data Warehouse schema (ADWPM* tables)
-- Run as: sa / SqlDemo12345!
-- These tables are the DW destination for all ETL pipelines.
--
-- Table naming follows exchange convention:
--   ADWPM* = DW destination (analytics / reporting layer)
--
-- DW columns added to every table:
--   dw_load_dt   DATETIME2  — when the row was loaded into the DW
--   dw_source    VARCHAR(50) — source system identifier ("oracle" or "mysql_fallback")
--   dw_row_hash  VARCHAR(64) — SHA-256 of business key columns for dedup detection

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'demopoc')
BEGIN
    CREATE DATABASE demopoc;
END
GO

USE demopoc;
GO

-- ── ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO ───────────────────────────────────────
-- Mirrors ASTADRVT_TRADE_MVMT from Oracle.
-- Destination for close_market_eod ETL job.
-- Caso 1: duplicate trade_id rows may appear here after fault injection.
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO'
)
BEGIN
    CREATE TABLE dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO (
        trade_id         VARCHAR(36)     NOT NULL,
        ticker           VARCHAR(20)     NOT NULL,
        notional         DECIMAL(18,4)   NOT NULL,
        quantity         DECIMAL(14,0)   NOT NULL,
        trade_dt         DATE            NOT NULL,
        settlement_dt    DATE,
        counterparty_id  VARCHAR(20),
        trader_id        VARCHAR(20),
        desk_code        VARCHAR(10),
        status           VARCHAR(20)     NOT NULL DEFAULT 'EXECUTED',
        -- DW audit columns
        dw_load_dt       DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
        dw_source        VARCHAR(50)     NOT NULL DEFAULT 'oracle',
        dw_row_hash      VARCHAR(64),
        CONSTRAINT pk_adwpm_mvmt_negocio PRIMARY KEY (trade_id, dw_load_dt)
    );

    CREATE INDEX idx_adwpm_mvmt_trade_dt
        ON dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO (trade_dt);

    CREATE INDEX idx_adwpm_mvmt_ticker
        ON dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO (ticker);

    -- Index for duplicate detection (Caso 1)
    CREATE INDEX idx_adwpm_mvmt_tradeid
        ON dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO (trade_id);
END
GO

-- ── ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL ────────────────────────────────────
-- Mirrors ASTANO_FGBE_DRVT_PSTN from Oracle.
-- Destination for reconcile_d1_positions ETL job.
-- Caso 2: settlement_price can be NULL — quality_gate detects nulls > 0.
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL'
)
BEGIN
    CREATE TABLE dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL (
        position_id      VARCHAR(36)     NOT NULL,
        instrument_id    VARCHAR(36)     NOT NULL,
        participant_code VARCHAR(20)     NOT NULL,
        position_date    DATE            NOT NULL,
        long_qty         DECIMAL(14,4)   NOT NULL DEFAULT 0,
        short_qty        DECIMAL(14,4)   NOT NULL DEFAULT 0,
        net_qty          DECIMAL(14,4)   NOT NULL DEFAULT 0,
        settlement_price DECIMAL(18,6),           -- NULL allowed; Caso 2 fault: NULL present
        margin_value     DECIMAL(18,4),
        -- DW audit columns
        dw_load_dt       DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
        dw_source        VARCHAR(50)     NOT NULL DEFAULT 'oracle',
        dw_row_hash      VARCHAR(64),
        CONSTRAINT pk_adwpm_posicao_drvt PRIMARY KEY (position_id, dw_load_dt)
    );

    CREATE INDEX idx_adwpm_drvt_posdt
        ON dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL (position_date);

    CREATE INDEX idx_adwpm_drvt_participant
        ON dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL (participant_code);

    -- Index for Caso 2 null check
    CREATE INDEX idx_adwpm_drvt_settlement_price
        ON dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL (position_date, settlement_price);
END
GO

-- ── ADWPM_POSICAO_MERCADO_A_VISTA ────────────────────────────────────────────
-- Mirrors ASTACASH_MRKT_PSTN from Oracle.
-- Destination for reconcile_d1_positions ETL job.
-- Caso 3: rows where long_value + short_value = 0 are flagged as a DQ fault.
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ADWPM_POSICAO_MERCADO_A_VISTA'
)
BEGIN
    CREATE TABLE dbo.ADWPM_POSICAO_MERCADO_A_VISTA (
        position_id      VARCHAR(36)     NOT NULL,
        ticker           VARCHAR(20)     NOT NULL,
        participant_code VARCHAR(20)     NOT NULL,
        position_date    DATE            NOT NULL,
        long_value       DECIMAL(18,4)   NOT NULL DEFAULT 0,
        short_value      DECIMAL(18,4)   NOT NULL DEFAULT 0,
        net_value        DECIMAL(18,4)   NOT NULL DEFAULT 0,  -- = long_value + short_value
        -- DW audit columns
        dw_load_dt       DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
        dw_source        VARCHAR(50)     NOT NULL DEFAULT 'oracle',
        dw_row_hash      VARCHAR(64),
        CONSTRAINT pk_adwpm_posicao_vista PRIMARY KEY (position_id, dw_load_dt)
    );

    CREATE INDEX idx_adwpm_vista_posdt
        ON dbo.ADWPM_POSICAO_MERCADO_A_VISTA (position_date);

    CREATE INDEX idx_adwpm_vista_ticker
        ON dbo.ADWPM_POSICAO_MERCADO_A_VISTA (ticker);
END
GO

-- ── ADWPM_DQ_RESULTS ────────────────────────────────────────────────────────
-- Stores quality gate check results for observability and alerting.
-- Written by quality_gate_d1 job; read by Datadog log monitors.
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'ADWPM_DQ_RESULTS'
)
BEGIN
    CREATE TABLE dbo.ADWPM_DQ_RESULTS (
        result_id        VARCHAR(36)     NOT NULL DEFAULT NEWID(),
        run_id           VARCHAR(36)     NOT NULL,
        business_date    DATE            NOT NULL,
        check_name       VARCHAR(100)    NOT NULL,
        check_type       VARCHAR(50)     NOT NULL,
        target_table     VARCHAR(100)    NOT NULL,
        severity         VARCHAR(20)     NOT NULL,  -- 'critical' | 'warning'
        passed           BIT             NOT NULL,
        expected_value   VARCHAR(200),
        actual_value     VARCHAR(200),
        details          VARCHAR(MAX),
        created_at       DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT pk_adwpm_dq_results PRIMARY KEY (result_id)
    );

    CREATE INDEX idx_adwpm_dq_bdate
        ON dbo.ADWPM_DQ_RESULTS (business_date);

    CREATE INDEX idx_adwpm_dq_passed
        ON dbo.ADWPM_DQ_RESULTS (business_date, passed, severity);
END
GO

-- ── Datadog monitoring user ──────────────────────────────────────────────────
-- Used by the DD Agent for DBM (Database Monitoring) + custom queries.
USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'datadog')
BEGIN
    CREATE LOGIN datadog WITH PASSWORD = 'Dd@DemoPoc2026!', CHECK_POLICY = OFF;
END
GO

USE demopoc;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'datadog')
BEGIN
    CREATE USER datadog FOR LOGIN datadog;
END
GO

USE master;
GO

-- Grant permissions for DBM
GRANT CONNECT ANY DATABASE TO datadog;
GRANT VIEW SERVER STATE TO datadog;
GRANT VIEW ANY DEFINITION TO datadog;
GO

USE demopoc;
GO

ALTER ROLE db_datareader ADD MEMBER datadog;
GO
