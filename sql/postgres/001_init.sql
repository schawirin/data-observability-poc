-- Data Pipeline POC - Postgres schema for dbt pipeline
-- Raw tables populated by market-mock, transformed by dbt

CREATE TABLE IF NOT EXISTS participants (
    participant_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    desk VARCHAR(50),
    participant_type VARCHAR(20) DEFAULT 'BROKER'
);

CREATE TABLE IF NOT EXISTS raw_trades (
    id SERIAL PRIMARY KEY,
    trade_id VARCHAR(36) NOT NULL,
    order_id VARCHAR(36),
    ticker VARCHAR(10) NOT NULL,
    trade_time TIMESTAMP NOT NULL,
    price NUMERIC(18,4) NOT NULL,
    quantity INTEGER NOT NULL,
    buyer_participant VARCHAR(20),
    seller_participant VARCHAR(20),
    business_date DATE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_bd ON raw_trades(business_date);
CREATE INDEX IF NOT EXISTS idx_trades_ticker ON raw_trades(ticker);

CREATE TABLE IF NOT EXISTS raw_orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    side VARCHAR(4) NOT NULL,
    price NUMERIC(18,4) NOT NULL,
    quantity INTEGER NOT NULL,
    participant_id VARCHAR(20),
    order_time TIMESTAMP NOT NULL,
    business_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_participant_positions (
    id SERIAL PRIMARY KEY,
    participant_id VARCHAR(20) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    net_quantity INTEGER NOT NULL,
    avg_price NUMERIC(18,4),
    business_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_settlement_instructions (
    id SERIAL PRIMARY KEY,
    settlement_id VARCHAR(36) NOT NULL,
    trade_id VARCHAR(36) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    quantity INTEGER NOT NULL,
    settlement_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    buyer_participant VARCHAR(20),
    seller_participant VARCHAR(20),
    business_date DATE NOT NULL
);

-- Seed participants
INSERT INTO participants (participant_id, name, desk, participant_type) VALUES
    ('XP001', 'XP Investimentos', 'Equities', 'BROKER'),
    ('XP002', 'XP Investimentos', 'Derivatives', 'BROKER'),
    ('BTG001', 'BTG Pactual', 'Flow Trading', 'BANK'),
    ('BTG002', 'BTG Pactual', 'Prop', 'BANK'),
    ('ITAU001', 'Itau BBA', 'Equities', 'BANK'),
    ('ITAU002', 'Itau BBA', 'Derivatives', 'BANK'),
    ('MOD001', 'Modal DTVM', 'Retail', 'BROKER'),
    ('SAF001', 'Safra CTV', 'Equities', 'BROKER'),
    ('GEN001', 'Genial Investimentos', 'Equities', 'BROKER'),
    ('CLR001', 'Clear Corretora', 'Retail', 'BROKER')
ON CONFLICT (participant_id) DO NOTHING;
