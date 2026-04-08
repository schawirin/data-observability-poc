USE exchange;

CREATE TABLE IF NOT EXISTS curated_market_close_snapshot (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    business_date DATE NOT NULL,
    closing_price DECIMAL(18,4),
    daily_volume BIGINT,
    vwap DECIMAL(18,4),
    total_trades INT,
    total_quantity BIGINT,
    total_notional DECIMAL(18,4),
    auction_flag TINYINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_snapshot_ticker_date (ticker, business_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS curated_d1_settlement_report (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_date DATE NOT NULL,
    total_trades INT,
    matched_trades INT,
    break_trades INT,
    missing_instruction_trades INT,
    match_rate DECIMAL(5,2),
    total_notional DECIMAL(18,4),
    break_notional DECIMAL(18,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_d1_report_date (business_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS curated_participant_exposure_report (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_date DATE NOT NULL,
    participant VARCHAR(20) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    net_position INT,
    avg_price DECIMAL(18,4),
    exposure_notional DECIMAL(18,4),
    closing_price DECIMAL(18,4),
    mark_to_market DECIMAL(18,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_exposure_date (business_date)
) ENGINE=InnoDB;
