USE exchange;

CREATE TABLE IF NOT EXISTS raw_trades (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_id VARCHAR(36) NOT NULL,
    order_id VARCHAR(36) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    trade_time DATETIME NOT NULL,
    price DECIMAL(18,4) NOT NULL,
    quantity INT NOT NULL,
    buyer_participant VARCHAR(20) NOT NULL,
    seller_participant VARCHAR(20) NOT NULL,
    business_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_trades_business_date (business_date),
    INDEX idx_trades_ticker (ticker)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS raw_orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    side ENUM('BUY','SELL') NOT NULL,
    order_time DATETIME NOT NULL,
    price DECIMAL(18,4) NOT NULL,
    quantity INT NOT NULL,
    participant VARCHAR(20) NOT NULL,
    business_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_orders_business_date (business_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS raw_close_prices (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    business_date DATE NOT NULL,
    closing_price DECIMAL(18,4),
    daily_volume BIGINT,
    vwap DECIMAL(18,4),
    auction_flag TINYINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_close_ticker_date (ticker, business_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS raw_participant_positions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    participant VARCHAR(20) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    business_date DATE NOT NULL,
    net_quantity INT NOT NULL,
    avg_price DECIMAL(18,4) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_positions_business_date (business_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS raw_settlement_instructions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    instruction_id VARCHAR(36) NOT NULL,
    trade_id VARCHAR(36) NOT NULL,
    settlement_date DATE NOT NULL,
    participant VARCHAR(20) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    amount DECIMAL(18,4) NOT NULL,
    status ENUM('PENDING','MATCHED','FAILED') DEFAULT 'PENDING',
    business_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_settlement_business_date (business_date)
    -- NOTE: no index on trade_id intentionally, to create slow join for DBM demo
) ENGINE=InnoDB;
