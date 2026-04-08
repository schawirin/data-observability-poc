USE exchange;

CREATE TABLE IF NOT EXISTS staging_trades_enriched (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_id VARCHAR(36) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    trade_time DATETIME NOT NULL,
    price DECIMAL(18,4) NOT NULL,
    quantity INT NOT NULL,
    buyer_participant VARCHAR(20) NOT NULL,
    seller_participant VARCHAR(20) NOT NULL,
    closing_price DECIMAL(18,4),
    price_vs_close_pct DECIMAL(10,4),
    business_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_enriched_business_date (business_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS staging_settlement_recon (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_id VARCHAR(36) NOT NULL,
    instruction_id VARCHAR(36),
    ticker VARCHAR(10) NOT NULL,
    trade_quantity INT NOT NULL,
    settlement_quantity INT,
    quantity_match TINYINT,
    trade_amount DECIMAL(18,4) NOT NULL,
    settlement_amount DECIMAL(18,4),
    amount_match TINYINT,
    recon_status ENUM('MATCHED','BREAK','MISSING_INSTRUCTION') NOT NULL,
    business_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_recon_business_date (business_date),
    INDEX idx_recon_status (recon_status)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS staging_position_recon (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    participant VARCHAR(20) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    position_quantity INT NOT NULL,
    traded_net_quantity INT NOT NULL,
    quantity_diff INT NOT NULL,
    recon_status ENUM('MATCHED','BREAK') NOT NULL,
    business_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_pos_recon_business_date (business_date)
) ENGINE=InnoDB;
