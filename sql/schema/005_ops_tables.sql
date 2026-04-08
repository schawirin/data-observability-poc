USE exchange;

CREATE TABLE IF NOT EXISTS ops_quality_results (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(36) NOT NULL,
    business_date DATE NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    check_type ENUM('completeness','uniqueness','consistency','reasonableness','timeliness') NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    severity ENUM('critical','warning','info') NOT NULL,
    passed TINYINT NOT NULL,
    expected_value VARCHAR(255),
    actual_value VARCHAR(255),
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_quality_run (run_id),
    INDEX idx_quality_date (business_date),
    INDEX idx_quality_severity (severity, passed)
) ENGINE=InnoDB;
