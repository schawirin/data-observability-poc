USE exchange;

-- Seed reference data: participants (brokers, desks, accounts)
-- These are fictional but representative of exchange market structure

CREATE TABLE IF NOT EXISTS ref_participants (
    id INT AUTO_INCREMENT PRIMARY KEY,
    participant_code VARCHAR(20) NOT NULL UNIQUE,
    participant_name VARCHAR(100) NOT NULL,
    participant_type ENUM('BROKER','BANK','PROP') NOT NULL,
    desk VARCHAR(50),
    account VARCHAR(30),
    active TINYINT DEFAULT 1
) ENGINE=InnoDB;

INSERT IGNORE INTO ref_participants (participant_code, participant_name, participant_type, desk, account) VALUES
('XP001',    'XP Investimentos',     'BROKER', 'Equities',   'XP-EQ-001'),
('XP002',    'XP Investimentos',     'BROKER', 'Derivatives', 'XP-DER-001'),
('BTG001',   'BTG Pactual',          'BANK',   'Flow Trading','BTG-FT-001'),
('BTG002',   'BTG Pactual',          'BANK',   'Prop',        'BTG-PR-001'),
('ITAU001',  'Itau BBA',             'BANK',   'Equities',    'ITAU-EQ-001'),
('ITAU002',  'Itau BBA',             'BANK',   'Derivatives', 'ITAU-DER-001'),
('MODAL001', 'Modal DTVM',           'BROKER', 'Equities',    'MOD-EQ-001'),
('SAFRA001', 'Safra CVC',            'BANK',   'Prop',        'SAF-PR-001'),
('GENIAL001','Genial Investimentos', 'BROKER', 'Equities',    'GEN-EQ-001'),
('CLEAR001', 'Clear Corretora',      'BROKER', 'Retail',      'CLR-RT-001');
