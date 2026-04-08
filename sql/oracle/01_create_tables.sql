-- Data Pipeline POC Oracle 21c XE — ASTA* transactional tables
-- Runs automatically on first startup via container-entrypoint-initdb.d
-- User: demopoc (created by APP_USER env var in docker-compose)

-- ─── Derivatives trades (ASTADRVT_TRADE_MVMT) ────────────────────────────────
CREATE TABLE demopoc.ASTADRVT_TRADE_MVMT (
    TRADE_ID         VARCHAR2(40)    NOT NULL,
    TRADE_DT         DATE            NOT NULL,
    INSTRUMENT_CODE  VARCHAR2(20)    NOT NULL,
    BUY_PARTICIPANT  VARCHAR2(20),
    SELL_PARTICIPANT VARCHAR2(20),
    QUANTITY         NUMBER(15)      NOT NULL,
    CLOSING_PRICE    NUMBER(18,6)    NOT NULL,
    GROSS_VALUE      NUMBER(22,6),
    SETTLEMENT_DT    DATE,
    STATUS           VARCHAR2(10)    DEFAULT 'OPEN',
    BUSINESS_DATE    DATE            NOT NULL,
    CREATED_AT       TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_astadrvt_trade PRIMARY KEY (TRADE_ID)
);

-- ─── Non-fungible derivative positions (ASTANO_FGBE_DRVT_PSTN) ───────────────
CREATE TABLE demopoc.ASTANO_FGBE_DRVT_PSTN (
    POSITION_ID      VARCHAR2(40)    NOT NULL,
    POSITION_DATE    DATE            NOT NULL,
    PARTICIPANT_CODE VARCHAR2(20)    NOT NULL,
    INSTRUMENT_CODE  VARCHAR2(20)    NOT NULL,
    NET_QUANTITY     NUMBER(15)      NOT NULL,
    LONG_QUANTITY    NUMBER(15)      DEFAULT 0,
    SHORT_QUANTITY   NUMBER(15)      DEFAULT 0,
    MARKET_VALUE     NUMBER(22,6),
    BUSINESS_DATE    DATE            NOT NULL,
    UPDATED_AT       TIMESTAMP       DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_astano_fgbe_pstn PRIMARY KEY (POSITION_ID)
);

-- ─── Cash market positions (ASTACASH_MRKT_PSTN) ──────────────────────────────
CREATE TABLE demopoc.ASTACASH_MRKT_PSTN (
    POSITION_ID      VARCHAR2(40)    NOT NULL,
    POSITION_DATE    DATE            NOT NULL,
    PARTICIPANT_CODE VARCHAR2(20)    NOT NULL,
    INSTRUMENT_CODE  VARCHAR2(20)    NOT NULL,
    NET_QUANTITY     NUMBER(15)      NOT NULL,
    SETTLEMENT_VALUE NUMBER(22,6),
    BUSINESS_DATE    DATE            NOT NULL,
    CONSTRAINT pk_astacash_mrkt PRIMARY KEY (POSITION_ID)
);

-- ─── Seed sample data for today ───────────────────────────────────────────────
DECLARE
    v_today DATE := TRUNC(SYSDATE);
BEGIN
    -- 500 trades
    FOR i IN 1..500 LOOP
        INSERT INTO demopoc.ASTADRVT_TRADE_MVMT (
            TRADE_ID, TRADE_DT, INSTRUMENT_CODE,
            BUY_PARTICIPANT, SELL_PARTICIPANT,
            QUANTITY, CLOSING_PRICE, GROSS_VALUE,
            SETTLEMENT_DT, STATUS, BUSINESS_DATE
        ) VALUES (
            'TRD' || LPAD(i, 7, '0'),
            v_today,
            'WIN' || MOD(i, 5),
            'PART' || MOD(i, 10),
            'PART' || MOD(i + 5, 10),
            ROUND(DBMS_RANDOM.VALUE(100, 10000)),
            ROUND(DBMS_RANDOM.VALUE(10, 500), 2),
            ROUND(DBMS_RANDOM.VALUE(1000, 5000000), 2),
            v_today + 2,
            CASE MOD(i, 10) WHEN 0 THEN 'CANCEL' ELSE 'OPEN' END,
            v_today
        );
    END LOOP;

    -- 25 positions
    FOR i IN 1..25 LOOP
        INSERT INTO demopoc.ASTANO_FGBE_DRVT_PSTN (
            POSITION_ID, POSITION_DATE, PARTICIPANT_CODE, INSTRUMENT_CODE,
            NET_QUANTITY, LONG_QUANTITY, SHORT_QUANTITY, MARKET_VALUE, BUSINESS_DATE
        ) VALUES (
            'POS' || LPAD(i, 7, '0'),
            v_today,
            'PART' || MOD(i, 10),
            'WIN' || MOD(i, 5),
            ROUND(DBMS_RANDOM.VALUE(-1000, 1000)),
            ROUND(DBMS_RANDOM.VALUE(0, 1000)),
            ROUND(DBMS_RANDOM.VALUE(0, 500)),
            ROUND(DBMS_RANDOM.VALUE(100000, 5000000), 2),
            v_today
        );
    END LOOP;

    -- 40 cash market positions
    FOR i IN 1..40 LOOP
        INSERT INTO demopoc.ASTACASH_MRKT_PSTN (
            POSITION_ID, POSITION_DATE, PARTICIPANT_CODE, INSTRUMENT_CODE,
            NET_QUANTITY, SETTLEMENT_VALUE, BUSINESS_DATE
        ) VALUES (
            'CASH' || LPAD(i, 7, '0'),
            v_today,
            'PART' || MOD(i, 10),
            'PETR' || MOD(i, 4),
            ROUND(DBMS_RANDOM.VALUE(100, 5000)),
            ROUND(DBMS_RANDOM.VALUE(10000, 5000000), 2),
            v_today
        );
    END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Seeded 500 trades, 25 derivative positions and 40 cash positions for ' || v_today);
END;
/
