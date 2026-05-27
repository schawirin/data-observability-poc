"""
etl_oracle_to_sqlserver.py — ETL jobs that read from Oracle ASTA* tables and
write to SQL Server ADWPM* tables, following the exchange architecture flow:

  Sistema de Origem → Oracle (ASTA*) → [Control-M] → SQL Server DW (ADWPM*)

Each function implements one step of the pipeline:
  1. close_market_eod:        ASTADRVT_TRADE_MVMT        → ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
  2. reconcile_d1_positions:  ASTANO_FGBE_DRVT_PSTN      → ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
                              ASTACASH_MRKT_PSTN          → ADWPM_POSICAO_MERCADO_A_VISTA
  3. quality_gate_d1:         Runs DQ checks on ADWPM* tables → ADWPM_DQ_RESULTS
  4. publish_d1_reports:      ADWPM* → MinIO (CSV/Parquet exports)

Fault injection for the 3 DQ cases is built into the data generation.
"""

import hashlib
import io
import json
import logging
import os
import random
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from ddtrace import tracer

logger = logging.getLogger("controlm-sim.etl")

# ── Database connection helpers ───────────────────────────────────────────────


def _compact_sql(sql):
    return " ".join(str(sql).split())[:500]


@contextmanager
def _client_span(name, service, resource, span_type="custom", **tags):
    with tracer.trace(name, service=service, resource=resource, span_type=span_type) as span:
        span.set_tag("span.kind", "client")
        span.set_tag("env", os.environ.get("DD_ENV", "demo"))
        span.set_tag("team", "data-platform")
        span.set_tag("domain", "capital-markets")
        for key, value in tags.items():
            if value is not None:
                span.set_tag(key, value)
        yield span


def _db_service(system):
    return "demo-oracle" if system == "oracle" else "demo-sqlserver"


def _db_name(system):
    if system == "oracle":
        return os.environ.get("ORACLE_SERVICE", "XEPDB1")
    return os.environ.get("SQLSERVER_DATABASE", "demopoc")


@contextmanager
def _db_span(system, operation, table, statement=None, rows=None):
    service = _db_service(system)
    resource = f"{operation} {table}"
    with _client_span(
        f"{system}.query",
        service=service,
        resource=resource,
        span_type="sql",
        component=system,
        **{
            "db.system": system,
            "db.name": _db_name(system),
            "db.operation": operation,
            "db.table": table,
            "db.statement": _compact_sql(statement) if statement else None,
            "db.rows": rows,
            "peer.service": service,
        },
    ) as span:
        yield span


def _db_execute(cursor, system, operation, table, statement, params=None):
    with _db_span(system, operation, table, statement) as span:
        if params is None:
            cursor.execute(statement)
        else:
            cursor.execute(statement, params)
        rowcount = getattr(cursor, "rowcount", None)
        if rowcount not in (None, -1):
            span.set_tag("db.rows_affected", rowcount)


def _db_fetchall(cursor, system, operation, table, statement, params=None):
    with _db_span(system, operation, table, statement) as span:
        if params is None:
            cursor.execute(statement)
        else:
            cursor.execute(statement, params)
        rows = cursor.fetchall()
        span.set_tag("db.rows_returned", len(rows))
        return rows


def _db_fetchone(cursor, system, operation, table, statement, params=None):
    with _db_span(system, operation, table, statement) as span:
        if params is None:
            cursor.execute(statement)
        else:
            cursor.execute(statement, params)
        row = cursor.fetchone()
        span.set_tag("db.rows_returned", 1 if row is not None else 0)
        return row


def _get_oracle_connection():
    """Connect to Oracle source database."""
    import oracledb
    dsn = oracledb.makedsn(
        os.environ.get("ORACLE_HOST", "oracle"),
        int(os.environ.get("ORACLE_PORT", "1521")),
        service_name=os.environ.get("ORACLE_SERVICE", "XEPDB1"),
    )
    with _client_span(
        "oracle.connect",
        service="demo-oracle",
        resource=os.environ.get("ORACLE_SERVICE", "XEPDB1"),
        span_type="sql",
        component="oracle",
        **{
            "db.system": "oracle",
            "db.name": os.environ.get("ORACLE_SERVICE", "XEPDB1"),
            "network.destination.name": os.environ.get("ORACLE_HOST", "oracle"),
            "peer.service": "demo-oracle",
        },
    ):
        return oracledb.connect(
            user=os.environ.get("ORACLE_USER", "demopoc"),
            password=os.environ.get("ORACLE_PASSWORD", "OracleDemo123!"),
            dsn=dsn,
        )


def _get_sqlserver_connection():
    """Connect to SQL Server destination DW."""
    import pymssql
    with _client_span(
        "sqlserver.connect",
        service="demo-sqlserver",
        resource=os.environ.get("SQLSERVER_DATABASE", "demopoc"),
        span_type="sql",
        component="sqlserver",
        **{
            "db.system": "sqlserver",
            "db.name": os.environ.get("SQLSERVER_DATABASE", "demopoc"),
            "network.destination.name": os.environ.get("SQLSERVER_HOST", "sqlserver"),
            "peer.service": "demo-sqlserver",
        },
    ):
        return pymssql.connect(
            server=os.environ.get("SQLSERVER_HOST", "sqlserver"),
            port=int(os.environ.get("SQLSERVER_PORT", "1433")),
            user=os.environ.get("SQLSERVER_USER", "sa"),
            password=os.environ.get("SQLSERVER_PASSWORD", "SqlDemo12345!"),
            database=os.environ.get("SQLSERVER_DATABASE", "demopoc"),
        )


def _row_hash(*values):
    """SHA-256 hash of business key columns for dedup detection."""
    raw = "|".join(str(v) for v in values)
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Seed Oracle data (generates fresh data for a business date) ───────────────


def seed_oracle_data(business_date, inject_fault=None):
    """
    Seed Oracle ASTA* tables with fresh market data for the given business_date.
    Optionally inject DQ faults for demo purposes.

    Fault types:
      - duplicate_trades:     Inserts 4 duplicate trade_ids (Caso 1)
      - null_settlement_price: Leaves settlement_price NULL for some positions (Caso 2)
      - zero_sum_positions:   Sets long_value + short_value = 0 for some rows (Caso 3)
    """
    conn = _get_oracle_connection()
    cursor = conn.cursor()

    # Tickers and participants used across all tables
    tickers = ["WINFUT", "INDFUT", "DOLFUT", "PETR4", "VALE3", "ITUB4", "BBDC4", "B3SA3"]
    participants = [f"PART{str(i).zfill(3)}" for i in range(1, 11)]
    desks = ["MESA01", "MESA02", "MESA03", "MESA04", "MESA05"]

    try:
        # Clean existing data for this date
        with _db_span("oracle", "DELETE", "ASTA_MARKET_DATA", rows=3):
            for table in ["ASTADRVT_TRADE_MVMT", "ASTANO_FGBE_DRVT_PSTN", "ASTACASH_MRKT_PSTN"]:
                cursor.execute(f"DELETE FROM {table} WHERE BUSINESS_DATE = TO_DATE(:bd, 'YYYY-MM-DD')", {"bd": business_date})

        # ── 1. ASTADRVT_TRADE_MVMT (500 trades) ──────────────────────────────
        trade_sql = """INSERT INTO ASTADRVT_TRADE_MVMT
                   (TRADE_ID, TRADE_DT, INSTRUMENT_CODE, BUY_PARTICIPANT, SELL_PARTICIPANT,
                    QUANTITY, CLOSING_PRICE, GROSS_VALUE, SETTLEMENT_DT, STATUS, BUSINESS_DATE)
                   VALUES (:tid, TO_DATE(:bd,'YYYY-MM-DD'), :ticker, :buyer, :seller,
                           :qty, :price, :gross, TO_DATE(:bd,'YYYY-MM-DD')+2, 'OPEN', TO_DATE(:bd,'YYYY-MM-DD'))"""

        trade_ids = []
        with _db_span("oracle", "INSERT", "ASTADRVT_TRADE_MVMT", trade_sql, rows=500) as span:
            for i in range(500):
                trade_id = f"TRD-{business_date.replace('-', '')}-{str(i+1).zfill(5)}"
                trade_ids.append(trade_id)
                ticker = random.choice(tickers)
                qty = random.randint(100, 10000)
                price = round(random.uniform(10.0, 500.0), 2)
                cursor.execute(trade_sql, {
                    "tid": trade_id, "bd": business_date, "ticker": ticker,
                    "buyer": random.choice(participants), "seller": random.choice(participants),
                    "qty": qty, "price": price, "gross": round(qty * price, 2),
                })
            span.set_tag("db.rows_inserted", 500)

        # Caso 1: Inject 4 duplicate trade_ids (different PK → disable PK first, or use different approach)
        if inject_fault in ("duplicate_trades", "all"):
            # Drop PK to allow duplicates, then re-add after
            try:
                cursor.execute("ALTER TABLE ASTADRVT_TRADE_MVMT DROP CONSTRAINT pk_astadrvt_trade")
            except Exception:
                pass  # PK may not exist
            for dup_id in random.sample(trade_ids[:100], 4):
                ticker = random.choice(tickers)
                qty = random.randint(100, 5000)
                price = round(random.uniform(10.0, 300.0), 2)
                cursor.execute(trade_sql, {
                    "tid": dup_id, "bd": business_date, "ticker": ticker,
                    "buyer": random.choice(participants), "seller": random.choice(participants),
                    "qty": qty, "price": price, "gross": round(qty * price, 2),
                })
            logger.warning("[FAULT] Injected 4 duplicate trade_ids in ASTADRVT_TRADE_MVMT")

        # ── 2. ASTANO_FGBE_DRVT_PSTN (50 positions) ─────────────────────────
        pos_sql = """INSERT INTO ASTANO_FGBE_DRVT_PSTN
                   (POSITION_ID, POSITION_DATE, PARTICIPANT_CODE, INSTRUMENT_CODE,
                    NET_QUANTITY, LONG_QUANTITY, SHORT_QUANTITY, MARKET_VALUE, BUSINESS_DATE)
                   VALUES (:pid, TO_DATE(:bd,'YYYY-MM-DD'), :part, :instr,
                           :net_qty, :long_qty, :short_qty, :mktval, TO_DATE(:bd,'YYYY-MM-DD'))"""

        with _db_span("oracle", "INSERT", "ASTANO_FGBE_DRVT_PSTN", pos_sql, rows=50) as span:
            for i in range(50):
                pos_id = f"POS-DRVT-{business_date.replace('-', '')}-{str(i+1).zfill(4)}"
                long_qty = random.randint(0, 1000)
                short_qty = random.randint(0, 500)
                settlement_price = round(random.uniform(10.0, 500.0), 6)

                # Caso 2: Null settlement_price for some rows
                if inject_fault in ("null_settlement_price", "all") and i < 8:
                    settlement_price = None

                cursor.execute(pos_sql, {
                    "pid": pos_id, "bd": business_date,
                    "part": random.choice(participants), "instr": random.choice(tickers),
                    "net_qty": long_qty - short_qty, "long_qty": long_qty,
                    "short_qty": short_qty, "mktval": settlement_price,
                })
            span.set_tag("db.rows_inserted", 50)

        if inject_fault in ("null_settlement_price", "all"):
            logger.warning("[FAULT] Injected 8 rows with NULL settlement_price in ASTANO_FGBE_DRVT_PSTN")

        # ── 3. ASTACASH_MRKT_PSTN (40 positions) ─────────────────────────────
        cash_sql = """INSERT INTO ASTACASH_MRKT_PSTN
                   (POSITION_ID, POSITION_DATE, PARTICIPANT_CODE, INSTRUMENT_CODE,
                    NET_QUANTITY, SETTLEMENT_VALUE, BUSINESS_DATE)
                   VALUES (:pid, TO_DATE(:bd,'YYYY-MM-DD'), :part, :instr,
                           :net_qty, :sval, TO_DATE(:bd,'YYYY-MM-DD'))"""

        with _db_span("oracle", "INSERT", "ASTACASH_MRKT_PSTN", cash_sql, rows=40) as span:
            for i in range(40):
                pos_id = f"POS-CASH-{business_date.replace('-', '')}-{str(i+1).zfill(4)}"
                long_val = round(random.uniform(10000.0, 5000000.0), 4)
                short_val = round(random.uniform(-5000000.0, -10000.0), 4)

                # Caso 3: Zero-sum positions (long_value + short_value = 0)
                if inject_fault in ("zero_sum_positions", "all") and i < 6:
                    short_val = -long_val  # Exact zero sum

                net_val = round(long_val + short_val, 4)
                cursor.execute(cash_sql, {
                    "pid": pos_id, "bd": business_date,
                    "part": random.choice(participants), "instr": random.choice(tickers),
                    "net_qty": random.randint(100, 5000), "sval": net_val,
                })
            span.set_tag("db.rows_inserted", 40)

        if inject_fault in ("zero_sum_positions", "all"):
            logger.warning("[FAULT] Injected 6 zero-sum positions in ASTACASH_MRKT_PSTN")

        with _db_span("oracle", "COMMIT", "ASTA_MARKET_DATA"):
            conn.commit()
        logger.info("Seeded Oracle data for %s (fault=%s)", business_date, inject_fault)

        return {
            "trades": 500 + (4 if inject_fault in ("duplicate_trades", "all") else 0),
            "derivative_positions": 50,
            "cash_positions": 40,
            "fault_injected": inject_fault,
        }

    finally:
        cursor.close()
        conn.close()


# ── ETL Job 1: close_market_eod ──────────────────────────────────────────────


def run_close_market_eod(business_date):
    """
    ETL: Oracle ASTADRVT_TRADE_MVMT → SQL Server ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO

    Reads all trades for the business_date from Oracle and loads them into
    the SQL Server DW with audit columns (dw_load_dt, dw_source, dw_row_hash).
    """
    # Fault: simulate Oracle connection timeout (persists across retries)
    fault_count = int(os.environ.get("_FAULT_ORACLE_TIMEOUT", "0"))
    if fault_count > 0:
        os.environ["_FAULT_ORACLE_TIMEOUT"] = str(fault_count - 1)
        time.sleep(2)
        raise ConnectionError("ORA-12170: TNS:Connect timeout — Oracle source unreachable after 30s")

    ora_conn = _get_oracle_connection()
    sql_conn = _get_sqlserver_connection()
    rows_loaded = 0

    try:
        ora_cursor = ora_conn.cursor()
        trade_rows = _db_fetchall(
            ora_cursor,
            "oracle",
            "SELECT",
            "ASTADRVT_TRADE_MVMT",
            """SELECT TRADE_ID, INSTRUMENT_CODE, CLOSING_PRICE * QUANTITY, QUANTITY,
                      TRADE_DT, SETTLEMENT_DT, BUY_PARTICIPANT, SELL_PARTICIPANT,
                      SUBSTR(INSTRUMENT_CODE, 1, 5), STATUS
               FROM ASTADRVT_TRADE_MVMT
               WHERE BUSINESS_DATE = TO_DATE(:bd, 'YYYY-MM-DD')""",
            {"bd": business_date},
        )

        sql_cursor = sql_conn.cursor()
        # Clear existing data for this date to allow re-runs
        _db_execute(
            sql_cursor,
            "sqlserver",
            "DELETE",
            "ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
            "DELETE FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO WHERE trade_dt = %s",
            (business_date,),
        )

        batch = []
        for row in trade_rows:
            trade_id, ticker, notional, qty, trade_dt, settle_dt, cpty, trader, desk, status = row
            row_hash = _row_hash(trade_id, business_date)
            batch.append((
                str(trade_id), str(ticker), float(notional or 0), int(qty),
                business_date, str(settle_dt) if settle_dt else None,
                str(cpty) if cpty else None, str(trader) if trader else None,
                str(desk) if desk else None, str(status),
                "oracle", row_hash,
            ))

        insert_sql = """INSERT INTO dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
                   (trade_id, ticker, notional, quantity, trade_dt, settlement_dt,
                    counterparty_id, trader_id, desk_code, status, dw_source, dw_row_hash)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        with _db_span("sqlserver", "INSERT", "ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO", insert_sql, rows=len(batch)) as span:
            for rec in batch:
                sql_cursor.execute(insert_sql, rec)
                rows_loaded += 1
            span.set_tag("db.rows_inserted", rows_loaded)

        with _db_span("sqlserver", "COMMIT", "ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO"):
            sql_conn.commit()
        logger.info("close_market_eod: loaded %d rows Oracle→SQL Server", rows_loaded)

        return {"status": "success", "rows_loaded": rows_loaded}

    finally:
        ora_conn.close()
        sql_conn.close()


# ── ETL Job 2: reconcile_d1_positions ─────────────────────────────────────────


def run_reconcile_d1_positions(business_date):
    """
    ETL: Oracle ASTANO_FGBE_DRVT_PSTN  → SQL Server ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
         Oracle ASTACASH_MRKT_PSTN      → SQL Server ADWPM_POSICAO_MERCADO_A_VISTA
    """
    ora_conn = _get_oracle_connection()
    sql_conn = _get_sqlserver_connection()
    drvt_loaded = 0
    cash_loaded = 0

    try:
        ora_cursor = ora_conn.cursor()
        sql_cursor = sql_conn.cursor()

        # ── Part A: ASTANO_FGBE_DRVT_PSTN → ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL ──
        _db_execute(
            sql_cursor,
            "sqlserver",
            "DELETE",
            "ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
            "DELETE FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL WHERE position_date = %s",
            (business_date,),
        )

        derivative_rows = _db_fetchall(
            ora_cursor,
            "oracle",
            "SELECT",
            "ASTANO_FGBE_DRVT_PSTN",
            """SELECT POSITION_ID, INSTRUMENT_CODE, PARTICIPANT_CODE, POSITION_DATE,
                      LONG_QUANTITY, SHORT_QUANTITY, NET_QUANTITY, MARKET_VALUE, MARKET_VALUE
               FROM ASTANO_FGBE_DRVT_PSTN
               WHERE BUSINESS_DATE = TO_DATE(:bd, 'YYYY-MM-DD')""",
            {"bd": business_date},
        )

        derivative_insert_sql = """INSERT INTO dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
                   (position_id, instrument_id, participant_code, position_date,
                    long_qty, short_qty, net_qty, settlement_price, margin_value,
                    dw_source, dw_row_hash)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        with _db_span(
            "sqlserver",
            "INSERT",
            "ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
            derivative_insert_sql,
            rows=len(derivative_rows),
        ) as span:
            for row in derivative_rows:
                pos_id, instr, part, pos_dt, long_q, short_q, net_q, settle_price, margin = row
                row_hash = _row_hash(pos_id, business_date)
                sql_cursor.execute(
                    derivative_insert_sql,
                    (str(pos_id), str(instr), str(part), business_date,
                     float(long_q or 0), float(short_q or 0), float(net_q or 0),
                     float(settle_price) if settle_price is not None else None,
                     float(margin) if margin is not None else None,
                     "oracle", row_hash)
                )
                drvt_loaded += 1
            span.set_tag("db.rows_inserted", drvt_loaded)

        # ── Part B: ASTACASH_MRKT_PSTN → ADWPM_POSICAO_MERCADO_A_VISTA ──────
        _db_execute(
            sql_cursor,
            "sqlserver",
            "DELETE",
            "ADWPM_POSICAO_MERCADO_A_VISTA",
            "DELETE FROM dbo.ADWPM_POSICAO_MERCADO_A_VISTA WHERE position_date = %s",
            (business_date,),
        )

        cash_rows = _db_fetchall(
            ora_cursor,
            "oracle",
            "SELECT",
            "ASTACASH_MRKT_PSTN",
            """SELECT POSITION_ID, INSTRUMENT_CODE, PARTICIPANT_CODE, POSITION_DATE,
                      SETTLEMENT_VALUE, NET_QUANTITY
               FROM ASTACASH_MRKT_PSTN
               WHERE BUSINESS_DATE = TO_DATE(:bd, 'YYYY-MM-DD')""",
            {"bd": business_date},
        )

        cash_insert_sql = """INSERT INTO dbo.ADWPM_POSICAO_MERCADO_A_VISTA
                   (position_id, ticker, participant_code, position_date,
                    long_value, short_value, net_value, dw_source, dw_row_hash)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        with _db_span(
            "sqlserver",
            "INSERT",
            "ADWPM_POSICAO_MERCADO_A_VISTA",
            cash_insert_sql,
            rows=len(cash_rows),
        ) as span:
            for row in cash_rows:
                pos_id, ticker, part, pos_dt, settle_val, net_qty = row
                # For cash positions, derive long/short from settlement_value sign
                settle_val = float(settle_val or 0)
                if settle_val >= 0:
                    long_val = settle_val
                    short_val = 0.0
                else:
                    long_val = 0.0
                    short_val = settle_val
                net_val = long_val + short_val
                row_hash = _row_hash(pos_id, business_date)

                sql_cursor.execute(
                    cash_insert_sql,
                    (str(pos_id), str(ticker), str(part), business_date,
                     long_val, short_val, net_val, "oracle", row_hash)
                )
                cash_loaded += 1
            span.set_tag("db.rows_inserted", cash_loaded)

        with _db_span("sqlserver", "COMMIT", "ADWPM_POSICAO_*"):
            sql_conn.commit()
        logger.info(
            "reconcile_d1_positions: loaded %d derivatives + %d cash positions Oracle→SQL Server",
            drvt_loaded, cash_loaded,
        )

        return {
            "status": "success",
            "derivative_positions_loaded": drvt_loaded,
            "cash_positions_loaded": cash_loaded,
            "rows_loaded": drvt_loaded + cash_loaded,
        }

    finally:
        ora_conn.close()
        sql_conn.close()


# ── ETL Job 3: quality_gate_d1 ───────────────────────────────────────────────


def run_quality_gate_d1(business_date):
    """
    Run 5 DQ checks on SQL Server ADWPM* tables and write results to ADWPM_DQ_RESULTS.

    Checks:
      1. Caso 1 — Duplicate trade_ids in ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
      2. Caso 2 — NULL settlement_price in ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
      3. Caso 3 — Zero-sum (long_value + short_value = 0) in ADWPM_POSICAO_MERCADO_A_VISTA
      4. Row count: Oracle source vs SQL Server destination for trades
      5. Row count: Oracle source vs SQL Server destination for positions
    """
    sql_conn = _get_sqlserver_connection()
    ora_conn = _get_oracle_connection()
    run_id = str(uuid.uuid4())
    results = []

    try:
        sql_cur = sql_conn.cursor()
        ora_cur = ora_conn.cursor()

        # ── Check 1: Duplicates (Caso 1) ─────────────────────────────────────
        dup_row = _db_fetchone(sql_cur, "sqlserver", "DQ_CHECK", "ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO", """
            SELECT COALESCE(SUM(cnt - 1), 0)
            FROM (
                SELECT trade_id, COUNT(*) AS cnt
                FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
                WHERE trade_dt = %s
                GROUP BY trade_id
                HAVING COUNT(*) > 1
            ) dup
        """, (business_date,))
        dup_count = dup_row[0] or 0
        results.append({
            "check_name": "caso1_duplicate_trades",
            "check_type": "uniqueness",
            "target_table": "ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
            "severity": "critical",
            "passed": dup_count == 0,
            "expected_value": "0",
            "actual_value": str(dup_count),
            "details": f"{dup_count} duplicate trade_id rows found" if dup_count > 0 else "No duplicates",
        })

        # ── Check 2: Null settlement_price (Caso 2) ──────────────────────────
        null_row = _db_fetchone(sql_cur, "sqlserver", "DQ_CHECK", "ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL", """
            SELECT COUNT(*)
            FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
            WHERE position_date = %s AND settlement_price IS NULL
        """, (business_date,))
        null_count = null_row[0] or 0
        results.append({
            "check_name": "caso2_null_settlement_price",
            "check_type": "completeness",
            "target_table": "ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
            "severity": "critical",
            "passed": null_count == 0,
            "expected_value": "0",
            "actual_value": str(null_count),
            "details": f"{null_count} rows with NULL settlement_price" if null_count > 0 else "All prices populated",
        })

        # ── Check 3: Zero-sum positions (Caso 3) ─────────────────────────────
        zero_row = _db_fetchone(sql_cur, "sqlserver", "DQ_CHECK", "ADWPM_POSICAO_MERCADO_A_VISTA", """
            SELECT COUNT(*)
            FROM dbo.ADWPM_POSICAO_MERCADO_A_VISTA
            WHERE position_date = %s AND (long_value + short_value) = 0
        """, (business_date,))
        zero_count = zero_row[0] or 0
        results.append({
            "check_name": "caso3_zero_sum_positions",
            "check_type": "consistency",
            "target_table": "ADWPM_POSICAO_MERCADO_A_VISTA",
            "severity": "critical",
            "passed": zero_count == 0,
            "expected_value": "0",
            "actual_value": str(zero_count),
            "details": f"{zero_count} positions with long_value + short_value = 0" if zero_count > 0 else "No zero-sum positions",
        })

        # ── Check 4: Row count comparison — trades ────────────────────────────
        oracle_trade_row = _db_fetchone(
            ora_cur,
            "oracle",
            "ROWCOUNT",
            "ASTADRVT_TRADE_MVMT",
            "SELECT COUNT(*) FROM ASTADRVT_TRADE_MVMT WHERE BUSINESS_DATE = TO_DATE(:bd, 'YYYY-MM-DD')",
            {"bd": business_date},
        )
        oracle_trade_count = oracle_trade_row[0] or 0

        dw_trade_row = _db_fetchone(
            sql_cur,
            "sqlserver",
            "ROWCOUNT",
            "ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
            "SELECT COUNT(*) FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO WHERE trade_dt = %s",
            (business_date,),
        )
        dw_trade_count = dw_trade_row[0] or 0

        results.append({
            "check_name": "rowcount_trades_oracle_vs_dw",
            "check_type": "completeness",
            "target_table": "ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
            "severity": "warning",
            "passed": oracle_trade_count == dw_trade_count,
            "expected_value": str(oracle_trade_count),
            "actual_value": str(dw_trade_count),
            "details": f"Oracle={oracle_trade_count}, DW={dw_trade_count}",
        })

        # ── Check 5: Row count comparison — derivative positions ──────────────
        oracle_pos_row = _db_fetchone(
            ora_cur,
            "oracle",
            "ROWCOUNT",
            "ASTANO_FGBE_DRVT_PSTN",
            "SELECT COUNT(*) FROM ASTANO_FGBE_DRVT_PSTN WHERE BUSINESS_DATE = TO_DATE(:bd, 'YYYY-MM-DD')",
            {"bd": business_date},
        )
        oracle_pos_count = oracle_pos_row[0] or 0

        dw_pos_row = _db_fetchone(
            sql_cur,
            "sqlserver",
            "ROWCOUNT",
            "ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
            "SELECT COUNT(*) FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL WHERE position_date = %s",
            (business_date,),
        )
        dw_pos_count = dw_pos_row[0] or 0

        results.append({
            "check_name": "rowcount_positions_oracle_vs_dw",
            "check_type": "completeness",
            "target_table": "ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
            "severity": "warning",
            "passed": oracle_pos_count == dw_pos_count,
            "expected_value": str(oracle_pos_count),
            "actual_value": str(dw_pos_count),
            "details": f"Oracle={oracle_pos_count}, DW={dw_pos_count}",
        })

        # ── Write results to ADWPM_DQ_RESULTS ─────────────────────────────────
        _db_execute(
            sql_cur,
            "sqlserver",
            "DELETE",
            "ADWPM_DQ_RESULTS",
            "DELETE FROM dbo.ADWPM_DQ_RESULTS WHERE business_date = %s",
            (business_date,),
        )

        dq_insert_sql = """INSERT INTO dbo.ADWPM_DQ_RESULTS
	                   (run_id, business_date, check_name, check_type, target_table,
	                    severity, passed, expected_value, actual_value, details)
	                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        with _db_span("sqlserver", "INSERT", "ADWPM_DQ_RESULTS", dq_insert_sql, rows=len(results)) as span:
            for r in results:
                sql_cur.execute(
                    dq_insert_sql,
                    (run_id, business_date, r["check_name"], r["check_type"],
                     r["target_table"], r["severity"],
                     1 if r["passed"] else 0,
                     r["expected_value"], r["actual_value"], r["details"])
                )
            span.set_tag("db.rows_inserted", len(results))

        with _db_span("sqlserver", "COMMIT", "ADWPM_DQ_RESULTS"):
            sql_conn.commit()

        checks_failed = sum(1 for r in results if not r["passed"])
        critical_failures = sum(1 for r in results if not r["passed"] and r["severity"] == "critical")

        gate_status = "PASS" if critical_failures == 0 else "FAIL"

        logger.info(
            "quality_gate_d1: %d checks, %d failed (%d critical) → %s",
            len(results), checks_failed, critical_failures, gate_status,
        )

        # If gate_fail_hard env var is set, raise exception on critical failures
        # This makes the job ENDED NOT OK instead of just reporting FAIL
        if critical_failures > 0 and os.environ.get("DQ_GATE_FAIL_HARD", "").lower() in ("1", "true", "yes"):
            raise RuntimeError(
                f"Quality gate HARD FAIL: {critical_failures} critical failures detected. "
                f"Checks: {', '.join(r['check_name'] for r in results if not r['passed'] and r['severity'] == 'critical')}"
            )

        return {
            "status": "success",
            "gate_status": gate_status,
            "checks_run": len(results),
            "checks_failed": checks_failed,
            "critical_failures": critical_failures,
            "run_id": run_id,
            "results": results,
        }

    finally:
        sql_conn.close()
        ora_conn.close()


# ── ETL Job 4: publish_d1_reports ─────────────────────────────────────────────


def run_publish_d1_reports(business_date):
    """
    Export ADWPM* tables to MinIO (S3-compatible) as CSV files.
    """
    # Fault: simulate S3/MinIO connection failure (persists across retries)
    fault_count = int(os.environ.get("_FAULT_S3_DOWN", "0"))
    if fault_count > 0:
        os.environ["_FAULT_S3_DOWN"] = str(fault_count - 1)
        time.sleep(2)
        raise ConnectionError("S3 PutObject failed: Connection refused — MinIO endpoint unreachable")

    import csv

    sql_conn = _get_sqlserver_connection()
    files_exported = []

    try:
        sql_cur = sql_conn.cursor()

        exports = [
            {
                "query": "SELECT * FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO WHERE trade_dt = %s",
                "params": (business_date,),
                "table": "ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
                "filename": f"derivatives/ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO_{business_date}.csv",
            },
            {
                "query": "SELECT * FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL WHERE position_date = %s",
                "params": (business_date,),
                "table": "ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
                "filename": f"derivatives/ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL_{business_date}.csv",
            },
            {
                "query": "SELECT * FROM dbo.ADWPM_POSICAO_MERCADO_A_VISTA WHERE position_date = %s",
                "params": (business_date,),
                "table": "ADWPM_POSICAO_MERCADO_A_VISTA",
                "filename": f"spot/ADWPM_POSICAO_MERCADO_A_VISTA_{business_date}.csv",
            },
        ]

        # MinIO upload
        try:
            from minio import Minio
            bucket = "mock-exchange"
            with _client_span(
                "s3.connect",
                service="mock-exchange",
                resource=os.environ.get("MINIO_ENDPOINT", "minio:9000"),
                span_type="http",
                component="minio",
                **{
                    "aws.s3.bucket": bucket,
                    "network.destination.name": os.environ.get("MINIO_ENDPOINT", "minio:9000"),
                    "peer.service": "mock-exchange",
                },
            ):
                minio_client = Minio(
                    os.environ.get("MINIO_ENDPOINT", "minio:9000"),
                    access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
                    secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
                    secure=False,
                )
                if not minio_client.bucket_exists(bucket):
                    minio_client.make_bucket(bucket)
        except Exception as exc:
            logger.warning("MinIO not available (%s), skipping S3 export", exc)
            minio_client = None

        total_rows = 0

        for export in exports:
            rows = _db_fetchall(
                sql_cur,
                "sqlserver",
                "SELECT",
                export["table"],
                export["query"],
                export["params"],
            )
            columns = [desc[0] for desc in sql_cur.description]
            total_rows += len(rows)

            if minio_client and rows:
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(columns)
                for row in rows:
                    writer.writerow([str(v) if v is not None else "" for v in row])

                data = buf.getvalue().encode("utf-8")
                with _client_span(
                    "s3.put_object",
                    service="mock-exchange",
                    resource=export["filename"],
                    span_type="http",
                    component="minio",
                    **{
                        "aws.s3.bucket": bucket,
                        "aws.s3.key": export["filename"],
                        "s3.rows": len(rows),
                        "s3.bytes": len(data),
                        "peer.service": "mock-exchange",
                    },
                ):
                    minio_client.put_object(
                        bucket, export["filename"],
                        io.BytesIO(data), len(data),
                        content_type="text/csv",
                    )
                files_exported.append(f"s3://{bucket}/{export['filename']}")
                logger.info("Exported %d rows to %s", len(rows), export["filename"])

        return {
            "status": "success",
            "files_exported": files_exported,
            "total_rows_exported": total_rows,
            "rows_loaded": total_rows,
        }

    finally:
        sql_conn.close()


# ── Database contention simulation ────────────────────────────────────────────


def simulate_blocking(duration_seconds=8):
    """Simulate a blocking query on SQL Server (holds exclusive lock on ADWPM_DQ_RESULTS).
    Creates a visible blocking event in Datadog DBM → Blocking Overview.
    """
    import threading

    def _blocker():
        conn = _get_sqlserver_connection()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN TRANSACTION")
            cur.execute("UPDATE TOP(1) dbo.ADWPM_DQ_RESULTS SET details = details")
            time.sleep(duration_seconds)
            cur.execute("COMMIT")
        except Exception as e:
            logger.warning("Blocker: %s", e)
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
        finally:
            conn.close()

    def _waiter():
        time.sleep(1)  # let blocker acquire lock first
        conn = _get_sqlserver_connection()
        cur = conn.cursor()
        try:
            cur.execute("SET LOCK_TIMEOUT 15000")  # wait up to 15s
            cur.execute("UPDATE TOP(1) dbo.ADWPM_DQ_RESULTS SET details = 'blocked_write_attempt'")
            conn.commit()
        except Exception as e:
            logger.warning("Waiter (expected): %s", e)
        finally:
            conn.close()

    t1 = threading.Thread(target=_blocker, daemon=True)
    t2 = threading.Thread(target=_waiter, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=duration_seconds + 5)
    t2.join(timeout=duration_seconds + 5)
    logger.info("Blocking simulation complete (%ds)", duration_seconds)


def simulate_deadlock():
    """Simulate a deadlock on SQL Server between two transactions.
    Creates a visible deadlock event in Datadog DBM → Deadlocks.
    """
    import threading

    def _tx_a():
        conn = _get_sqlserver_connection()
        cur = conn.cursor()
        try:
            cur.execute("SET DEADLOCK_PRIORITY LOW")
            cur.execute("BEGIN TRANSACTION")
            cur.execute("UPDATE TOP(1) dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO SET status = 'LOCK_A' WHERE trade_dt = CAST(GETDATE() AS DATE)")
            time.sleep(3)
            cur.execute("UPDATE TOP(1) dbo.ADWPM_POSICAO_MERCADO_A_VISTA SET net_value = net_value WHERE position_date = CAST(GETDATE() AS DATE)")
            cur.execute("COMMIT")
        except Exception as e:
            logger.info("Deadlock victim (expected): %s", str(e)[:80])
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
        finally:
            conn.close()

    def _tx_b():
        conn = _get_sqlserver_connection()
        cur = conn.cursor()
        try:
            time.sleep(0.5)  # slight delay so tx_a locks first
            cur.execute("SET DEADLOCK_PRIORITY HIGH")
            cur.execute("BEGIN TRANSACTION")
            cur.execute("UPDATE TOP(1) dbo.ADWPM_POSICAO_MERCADO_A_VISTA SET net_value = net_value WHERE position_date = CAST(GETDATE() AS DATE)")
            time.sleep(3)
            cur.execute("UPDATE TOP(1) dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO SET status = 'LOCK_B' WHERE trade_dt = CAST(GETDATE() AS DATE)")
            cur.execute("COMMIT")
        except Exception as e:
            logger.info("Deadlock winner or victim: %s", str(e)[:80])
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
        finally:
            conn.close()

    t1 = threading.Thread(target=_tx_a, daemon=True)
    t2 = threading.Thread(target=_tx_b, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=15)
    t2.join(timeout=15)
    logger.info("Deadlock simulation complete")


def simulate_long_query():
    """Simulate a slow/waiting query on SQL Server.
    Creates a visible waiting query in Datadog DBM → Waiting Queries.
    """
    conn = _get_sqlserver_connection()
    cur = conn.cursor()
    try:
        # Heavy cross-join that takes time and generates waits
        cur.execute("""
            SELECT COUNT(*) FROM
                dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO a
                CROSS JOIN dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL b
            WHERE a.trade_dt = CAST(GETDATE() AS DATE)
        """)
        cur.fetchone()
    except Exception as e:
        logger.warning("Long query: %s", str(e)[:80])
    finally:
        conn.close()
    logger.info("Long query simulation complete")


# ── Job dispatcher ────────────────────────────────────────────────────────────

JOB_FUNCTIONS = {
    "close_market_eod": run_close_market_eod,
    "reconcile_d1_positions": run_reconcile_d1_positions,
    "quality_gate_d1": run_quality_gate_d1,
    "publish_d1_reports": run_publish_d1_reports,
}


def run_job(job_name, business_date):
    """Dispatch a job by name. Returns result dict."""
    if job_name not in JOB_FUNCTIONS:
        raise KeyError(f"Unknown ETL job: {job_name}. Available: {list(JOB_FUNCTIONS.keys())}")

    # Inject DB contention faults if env vars are set
    if os.environ.get("_FAULT_DB_BLOCKING") == "1":
        os.environ.pop("_FAULT_DB_BLOCKING", None)
        import threading
        threading.Thread(target=simulate_blocking, kwargs={"duration_seconds": 10}, daemon=True).start()
        logger.warning("[FAULT] DB blocking simulation started (10s)")

    if os.environ.get("_FAULT_DB_DEADLOCK") == "1":
        os.environ.pop("_FAULT_DB_DEADLOCK", None)
        import threading
        threading.Thread(target=simulate_deadlock, daemon=True).start()
        logger.warning("[FAULT] DB deadlock simulation started")

    if os.environ.get("_FAULT_DB_SLOW_QUERY") == "1":
        os.environ.pop("_FAULT_DB_SLOW_QUERY", None)
        import threading
        threading.Thread(target=simulate_long_query, daemon=True).start()
        logger.warning("[FAULT] Slow query simulation started")

    return JOB_FUNCTIONS[job_name](business_date)
