"""
close_market_eod — ETL: ASTADRVT_TRADE_MVMT (Oracle) → ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO (SQL Server DW).

Reads derivatives trade movement records from Oracle source table ASTADRVT_TRADE_MVMT
for the given business_date, deduplicates, and loads into the SQL Server DW table
ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO.

Caso 1 scenario: if 4 duplicate rows were injected into Oracle (via inject-fault
--fault-type duplicate_trade_mvmt), they survive into the DW and are detected
by quality_gate_d1 check 1.

Falls back to MySQL raw_trades if Oracle is unreachable — the pipeline continues
to work for demo purposes.  The OpenLineage events (run_job.py JOB_IO) always
declare Oracle as the upstream source regardless of fallback.
"""

import hashlib
import os


ORACLE_HOST    = os.environ.get("ORACLE_HOST",    "demoorg-oracle")
ORACLE_PORT    = int(os.environ.get("ORACLE_PORT", "1521"))
ORACLE_SERVICE = os.environ.get("ORACLE_SERVICE", "FREEPDB1")
ORACLE_USER    = os.environ.get("ORACLE_USER",    "system")
ORACLE_PASSWORD = os.environ.get("ORACLE_PASSWORD", "OracleDemo123!")

MYSQL_HOST     = os.environ.get("MYSQL_HOST",     "demo-mysql")
MYSQL_PORT     = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER     = os.environ.get("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "demopoc2026")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "exchange")


def _row_hash(*values):
    """SHA-256 of pipe-joined string representation of values."""
    raw = "|".join(str(v) for v in values)
    return hashlib.sha256(raw.encode()).hexdigest()


def _read_trades_from_oracle(business_date):
    """
    Read ASTADRVT_TRADE_MVMT from Oracle for business_date.
    Returns list of dicts with keys matching the DW columns.
    Raises on connection or query failure.
    """
    import oracledb

    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    ora_conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
    try:
        cur = ora_conn.cursor()
        cur.execute(
            """
            SELECT trade_id, ticker, notional, quantity,
                   trade_dt, settlement_dt, counterparty_id,
                   trader_id, desk_code, status
            FROM ASTADRVT_TRADE_MVMT
            WHERE trade_dt = TO_DATE(:bdate, 'YYYY-MM-DD')
            """,
            bdate=business_date,
        )
        cols = [d[0].lower() for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return rows
    finally:
        ora_conn.close()


def _read_trades_from_mysql(business_date):
    """
    Fallback: read from MySQL raw_trades for business_date.
    Maps MySQL columns to the same dict keys as the Oracle reader.
    """
    import mysql.connector

    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                trade_id,
                ticker,
                price * quantity        AS notional,
                quantity,
                business_date           AS trade_dt,
                NULL                    AS settlement_dt,
                seller_participant      AS counterparty_id,
                buyer_participant       AS trader_id,
                NULL                    AS desk_code,
                'EXECUTED'              AS status
            FROM raw_trades
            WHERE business_date = %s
            """,
            (business_date,),
        )
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


def run(conn, business_date):
    """
    ETL: ASTADRVT_TRADE_MVMT → ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO.

    Args:
        conn: pymssql connection to SQL Server DW (demopoc database)
        business_date: str in YYYY-MM-DD format

    Returns:
        dict {"source": str, "rows_read": int, "rows_written": int, "duplicates": int}
    """
    # ── 1. Read from Oracle; fall back to MySQL ────────────────────────────
    source = "oracle"
    try:
        trade_rows = _read_trades_from_oracle(business_date)
        print(
            f"[close_market_eod] read {len(trade_rows)} rows from Oracle "
            f"ASTADRVT_TRADE_MVMT for {business_date}",
            flush=True,
        )
    except Exception as exc:
        print(
            f"[close_market_eod] Oracle unavailable ({exc}), "
            "falling back to MySQL raw_trades",
            flush=True,
        )
        source = "mysql_fallback"
        trade_rows = _read_trades_from_mysql(business_date)
        print(
            f"[close_market_eod] read {len(trade_rows)} rows from MySQL raw_trades "
            f"for {business_date}",
            flush=True,
        )

    rows_read = len(trade_rows)

    # ── 2. Detect duplicates (Caso 1) — do NOT deduplicate before load ────
    # The ETL intentionally passes duplicates through so quality_gate catches them.
    seen_trade_ids = {}
    for r in trade_rows:
        tid = r.get("trade_id") or r.get("TRADE_ID")
        seen_trade_ids[tid] = seen_trade_ids.get(tid, 0) + 1
    duplicates_found = sum(cnt - 1 for cnt in seen_trade_ids.values() if cnt > 1)

    if duplicates_found:
        print(
            f"[close_market_eod] WARNING: {duplicates_found} duplicate trade_id(s) "
            f"detected in source — loading into DW as-is (quality_gate will catch)",
            flush=True,
        )

    # ── 3. Write to SQL Server ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO ─────────
    cur = conn.cursor()

    # Clear existing rows for this business_date before reload
    cur.execute(
        "DELETE FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO WHERE trade_dt = %s",
        (business_date,),
    )

    insert_sql = """
        INSERT INTO dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
            (trade_id, ticker, notional, quantity,
             trade_dt, settlement_dt, counterparty_id,
             trader_id, desk_code, status,
             dw_source, dw_row_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows_written = 0
    for r in trade_rows:
        trade_id     = r.get("trade_id") or r.get("TRADE_ID")
        ticker       = r.get("ticker")   or r.get("TICKER")
        notional     = float(r.get("notional") or r.get("NOTIONAL") or 0)
        quantity     = int(r.get("quantity")   or r.get("QUANTITY") or 0)
        trade_dt     = str(r.get("trade_dt")   or r.get("TRADE_DT") or business_date)[:10]
        settlement_dt = r.get("settlement_dt") or r.get("SETTLEMENT_DT")
        counterparty_id = r.get("counterparty_id") or r.get("COUNTERPARTY_ID")
        trader_id    = r.get("trader_id")   or r.get("TRADER_ID")
        desk_code    = r.get("desk_code")   or r.get("DESK_CODE")
        status       = r.get("status")      or r.get("STATUS") or "EXECUTED"
        row_hash     = _row_hash(trade_id, ticker, notional, quantity, trade_dt)

        try:
            cur.execute(insert_sql, (
                trade_id, ticker, notional, quantity,
                trade_dt, settlement_dt, counterparty_id,
                trader_id, desk_code, status,
                source, row_hash,
            ))
            rows_written += 1
        except Exception as exc:
            print(f"[close_market_eod] row insert error trade_id={trade_id}: {exc}", flush=True)

    conn.commit()
    cur.close()

    print(
        f"[close_market_eod] written {rows_written}/{rows_read} rows to "
        f"ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO; duplicates_in_source={duplicates_found}",
        flush=True,
    )

    return {
        "source": source,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "duplicates": duplicates_found,
    }
