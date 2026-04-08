"""
reconcile_d1_positions — ETL for the position tables.

Reads two Oracle source tables and loads into two SQL Server DW tables:

  ASTANO_FGBE_DRVT_PSTN  (Oracle)  →  ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL (SQL Server DW)
  ASTACASH_MRKT_PSTN      (Oracle)  →  ADWPM_POSICAO_MERCADO_A_VISTA         (SQL Server DW)

Caso 2: settlement_price may be NULL in source rows — these propagate into the DW
and are detected by quality_gate_d1 check 2.

Caso 3: rows where long_value + short_value = 0 in ASTACASH_MRKT_PSTN propagate
to ADWPM_POSICAO_MERCADO_A_VISTA and are detected by quality_gate_d1 check 3.

Falls back to MySQL (raw_participant_positions) if Oracle or SQL Server unavailable.
"""

import hashlib
import os
import uuid


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
    raw = "|".join(str(v) for v in values)
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Oracle readers ────────────────────────────────────────────────────────────

def _read_drvt_positions_from_oracle(business_date):
    """
    Read ASTANO_FGBE_DRVT_PSTN for position_date = business_date.
    Returns list of dicts. settlement_price may be None (Caso 2).
    """
    import oracledb

    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT position_id, instrument_id, participant_code,
                   position_date, long_qty, short_qty, net_qty,
                   settlement_price, margin_value
            FROM ASTANO_FGBE_DRVT_PSTN
            WHERE position_date = TO_DATE(:bdate, 'YYYY-MM-DD')
            """,
            bdate=business_date,
        )
        cols = [d[0].lower() for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def _read_cash_positions_from_oracle(business_date):
    """
    Read ASTACASH_MRKT_PSTN for position_date = business_date.
    Returns list of dicts. long_value + short_value = 0 indicates Caso 3 fault.
    """
    import oracledb

    dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT position_id, ticker, participant_code,
                   position_date, long_value, short_value, net_value
            FROM ASTACASH_MRKT_PSTN
            WHERE position_date = TO_DATE(:bdate, 'YYYY-MM-DD')
            """,
            bdate=business_date,
        )
        cols = [d[0].lower() for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


# ── MySQL fallback readers ────────────────────────────────────────────────────

def _get_mysql_conn():
    import mysql.connector
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def _read_drvt_positions_from_mysql(business_date):
    """
    Synthesise derivative position data from MySQL raw_participant_positions.
    Approximates ASTANO_FGBE_DRVT_PSTN shape; settlement_price set to NULL
    at 5% rate to partially simulate the source behaviour.
    """
    import random

    conn = _get_mysql_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT participant AS participant_code,
                   ticker      AS instrument_id,
                   business_date AS position_date,
                   GREATEST(net_quantity, 0) AS long_qty,
                   ABS(LEAST(net_quantity, 0)) AS short_qty,
                   net_quantity AS net_qty,
                   avg_price AS settlement_price
            FROM raw_participant_positions
            WHERE business_date = %s
            """,
            (business_date,),
        )
        rows = cur.fetchall()
        cur.close()
        # Ensure each row has a position_id
        for r in rows:
            r["position_id"] = str(uuid.uuid4())
            r["margin_value"] = None
            # Simulate 5% null settlement_price (mirrors upstream data quality issue)
            if random.random() < 0.05:
                r["settlement_price"] = None
        return rows
    finally:
        conn.close()


def _read_cash_positions_from_mysql(business_date):
    """
    Synthesise spot market position data from MySQL raw_participant_positions.
    Maps to ASTACASH_MRKT_PSTN shape.
    """
    conn = _get_mysql_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT participant AS participant_code,
                   ticker,
                   business_date AS position_date,
                   GREATEST(net_quantity * avg_price, 0) AS long_value,
                   LEAST(net_quantity * avg_price, 0)    AS short_value,
                   net_quantity * avg_price               AS net_value
            FROM raw_participant_positions
            WHERE business_date = %s
            """,
            (business_date,),
        )
        rows = cur.fetchall()
        cur.close()
        for r in rows:
            r["position_id"] = str(uuid.uuid4())
        return rows
    finally:
        conn.close()


# ── SQL Server writers ────────────────────────────────────────────────────────

def _write_drvt_positions(conn, rows, source, business_date):
    """
    Insert ASTANO_FGBE_DRVT_PSTN rows into ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL.
    Returns (rows_written, nulls_found).
    """
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL WHERE position_date = %s",
        (business_date,),
    )

    insert_sql = """
        INSERT INTO dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
            (position_id, instrument_id, participant_code, position_date,
             long_qty, short_qty, net_qty, settlement_price, margin_value,
             dw_source, dw_row_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows_written = 0
    nulls_found = 0
    for r in rows:
        pos_id      = r.get("position_id")  or str(uuid.uuid4())
        instr_id    = r.get("instrument_id") or r.get("INSTRUMENT_ID", "")
        part_code   = r.get("participant_code") or r.get("PARTICIPANT_CODE", "")
        pos_date    = str(r.get("position_date") or r.get("POSITION_DATE") or business_date)[:10]
        long_qty    = float(r.get("long_qty")  or r.get("LONG_QTY")  or 0)
        short_qty   = float(r.get("short_qty") or r.get("SHORT_QTY") or 0)
        net_qty     = float(r.get("net_qty")   or r.get("NET_QTY")   or 0)
        sett_price  = r.get("settlement_price") or r.get("SETTLEMENT_PRICE")
        margin_val  = r.get("margin_value") or r.get("MARGIN_VALUE")
        row_hash    = _row_hash(pos_id, instr_id, part_code, pos_date)

        if sett_price is None:
            nulls_found += 1

        try:
            cur.execute(insert_sql, (
                pos_id, instr_id, part_code, pos_date,
                long_qty, short_qty, net_qty, sett_price, margin_val,
                source, row_hash,
            ))
            rows_written += 1
        except Exception as exc:
            print(f"[reconcile] drvt insert error position_id={pos_id}: {exc}", flush=True)

    conn.commit()
    cur.close()
    return rows_written, nulls_found


def _write_cash_positions(conn, rows, source, business_date):
    """
    Insert ASTACASH_MRKT_PSTN rows into ADWPM_POSICAO_MERCADO_A_VISTA.
    Returns (rows_written, zero_sum_found).
    """
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM dbo.ADWPM_POSICAO_MERCADO_A_VISTA WHERE position_date = %s",
        (business_date,),
    )

    insert_sql = """
        INSERT INTO dbo.ADWPM_POSICAO_MERCADO_A_VISTA
            (position_id, ticker, participant_code, position_date,
             long_value, short_value, net_value,
             dw_source, dw_row_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows_written = 0
    zero_sum_found = 0
    for r in rows:
        pos_id    = r.get("position_id")    or str(uuid.uuid4())
        ticker    = r.get("ticker")         or r.get("TICKER", "")
        part_code = r.get("participant_code") or r.get("PARTICIPANT_CODE", "")
        pos_date  = str(r.get("position_date") or r.get("POSITION_DATE") or business_date)[:10]
        long_val  = float(r.get("long_value")  or r.get("LONG_VALUE")  or 0)
        short_val = float(r.get("short_value") or r.get("SHORT_VALUE") or 0)
        net_val   = float(r.get("net_value")   or r.get("NET_VALUE")   or 0)
        row_hash  = _row_hash(pos_id, ticker, part_code, pos_date)

        if (long_val + short_val) == 0:
            zero_sum_found += 1

        try:
            cur.execute(insert_sql, (
                pos_id, ticker, part_code, pos_date,
                long_val, short_val, net_val,
                source, row_hash,
            ))
            rows_written += 1
        except Exception as exc:
            print(f"[reconcile] cash insert error position_id={pos_id}: {exc}", flush=True)

    conn.commit()
    cur.close()
    return rows_written, zero_sum_found


# ── Main entry point ──────────────────────────────────────────────────────────

def run(conn, business_date):
    """
    Run D+1 position reconciliation ETL for the given business date.

    Args:
        conn: pymssql connection to SQL Server DW (demopoc database)
        business_date: str in YYYY-MM-DD format

    Returns:
        dict with status, rows written per table, nulls found, zero-sum found, source
    """
    source = "oracle"

    # ── ASTANO_FGBE_DRVT_PSTN → ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL ────
    drvt_rows = None
    try:
        drvt_rows = _read_drvt_positions_from_oracle(business_date)
        print(
            f"[reconcile] read {len(drvt_rows)} rows from Oracle "
            f"ASTANO_FGBE_DRVT_PSTN for {business_date}",
            flush=True,
        )
    except Exception as exc:
        print(
            f"[reconcile] Oracle unavailable for ASTANO_FGBE_DRVT_PSTN ({exc}), "
            "falling back to MySQL",
            flush=True,
        )
        source = "mysql_fallback"
        drvt_rows = _read_drvt_positions_from_mysql(business_date)
        print(
            f"[reconcile] read {len(drvt_rows)} rows from MySQL fallback "
            f"(derivative positions) for {business_date}",
            flush=True,
        )

    drvt_written, nulls_found = _write_drvt_positions(conn, drvt_rows, source, business_date)
    print(
        f"[reconcile] ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL: "
        f"written={drvt_written}, null_settlement_price={nulls_found}",
        flush=True,
    )

    # ── ASTACASH_MRKT_PSTN → ADWPM_POSICAO_MERCADO_A_VISTA ───────────────
    cash_rows = None
    try:
        cash_rows = _read_cash_positions_from_oracle(business_date)
        print(
            f"[reconcile] read {len(cash_rows)} rows from Oracle "
            f"ASTACASH_MRKT_PSTN for {business_date}",
            flush=True,
        )
    except Exception as exc:
        print(
            f"[reconcile] Oracle unavailable for ASTACASH_MRKT_PSTN ({exc}), "
            "falling back to MySQL",
            flush=True,
        )
        source = "mysql_fallback"
        cash_rows = _read_cash_positions_from_mysql(business_date)
        print(
            f"[reconcile] read {len(cash_rows)} rows from MySQL fallback "
            f"(cash positions) for {business_date}",
            flush=True,
        )

    cash_written, zero_sum_found = _write_cash_positions(conn, cash_rows, source, business_date)
    print(
        f"[reconcile] ADWPM_POSICAO_MERCADO_A_VISTA: "
        f"written={cash_written}, zero_sum_rows={zero_sum_found}",
        flush=True,
    )

    return {
        "status": "success",
        "source": source,
        "drvt_rows_read": len(drvt_rows),
        "drvt_rows_written": drvt_written,
        "nulls_found": nulls_found,
        "cash_rows_read": len(cash_rows),
        "cash_rows_written": cash_written,
        "zero_sum_found": zero_sum_found,
    }
