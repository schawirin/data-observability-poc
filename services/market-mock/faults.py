"""
faults.py — Fault injection functions for Data Pipeline POC demo scenarios.

Each function accepts (mysql_conn, business_date) and injects the fault into
MySQL (always), Oracle (if reachable), and SQL Server (if reachable).

Real Exchange fault types:
  duplicate_trade_mvmt     — Caso 1: 4 duplicate rows in ASTADRVT_TRADE_MVMT + DW
  null_settlement_price    — Caso 2: settlement_price = NULL in ASTANO_FGBE_DRVT_PSTN
  zero_sum_position        — Caso 3: long_value + short_value = 0 in ASTACASH_MRKT_PSTN
  overflow                 — Overflow: value exceeds column precision
  row_count_diff           — Row count diff: delete 10% of Oracle rows after DW is loaded

Legacy MySQL-only faults (kept for backward compatibility):
  duplicate_trade_id       — original MySQL raw_trades duplicate
  null_closing_price       — original MySQL raw_close_prices null
  late_settlement_file     — original settlement date shift
  volume_spike             — original trade volume spike
  slow_reconciliation_query — original index drop
"""

import os
import random
import uuid


# ── Connection helpers ────────────────────────────────────────────────────────

def _get_oracle_conn():
    try:
        import oracledb
        host     = os.environ.get("ORACLE_HOST",    "demoorg-oracle")
        port     = int(os.environ.get("ORACLE_PORT", "1521"))
        service  = os.environ.get("ORACLE_SERVICE", "FREEPDB1")
        user     = os.environ.get("ORACLE_USER",    "system")
        password = os.environ.get("ORACLE_PASSWORD", "OracleDemo123!")
        return oracledb.connect(user=user, password=password, dsn=f"{host}:{port}/{service}")
    except Exception as exc:
        print(f"[faults] Oracle connection failed: {exc}", flush=True)
        return None


def _get_sqlserver_conn():
    try:
        import pymssql
        return pymssql.connect(
            server=os.environ.get("MSSQL_HOST", "demoorg-sqlserver"),
            port=int(os.environ.get("MSSQL_PORT", "1433")),
            user=os.environ.get("MSSQL_USER", "sa"),
            password=os.environ.get("MSSQL_SA_PASSWORD", "SqlDemo12345!"),
            database=os.environ.get("MSSQL_DATABASE", "demopoc"),
        )
    except Exception as exc:
        print(f"[faults] SQL Server connection failed: {exc}", flush=True)
        return None


# ── Real Exchange fault injectors ───────────────────────────────────────────────────

def duplicate_trade_mvmt(mysql_conn, business_date):
    """
    Caso 1: Inject 4 duplicate rows into ASTADRVT_TRADE_MVMT (Oracle) and
    ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO (SQL Server DW).

    The ETL (close_market_eod) does NOT deduplicate, so the duplicates survive
    into the DW and are caught by quality_gate_d1 check 1.
    Also injects into MySQL raw_trades for fallback-mode demos.
    """
    print(f"[faults] [Caso 1] injecting 4 duplicate trade rows for {business_date}", flush=True)

    # ── MySQL raw_trades (fallback layer) ──────────────────────────────────
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(
        """
        SELECT trade_id, order_id, ticker, trade_time, price, quantity,
               buyer_participant, seller_participant, business_date
        FROM raw_trades
        WHERE business_date = %s
        ORDER BY RAND()
        LIMIT 4
        """,
        (business_date,),
    )
    template_rows = mysql_cursor.fetchall()

    mysql_inserted = 0
    for row in template_rows:
        try:
            mysql_cursor.execute(
                """
                INSERT INTO raw_trades
                    (trade_id, order_id, ticker, trade_time, price, quantity,
                     buyer_participant, seller_participant, business_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row,  # same trade_id = duplicate
            )
            mysql_inserted += 1
        except Exception as e:
            print(f"[faults] MySQL duplicate skipped trade_id={row[0]}: {e}", flush=True)

    mysql_conn.commit()
    mysql_cursor.close()
    print(f"[faults] MySQL: duplicated {mysql_inserted} trades in raw_trades", flush=True)

    # ── Oracle ASTADRVT_TRADE_MVMT ─────────────────────────────────────────
    ora_conn = _get_oracle_conn()
    if ora_conn:
        try:
            ora_cur = ora_conn.cursor()
            ora_cur.execute(
                """
                SELECT trade_id, ticker, notional, quantity, trade_dt,
                       settlement_dt, counterparty_id, trader_id, desk_code, status
                FROM ASTADRVT_TRADE_MVMT
                WHERE trade_dt = TO_DATE(:bdate, 'YYYY-MM-DD')
                ORDER BY DBMS_RANDOM.VALUE
                FETCH FIRST 4 ROWS ONLY
                """,
                bdate=business_date,
            )
            dup_rows = ora_cur.fetchall()
            count = 0
            for row in dup_rows:
                try:
                    ora_cur.execute(
                        """
                        INSERT INTO ASTADRVT_TRADE_MVMT
                            (trade_id, ticker, notional, quantity, trade_dt,
                             settlement_dt, counterparty_id, trader_id, desk_code, status)
                        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)
                        """,
                        row,
                    )
                    count += 1
                except Exception as e:
                    print(f"[faults] Oracle duplicate skipped trade_id={row[0]}: {e}", flush=True)
            ora_conn.commit()
            ora_cur.close()
            print(f"[faults] Oracle: duplicated {count} rows in ASTADRVT_TRADE_MVMT", flush=True)
        finally:
            ora_conn.close()

    # ── SQL Server DW ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO ──────────────────
    ss_conn = _get_sqlserver_conn()
    if ss_conn:
        try:
            ss_cur = ss_conn.cursor(as_dict=True)
            ss_cur.execute(
                """
                SELECT TOP 4 trade_id, ticker, notional, quantity,
                       trade_dt, settlement_dt, counterparty_id,
                       trader_id, desk_code, status, dw_source
                FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
                WHERE trade_dt = %s
                ORDER BY NEWID()
                """,
                (business_date,),
            )
            dw_rows = ss_cur.fetchall()
            count = 0
            for r in dw_rows:
                try:
                    ss_cur.execute(
                        """
                        INSERT INTO dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
                            (trade_id, ticker, notional, quantity,
                             trade_dt, settlement_dt, counterparty_id,
                             trader_id, desk_code, status, dw_source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            r["trade_id"], r["ticker"], r["notional"], r["quantity"],
                            r["trade_dt"], r["settlement_dt"], r["counterparty_id"],
                            r["trader_id"], r["desk_code"], r["status"], r["dw_source"],
                        ),
                    )
                    count += 1
                except Exception as e:
                    print(f"[faults] SS DW duplicate skipped trade_id={r['trade_id']}: {e}", flush=True)
            ss_conn.commit()
            ss_cur.close()
            print(
                f"[faults] SQL Server DW: duplicated {count} rows in "
                f"ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
                flush=True,
            )
        finally:
            ss_conn.close()

    print(f"[faults] [Caso 1] duplicate_trade_mvmt injection complete for {business_date}", flush=True)


def null_settlement_price(mysql_conn, business_date):
    """
    Caso 2: Set settlement_price = NULL for ~5% of rows in ASTANO_FGBE_DRVT_PSTN (Oracle)
    and propagate to ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL (SQL Server DW).
    Also updates MySQL raw_participant_positions for fallback demos.
    """
    print(f"[faults] [Caso 2] injecting null settlement_price for {business_date}", flush=True)

    # ── Oracle ASTANO_FGBE_DRVT_PSTN ──────────────────────────────────────
    ora_conn = _get_oracle_conn()
    if ora_conn:
        try:
            ora_cur = ora_conn.cursor()
            # Count total rows for the date
            ora_cur.execute(
                "SELECT COUNT(*) FROM ASTANO_FGBE_DRVT_PSTN "
                "WHERE position_date = TO_DATE(:bdate, 'YYYY-MM-DD')",
                bdate=business_date,
            )
            total = ora_cur.fetchone()[0]
            target = max(1, int(total * 0.05))  # 5% or at least 1

            ora_cur.execute(
                """
                UPDATE ASTANO_FGBE_DRVT_PSTN
                SET settlement_price = NULL
                WHERE position_id IN (
                    SELECT position_id FROM (
                        SELECT position_id FROM ASTANO_FGBE_DRVT_PSTN
                        WHERE position_date = TO_DATE(:bdate, 'YYYY-MM-DD')
                        ORDER BY DBMS_RANDOM.VALUE
                        FETCH FIRST :n ROWS ONLY
                    )
                )
                """,
                bdate=business_date,
                n=target,
            )
            ora_conn.commit()
            print(
                f"[faults] Oracle: set settlement_price=NULL for {target} rows "
                f"in ASTANO_FGBE_DRVT_PSTN",
                flush=True,
            )
            ora_cur.close()
        finally:
            ora_conn.close()

    # ── SQL Server DW ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL ───────────────
    ss_conn = _get_sqlserver_conn()
    if ss_conn:
        try:
            ss_cur = ss_conn.cursor()
            # Count rows and null 5%
            ss_cur.execute(
                "SELECT COUNT(*) FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL "
                "WHERE position_date = %s",
                (business_date,),
            )
            total_dw = ss_cur.fetchone()[0]
            target_dw = max(1, int(total_dw * 0.05))

            ss_cur.execute(
                """
                UPDATE TOP (%s) dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
                SET settlement_price = NULL
                WHERE position_date = %s
                """,
                (target_dw, business_date),
            )
            ss_conn.commit()
            print(
                f"[faults] SQL Server DW: set settlement_price=NULL for {target_dw} rows "
                f"in ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
                flush=True,
            )
            ss_cur.close()
        finally:
            ss_conn.close()

    print(f"[faults] [Caso 2] null_settlement_price injection complete for {business_date}", flush=True)


def zero_sum_position(mysql_conn, business_date):
    """
    Caso 3: Create positions where long_value + short_value = 0 in
    ASTACASH_MRKT_PSTN (Oracle) and ADWPM_POSICAO_MERCADO_A_VISTA (SQL Server DW).
    Inserts synthetic zero-sum rows to guarantee the fault is detectable.
    """
    print(f"[faults] [Caso 3] injecting zero-sum positions for {business_date}", flush=True)

    PARTICIPANTS = ["PART001", "PART002", "PART003"]
    TICKERS = ["PETR4", "VALE3", "B3SA3"]
    num_zero_rows = 5

    # ── Oracle ASTACASH_MRKT_PSTN ─────────────────────────────────────────
    ora_conn = _get_oracle_conn()
    if ora_conn:
        try:
            ora_cur = ora_conn.cursor()
            for i in range(num_zero_rows):
                pos_id   = str(uuid.uuid4())
                ticker   = TICKERS[i % len(TICKERS)]
                part     = PARTICIPANTS[i % len(PARTICIPANTS)]
                value    = round(random.uniform(1000, 50000), 4)
                ora_cur.execute(
                    """
                    INSERT INTO ASTACASH_MRKT_PSTN
                        (position_id, ticker, participant_code, position_date,
                         long_value, short_value, net_value)
                    VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), :5, :6, :7)
                    """,
                    (pos_id, ticker, part, business_date, value, -value, 0.0),
                )
            ora_conn.commit()
            print(
                f"[faults] Oracle: inserted {num_zero_rows} zero-sum rows in ASTACASH_MRKT_PSTN",
                flush=True,
            )
            ora_cur.close()
        finally:
            ora_conn.close()

    # ── SQL Server DW ADWPM_POSICAO_MERCADO_A_VISTA ───────────────────────
    ss_conn = _get_sqlserver_conn()
    if ss_conn:
        try:
            ss_cur = ss_conn.cursor()
            for i in range(num_zero_rows):
                pos_id = str(uuid.uuid4())
                ticker = TICKERS[i % len(TICKERS)]
                part   = PARTICIPANTS[i % len(PARTICIPANTS)]
                value  = round(random.uniform(1000, 50000), 4)
                ss_cur.execute(
                    """
                    INSERT INTO dbo.ADWPM_POSICAO_MERCADO_A_VISTA
                        (position_id, ticker, participant_code, position_date,
                         long_value, short_value, net_value, dw_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (pos_id, ticker, part, business_date, value, -value, 0.0, "fault_injection"),
                )
            ss_conn.commit()
            print(
                f"[faults] SQL Server DW: inserted {num_zero_rows} zero-sum rows in "
                f"ADWPM_POSICAO_MERCADO_A_VISTA",
                flush=True,
            )
            ss_cur.close()
        finally:
            ss_conn.close()

    # ── MySQL fallback (raw_participant_positions) ─────────────────────────
    mysql_cursor = mysql_conn.cursor()
    for i in range(num_zero_rows):
        try:
            mysql_cursor.execute(
                """
                INSERT INTO raw_participant_positions
                    (participant, ticker, business_date, net_quantity, avg_price)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    PARTICIPANTS[i % len(PARTICIPANTS)],
                    TICKERS[i % len(TICKERS)],
                    business_date,
                    0,   # net_quantity = 0 signals zero-sum in fallback
                    round(random.uniform(10, 100), 2),
                ),
            )
        except Exception as e:
            print(f"[faults] MySQL zero-sum insert skipped: {e}", flush=True)
    mysql_conn.commit()
    mysql_cursor.close()

    print(f"[faults] [Caso 3] zero_sum_position injection complete for {business_date}", flush=True)


def overflow(mysql_conn, business_date):
    """
    Overflow: Change a decimal value to exceed column precision in
    ASTADRVT_TRADE_MVMT (Oracle) and ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO (SQL Server DW).
    Simulates a source system type/scale change breaking the DW precision contract.
    """
    print(f"[faults] [overflow] injecting overflow value for {business_date}", flush=True)

    # SQL Server: notional DECIMAL(18,4) — inject a value that exceeds scale
    overflow_value = 999999999999999.12345  # 15 digits + 5 decimal = overflows (18,4)

    ss_conn = _get_sqlserver_conn()
    if ss_conn:
        try:
            ss_cur = ss_conn.cursor()
            # Update top 1 row — SQL Server will raise arithmetic overflow on insert
            # We INSERT an extra row with bad data to trigger the error path
            bad_trade_id = str(uuid.uuid4())
            try:
                ss_cur.execute(
                    """
                    INSERT INTO dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
                        (trade_id, ticker, notional, quantity, trade_dt, status, dw_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (bad_trade_id, "OVERFLOW_TEST", overflow_value, 1,
                     business_date, "EXECUTED", "fault_overflow"),
                )
                ss_conn.commit()
                print(
                    f"[faults] SQL Server DW: inserted overflow notional row trade_id={bad_trade_id}",
                    flush=True,
                )
            except Exception as e:
                print(
                    f"[faults] SQL Server DW: overflow INSERT raised error as expected: {e}",
                    flush=True,
                )
            ss_cur.close()
        finally:
            ss_conn.close()

    # Oracle: notional NUMBER(18,4) — same overflow
    ora_conn = _get_oracle_conn()
    if ora_conn:
        try:
            ora_cur = ora_conn.cursor()
            bad_trade_id = str(uuid.uuid4())
            try:
                ora_cur.execute(
                    """
                    INSERT INTO ASTADRVT_TRADE_MVMT
                        (trade_id, ticker, notional, quantity, trade_dt, status)
                    VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), :6)
                    """,
                    (bad_trade_id, "OVERFLOW_TEST", overflow_value, 1, business_date, "EXECUTED"),
                )
                ora_conn.commit()
                print(
                    f"[faults] Oracle: inserted overflow notional row trade_id={bad_trade_id}",
                    flush=True,
                )
            except Exception as e:
                print(
                    f"[faults] Oracle: overflow INSERT raised error as expected: {e}",
                    flush=True,
                )
            ora_cur.close()
        finally:
            ora_conn.close()

    # MySQL: overflow in raw_trades price column
    mysql_cursor = mysql_conn.cursor()
    try:
        bad_id = str(uuid.uuid4())
        mysql_cursor.execute(
            """
            INSERT INTO raw_trades
                (trade_id, order_id, ticker, trade_time, price, quantity,
                 buyer_participant, seller_participant, business_date)
            VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s)
            """,
            (bad_id, str(uuid.uuid4()), "OVERFLOW_TEST", overflow_value, 1,
             "PART001", "PART002", business_date),
        )
        mysql_conn.commit()
    except Exception as e:
        print(f"[faults] MySQL overflow INSERT raised error as expected: {e}", flush=True)
    mysql_cursor.close()

    print(f"[faults] [overflow] injection complete for {business_date}", flush=True)


def row_count_diff(mysql_conn, business_date):
    """
    Row count diff: Delete 10% of Oracle source rows from ASTADRVT_TRADE_MVMT
    after the SQL Server DW has already been loaded. The DW then has more rows
    than the source, causing quality_gate check 4 to fire.
    Also deletes from MySQL raw_trades for consistency.
    """
    print(f"[faults] [row_count_diff] deleting 10% of Oracle source rows for {business_date}", flush=True)

    # ── Oracle ASTADRVT_TRADE_MVMT ─────────────────────────────────────────
    ora_conn = _get_oracle_conn()
    if ora_conn:
        try:
            ora_cur = ora_conn.cursor()
            ora_cur.execute(
                "SELECT COUNT(*) FROM ASTADRVT_TRADE_MVMT "
                "WHERE trade_dt = TO_DATE(:bdate, 'YYYY-MM-DD')",
                bdate=business_date,
            )
            total = ora_cur.fetchone()[0]
            to_delete = max(1, int(total * 0.10))

            ora_cur.execute(
                """
                DELETE FROM ASTADRVT_TRADE_MVMT
                WHERE trade_id IN (
                    SELECT trade_id FROM (
                        SELECT trade_id FROM ASTADRVT_TRADE_MVMT
                        WHERE trade_dt = TO_DATE(:bdate, 'YYYY-MM-DD')
                        ORDER BY DBMS_RANDOM.VALUE
                        FETCH FIRST :n ROWS ONLY
                    )
                )
                """,
                bdate=business_date,
                n=to_delete,
            )
            ora_conn.commit()
            print(
                f"[faults] Oracle: deleted {to_delete}/{total} rows from ASTADRVT_TRADE_MVMT "
                f"(DW still has the original count — gap will trigger Check 4)",
                flush=True,
            )
            ora_cur.close()
        finally:
            ora_conn.close()

    # ── MySQL raw_trades (fallback layer) ──────────────────────────────────
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(
        "SELECT COUNT(*) FROM raw_trades WHERE business_date = %s",
        (business_date,),
    )
    mysql_total = mysql_cursor.fetchone()[0]
    mysql_to_delete = max(1, int(mysql_total * 0.10))

    mysql_cursor.execute(
        """
        DELETE FROM raw_trades
        WHERE trade_id IN (
            SELECT trade_id FROM (
                SELECT trade_id FROM raw_trades
                WHERE business_date = %s
                ORDER BY RAND()
                LIMIT %s
            ) AS sub
        )
        """,
        (business_date, mysql_to_delete),
    )
    mysql_conn.commit()
    mysql_cursor.close()
    print(
        f"[faults] MySQL: deleted {mysql_to_delete}/{mysql_total} rows from raw_trades",
        flush=True,
    )

    print(f"[faults] [row_count_diff] injection complete for {business_date}", flush=True)


# ── Legacy MySQL-only fault injectors (kept for backward compatibility) ───────

def duplicate_trade_id(mysql_conn, business_date):
    """Legacy: duplicate ~5 trade_ids in MySQL raw_trades only."""
    print(f"[faults] [legacy] injecting duplicate trade IDs for {business_date}")
    cursor = mysql_conn.cursor()

    cursor.execute(
        """
        SELECT trade_id, order_id, ticker, trade_time, price, quantity,
               buyer_participant, seller_participant, business_date
        FROM raw_trades
        WHERE business_date = %s
        ORDER BY RAND()
        LIMIT 5
        """,
        (business_date,),
    )
    rows = cursor.fetchall()

    insert_sql = """
        INSERT INTO raw_trades
            (trade_id, order_id, ticker, trade_time, price, quantity,
             buyer_participant, seller_participant, business_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    count = 0
    for row in rows:
        try:
            cursor.execute(insert_sql, row)
            count += 1
        except Exception as e:
            print(f"[faults] could not duplicate trade_id={row[0]}: {e}")

    mysql_conn.commit()
    cursor.close()
    print(f"[faults] duplicated {count} trade IDs")


def null_closing_price(mysql_conn, business_date):
    """Legacy: set closing_price = NULL for 1-2 MySQL tickers."""
    print(f"[faults] [legacy] nullifying closing prices for {business_date}")
    cursor = mysql_conn.cursor()

    cursor.execute(
        """
        SELECT ticker FROM raw_close_prices
        WHERE business_date = %s
        ORDER BY RAND()
        LIMIT 2
        """,
        (business_date,),
    )
    tickers = [r[0] for r in cursor.fetchall()]

    for ticker in tickers:
        cursor.execute(
            """
            UPDATE raw_close_prices
            SET closing_price = NULL, vwap = NULL
            WHERE business_date = %s AND ticker = %s
            """,
            (business_date, ticker),
        )
        print(f"[faults] set closing_price=NULL for {ticker}")

    mysql_conn.commit()
    cursor.close()
    print(f"[faults] nullified closing prices for {len(tickers)} tickers")


def late_settlement_file(mysql_conn, business_date):
    """Legacy: shift settlement dates in MySQL raw_settlement_instructions."""
    print(f"[faults] [legacy] injecting late settlement dates for {business_date}")
    cursor = mysql_conn.cursor()

    cursor.execute(
        """
        SELECT instruction_id FROM raw_settlement_instructions
        WHERE business_date = %s
        ORDER BY RAND()
        LIMIT 50
        """,
        (business_date,),
    )
    ids = [r[0] for r in cursor.fetchall()]

    count = 0
    for instruction_id in ids:
        extra_days = random.randint(1, 3)
        cursor.execute(
            """
            UPDATE raw_settlement_instructions
            SET settlement_date = DATE_ADD(settlement_date, INTERVAL %s DAY),
                status = 'FAILED'
            WHERE instruction_id = %s
            """,
            (extra_days, instruction_id),
        )
        count += 1

    mysql_conn.commit()
    cursor.close()
    print(f"[faults] delayed {count} settlement instructions")


def volume_spike(mysql_conn, business_date):
    """Legacy: insert 10x normal volume for one MySQL ticker."""
    print(f"[faults] [legacy] injecting volume spike for {business_date}")
    cursor = mysql_conn.cursor()

    cursor.execute(
        """
        SELECT ticker, COUNT(*) AS cnt
        FROM raw_trades
        WHERE business_date = %s
        GROUP BY ticker
        ORDER BY RAND()
        LIMIT 1
        """,
        (business_date,),
    )
    row = cursor.fetchone()
    if not row:
        print("[faults] no trades found, skipping volume spike")
        cursor.close()
        return

    ticker, current_count = row[0], row[1]
    spike_count = current_count * 10

    cursor.execute(
        """
        SELECT order_id, price, quantity, buyer_participant, seller_participant, trade_time
        FROM raw_trades
        WHERE business_date = %s AND ticker = %s
        LIMIT 1
        """,
        (business_date, ticker),
    )
    template = cursor.fetchone()
    if not template:
        cursor.close()
        return

    insert_sql = """
        INSERT INTO raw_trades
            (trade_id, order_id, ticker, trade_time, price, quantity,
             buyer_participant, seller_participant, business_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = []
    for _ in range(spike_count):
        rows.append((
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            ticker,
            template[5],
            float(template[1]),
            int(template[2]),
            template[3],
            template[4],
            business_date,
        ))

    cursor.executemany(insert_sql, rows)
    mysql_conn.commit()
    cursor.close()
    print(f"[faults] inserted {spike_count} spike trades for {ticker}")


def slow_reconciliation_query(mysql_conn, business_date):
    """Legacy: drop indexes in MySQL to slow reconciliation queries for DBM demo."""
    print(f"[faults] [legacy] dropping indexes to slow down reconciliation for {business_date}")
    cursor = mysql_conn.cursor()

    try:
        cursor.execute("DROP INDEX idx_settlement_business_date ON raw_settlement_instructions")
        print("[faults] dropped index idx_settlement_business_date")
    except Exception as e:
        print(f"[faults] index may not exist, skipping: {e}")

    try:
        cursor.execute("DROP INDEX idx_trades_business_date ON raw_trades")
        print("[faults] dropped index idx_trades_business_date")
    except Exception as e:
        print(f"[faults] index may not exist, skipping: {e}")

    mysql_conn.commit()
    cursor.close()
    print("[faults] indexes dropped - reconciliation queries will be slow")
