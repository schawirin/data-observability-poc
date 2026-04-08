import os
import uuid
import random
from datetime import datetime, timedelta

TICKERS = {
    "PETR4": {"min_price": 35.00, "max_price": 40.00, "lot_size": 100},
    "VALE3": {"min_price": 55.00, "max_price": 65.00, "lot_size": 100},
    "B3SA3": {"min_price": 11.00, "max_price": 14.00, "lot_size": 100},
    "ITUB4": {"min_price": 30.00, "max_price": 35.00, "lot_size": 100},
    "WINJ26": {"min_price": 125000.00, "max_price": 130000.00, "lot_size": 1},
}

MARKET_OPEN_HOUR = 10
MARKET_CLOSE_HOUR = 17


# ── Oracle / SQL Server connection helpers ───────────────────────────────────

def _get_oracle_conn():
    """Return an oracledb connection, or raise if unavailable."""
    import oracledb
    host     = os.environ.get("ORACLE_HOST", "demoorg-oracle")
    port     = int(os.environ.get("ORACLE_PORT", "1521"))
    service  = os.environ.get("ORACLE_SERVICE", "FREE")
    user     = os.environ.get("ORACLE_USER", "system")
    password = os.environ.get("ORACLE_PASSWORD", "OracleDemo123!")
    dsn = f"{host}:{port}/{service}"
    return oracledb.connect(user=user, password=password, dsn=dsn)


def _get_sqlserver_conn():
    """Return a pymssql connection, or raise if unavailable."""
    import pymssql
    return pymssql.connect(
        server=os.environ.get("MSSQL_HOST", "demoorg-sqlserver"),
        port=int(os.environ.get("MSSQL_PORT", "1433")),
        user=os.environ.get("MSSQL_USER", "sa"),
        password=os.environ.get("MSSQL_SA_PASSWORD", "SqlDemo12345!"),
        database=os.environ.get("MSSQL_DATABASE", "demopoc"),
    )


def _write_trades_to_oracle(rows):
    """
    Write trade rows to Oracle ASTADRVT_TRADE_MVMT (real exchange table).
    rows is a list of tuples in the same order as the MySQL raw_trades INSERT:
    (trade_id, order_id, ticker, trade_time, price, quantity,
     buyer_participant, seller_participant, business_date)

    Maps MySQL columns → ASTADRVT_TRADE_MVMT columns:
      trade_id          → trade_id
      ticker            → ticker
      price * quantity  → notional
      quantity          → quantity
      business_date     → trade_dt
      seller_participant → counterparty_id
      buyer_participant  → trader_id
    """
    try:
        ora_conn = _get_oracle_conn()
        ora_cursor = ora_conn.cursor()
        # Transform to ASTADRVT_TRADE_MVMT shape
        asta_rows = [
            (
                r[0],                          # trade_id
                r[2],                          # ticker
                round(float(r[4]) * int(r[5]), 4),  # notional = price * quantity
                int(r[5]),                     # quantity
                r[8],                          # trade_dt (business_date)
                None,                          # settlement_dt
                r[7],                          # counterparty_id (seller_participant)
                r[6],                          # trader_id (buyer_participant)
                None,                          # desk_code
                "EXECUTED",                    # status
            )
            for r in rows
        ]
        ora_cursor.executemany(
            """
            INSERT INTO ASTADRVT_TRADE_MVMT
                (trade_id, ticker, notional, quantity, trade_dt,
                 settlement_dt, counterparty_id, trader_id, desk_code, status)
            VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'),
                    :6, :7, :8, :9, :10)
            """,
            asta_rows,
        )
        ora_conn.commit()
        ora_cursor.close()
        ora_conn.close()
        print(f"[generators] wrote {len(asta_rows)} trades to Oracle ASTADRVT_TRADE_MVMT", flush=True)
    except Exception as exc:
        print(f"[generators] Oracle ASTADRVT_TRADE_MVMT write skipped ({exc})", flush=True)


def _write_positions_to_oracle(rows_drvt, rows_cash, business_date):
    """
    Write synthetic position rows to Oracle ASTANO_FGBE_DRVT_PSTN and ASTACASH_MRKT_PSTN.
    rows_drvt: list of (position_id, instrument_id, participant_code, long_qty, short_qty, net_qty, settlement_price)
    rows_cash: list of (position_id, ticker, participant_code, long_value, short_value, net_value)
    """
    try:
        ora_conn = _get_oracle_conn()
        ora_cursor = ora_conn.cursor()

        if rows_drvt:
            ora_cursor.executemany(
                """
                INSERT INTO ASTANO_FGBE_DRVT_PSTN
                    (position_id, instrument_id, participant_code,
                     position_date, long_qty, short_qty, net_qty,
                     settlement_price, margin_value)
                VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), :5, :6, :7, :8, :9)
                """,
                [(r[0], r[1], r[2], business_date, r[3], r[4], r[5], r[6], None)
                 for r in rows_drvt],
            )
            print(f"[generators] wrote {len(rows_drvt)} rows to Oracle ASTANO_FGBE_DRVT_PSTN", flush=True)

        if rows_cash:
            ora_cursor.executemany(
                """
                INSERT INTO ASTACASH_MRKT_PSTN
                    (position_id, ticker, participant_code,
                     position_date, long_value, short_value, net_value)
                VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), :5, :6, :7)
                """,
                [(r[0], r[1], r[2], business_date, r[3], r[4], r[5])
                 for r in rows_cash],
            )
            print(f"[generators] wrote {len(rows_cash)} rows to Oracle ASTACASH_MRKT_PSTN", flush=True)

        ora_conn.commit()
        ora_cursor.close()
        ora_conn.close()
    except Exception as exc:
        print(f"[generators] Oracle position write skipped ({exc})", flush=True)


def _write_orders_to_oracle(rows):
    """
    Write order rows to Oracle (legacy raw_orders_src kept for compatibility).
    rows tuples: (order_id, ticker, side, order_time, price, quantity,
                  participant, business_date)
    """
    try:
        ora_conn = _get_oracle_conn()
        ora_cursor = ora_conn.cursor()
        ora_cursor.executemany(
            """
            INSERT INTO raw_orders_src
                (order_id, ticker, side, order_time, price, quantity,
                 participant_code, business_date)
            VALUES (:1, :2, :3, :4, :5, :6, :7,
                    TO_DATE(:8, 'YYYY-MM-DD'))
            """,
            rows,
        )
        ora_conn.commit()
        ora_cursor.close()
        ora_conn.close()
        print(f"[generators] wrote {len(rows)} orders to Oracle raw_orders_src", flush=True)
    except Exception as exc:
        print(f"[generators] Oracle raw_orders_src write skipped ({exc})", flush=True)


def _write_settlements_to_sqlserver(rows):
    """
    Write settlement instruction rows to SQL Server settlement_instructions_src (legacy).
    rows tuples: (instruction_id, trade_id, settlement_date, participant,
                  ticker, quantity, amount, status, business_date)
    """
    try:
        ss_conn = _get_sqlserver_conn()
        ss_cursor = ss_conn.cursor()
        ss_cursor.executemany(
            """
            INSERT INTO settlement_instructions_src
                (instruction_id, trade_id, settlement_date, net_amount,
                 status, counterparty, participant, ticker, quantity,
                 amount, business_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
        ss_conn.commit()
        ss_cursor.close()
        ss_conn.close()
        print(
            f"[generators] wrote {len(rows)} settlement instructions to SQL Server",
            flush=True,
        )
    except Exception as exc:
        print(f"[generators] SQL Server write skipped ({exc})", flush=True)


# ── Existing helpers ─────────────────────────────────────────────────────────

def _get_participants(conn):
    """Fetch participant codes from ref_participants table."""
    cursor = conn.cursor()
    cursor.execute("SELECT participant_code FROM ref_participants WHERE active = 1")
    rows = cursor.fetchall()
    cursor.close()
    if not rows:
        return [f"PART{i:03d}" for i in range(1, 11)]
    return [r[0] for r in rows]


def _random_timestamp(business_date_str):
    """Return a random datetime during market hours for the given date."""
    base = datetime.strptime(business_date_str, "%Y-%m-%d")
    seconds_in_session = (MARKET_CLOSE_HOUR - MARKET_OPEN_HOUR) * 3600
    offset = random.randint(0, seconds_in_session)
    return base.replace(hour=MARKET_OPEN_HOUR) + timedelta(seconds=offset)


def _random_price(ticker):
    info = TICKERS[ticker]
    price = random.uniform(info["min_price"], info["max_price"])
    if ticker == "WINJ26":
        return round(price / 5) * 5
    return round(price, 2)


def _random_quantity(ticker):
    info = TICKERS[ticker]
    if ticker == "WINJ26":
        return random.randint(1, 20)
    lots = random.randint(1, 50)
    return lots * info["lot_size"]


def generate_trades(conn, business_date, num_trades=500):
    """Generate trade records into raw_trades."""
    print(f"[generators] generating {num_trades} trades for {business_date}")
    participants = _get_participants(conn)
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO raw_trades
            (trade_id, order_id, ticker, trade_time, price, quantity,
             buyer_participant, seller_participant, business_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = []
    for _ in range(num_trades):
        ticker = random.choice(list(TICKERS.keys()))
        buyer = random.choice(participants)
        seller = random.choice([p for p in participants if p != buyer] or participants)
        rows.append((
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            ticker,
            _random_timestamp(business_date),
            _random_price(ticker),
            _random_quantity(ticker),
            buyer,
            seller,
            business_date,
        ))

    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    print(f"[generators] inserted {num_trades} trades")

    # Mirror to Oracle raw_trades_src (graceful: skip if unreachable)
    _write_trades_to_oracle(rows)


def generate_orders(conn, business_date, num_orders=800):
    """Generate order records into raw_orders."""
    print(f"[generators] generating {num_orders} orders for {business_date}")
    participants = _get_participants(conn)
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO raw_orders
            (order_id, ticker, side, order_time, price, quantity,
             participant, business_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = []
    for _ in range(num_orders):
        ticker = random.choice(list(TICKERS.keys()))
        side = random.choice(["BUY", "SELL"])
        rows.append((
            str(uuid.uuid4()),
            ticker,
            side,
            _random_timestamp(business_date),
            _random_price(ticker),
            _random_quantity(ticker),
            random.choice(participants),
            business_date,
        ))

    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    print(f"[generators] inserted {num_orders} orders")

    # Mirror to Oracle raw_orders_src (graceful: skip if unreachable)
    _write_orders_to_oracle(rows)


def generate_close_prices(conn, business_date):
    """Calculate and insert closing prices from trades into raw_close_prices."""
    print(f"[generators] calculating closing prices for {business_date}")
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT ticker,
               SUM(price * quantity) / SUM(quantity) AS vwap,
               SUM(quantity) AS daily_volume
        FROM raw_trades
        WHERE business_date = %s
        GROUP BY ticker
        """,
        (business_date,),
    )

    rows = cursor.fetchall()
    insert_sql = """
        INSERT INTO raw_close_prices
            (ticker, business_date, closing_price, daily_volume, vwap, auction_flag)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            closing_price = VALUES(closing_price),
            daily_volume = VALUES(daily_volume),
            vwap = VALUES(vwap)
    """

    count = 0
    for ticker, vwap, volume in rows:
        closing_price = round(float(vwap), 2)
        cursor.execute(insert_sql, (
            ticker,
            business_date,
            closing_price,
            int(volume),
            round(float(vwap), 4),
            0,
        ))
        count += 1

    conn.commit()
    cursor.close()
    print(f"[generators] inserted {count} closing prices")


def generate_positions(conn, business_date):
    """Calculate net positions per participant/ticker from trades into raw_participant_positions."""
    print(f"[generators] calculating positions for {business_date}")
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT participant, ticker, SUM(net_qty) AS net_position,
               AVG(avg_p) AS avg_price
        FROM (
            SELECT buyer_participant AS participant, ticker,
                   SUM(quantity) AS net_qty,
                   AVG(price) AS avg_p
            FROM raw_trades WHERE business_date = %s
            GROUP BY buyer_participant, ticker
            UNION ALL
            SELECT seller_participant AS participant, ticker,
                   -SUM(quantity) AS net_qty,
                   AVG(price) AS avg_p
            FROM raw_trades WHERE business_date = %s
            GROUP BY seller_participant, ticker
        ) AS combined
        GROUP BY participant, ticker
        """,
        (business_date, business_date),
    )

    rows = cursor.fetchall()
    insert_sql = """
        INSERT INTO raw_participant_positions
            (participant, ticker, business_date, net_quantity, avg_price)
        VALUES (%s, %s, %s, %s, %s)
    """

    count = 0
    for participant, ticker, net_qty, avg_price in rows:
        cursor.execute(insert_sql, (
            participant, ticker, business_date, int(net_qty), round(float(avg_price), 4)
        ))
        count += 1

    conn.commit()
    cursor.close()
    print(f"[generators] inserted {count} positions")

    # Mirror to Oracle ASTANO_FGBE_DRVT_PSTN and ASTACASH_MRKT_PSTN
    # Build two separate position datasets from the MySQL positions
    if rows:
        drvt_oracle_rows = []
        cash_oracle_rows = []
        for participant, ticker, net_qty, avg_price in rows:
            pos_id = str(uuid.uuid4())
            net_qty_f = float(net_qty)
            avg_price_f = float(avg_price)
            long_qty  = max(0.0, net_qty_f)
            short_qty = abs(min(0.0, net_qty_f))
            # Derivatives position (ASTANO_FGBE_DRVT_PSTN)
            drvt_oracle_rows.append((
                pos_id,
                ticker,          # instrument_id = ticker for demo
                participant,
                long_qty,
                short_qty,
                net_qty_f,
                avg_price_f,     # settlement_price
            ))
            # Cash/spot position (ASTACASH_MRKT_PSTN)
            long_val  = long_qty  * avg_price_f
            short_val = -short_qty * avg_price_f
            net_val   = net_qty_f  * avg_price_f
            cash_oracle_rows.append((
                str(uuid.uuid4()),
                ticker,
                participant,
                long_val,
                short_val,
                net_val,
            ))
        _write_positions_to_oracle(drvt_oracle_rows, cash_oracle_rows, business_date)


def generate_settlements(conn, business_date):
    """Create settlement instructions from trades into raw_settlement_instructions."""
    print(f"[generators] generating settlement instructions for {business_date}")
    cursor = conn.cursor()

    settlement_date = (
        datetime.strptime(business_date, "%Y-%m-%d") + timedelta(days=2)
    ).strftime("%Y-%m-%d")

    cursor.execute(
        """
        SELECT trade_id, ticker, price, quantity, buyer_participant
        FROM raw_trades
        WHERE business_date = %s
        """,
        (business_date,),
    )

    trades = cursor.fetchall()
    insert_sql = """
        INSERT INTO raw_settlement_instructions
            (instruction_id, trade_id, settlement_date, participant, ticker,
             quantity, amount, status, business_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    settle_rows = []
    count = 0
    for trade_id, ticker, price, quantity, participant in trades:
        amount = round(float(price) * int(quantity), 4)
        instruction_id = str(uuid.uuid4())
        row = (
            instruction_id,
            trade_id,
            settlement_date,
            participant,
            ticker,
            int(quantity),
            amount,
            "PENDING",
            business_date,
        )
        cursor.execute(insert_sql, row)
        settle_rows.append(row)
        count += 1

    conn.commit()
    cursor.close()
    print(f"[generators] inserted {count} settlement instructions (settle date: {settlement_date})")

    # Mirror to SQL Server settlement_instructions_src (graceful: skip if unreachable)
    # SQL Server row: (instruction_id, trade_id, settlement_date, net_amount,
    #                  status, counterparty, participant, ticker, quantity, amount, business_date)
    ss_rows = [
        (
            r[0],            # instruction_id
            r[1],            # trade_id
            r[2],            # settlement_date
            r[6],            # net_amount (= amount)
            r[7],            # status
            r[3],            # counterparty (same as participant for buyer side)
            r[3],            # participant
            r[4],            # ticker
            r[5],            # quantity
            r[6],            # amount
            r[8],            # business_date
        )
        for r in settle_rows
    ]
    _write_settlements_to_sqlserver(ss_rows)
