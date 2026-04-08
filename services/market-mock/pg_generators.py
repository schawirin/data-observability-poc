"""Market data generators for Postgres (used by dbt pipeline)."""

import random
import uuid
from datetime import datetime, timedelta

TICKERS = {
    "PETR4": {"min_price": 28.0, "max_price": 38.0, "lot": 100},
    "VALE3": {"min_price": 58.0, "max_price": 72.0, "lot": 100},
    "B3SA3": {"min_price": 10.0, "max_price": 14.0, "lot": 100},
    "ITUB4": {"min_price": 25.0, "max_price": 33.0, "lot": 100},
    "WINJ26": {"min_price": 125000.0, "max_price": 132000.0, "lot": 1},
}

PARTICIPANTS = [
    "XP001", "XP002", "BTG001", "BTG002", "ITAU001",
    "ITAU002", "MOD001", "SAF001", "GEN001", "CLR001",
]


def generate_trades(conn, business_date, count=500):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM raw_trades WHERE business_date = %s", (business_date,))

    rows = []
    base_time = datetime.strptime(f"{business_date} 10:00:00", "%Y-%m-%d %H:%M:%S")

    for i in range(count):
        ticker = random.choice(list(TICKERS.keys()))
        spec = TICKERS[ticker]
        price = round(random.uniform(spec["min_price"], spec["max_price"]), 2)
        qty = random.randint(1, 50) * spec["lot"]
        trade_time = base_time + timedelta(seconds=random.randint(0, 28800))
        buyer = random.choice(PARTICIPANTS)
        seller = random.choice([p for p in PARTICIPANTS if p != buyer])

        rows.append((
            str(uuid.uuid4()), str(uuid.uuid4()), ticker,
            trade_time, price, qty, buyer, seller, business_date,
        ))

    cursor.executemany(
        """INSERT INTO raw_trades
           (trade_id, order_id, ticker, trade_time, price, quantity,
            buyer_participant, seller_participant, business_date)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        rows,
    )
    conn.commit()
    return count


def generate_orders(conn, business_date, count=800):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM raw_orders WHERE business_date = %s", (business_date,))

    rows = []
    base_time = datetime.strptime(f"{business_date} 10:00:00", "%Y-%m-%d %H:%M:%S")

    for i in range(count):
        ticker = random.choice(list(TICKERS.keys()))
        spec = TICKERS[ticker]
        price = round(random.uniform(spec["min_price"], spec["max_price"]), 2)
        qty = random.randint(1, 50) * spec["lot"]
        side = random.choice(["BUY", "SELL"])
        order_time = base_time + timedelta(seconds=random.randint(0, 28800))
        participant = random.choice(PARTICIPANTS)

        rows.append((
            str(uuid.uuid4()), ticker, side, price, qty,
            participant, order_time, business_date,
        ))

    cursor.executemany(
        """INSERT INTO raw_orders
           (order_id, ticker, side, price, quantity,
            participant_id, order_time, business_date)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        rows,
    )
    conn.commit()
    return count


def generate_positions(conn, business_date):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM raw_participant_positions WHERE business_date = %s", (business_date,))

    cursor.execute("""
        SELECT buyer_participant AS pid, ticker, SUM(quantity) AS qty
        FROM raw_trades WHERE business_date = %s
        GROUP BY buyer_participant, ticker
    """, (business_date,))
    buys = {(r[0], r[1]): r[2] for r in cursor.fetchall()}

    cursor.execute("""
        SELECT seller_participant AS pid, ticker, SUM(quantity) AS qty
        FROM raw_trades WHERE business_date = %s
        GROUP BY seller_participant, ticker
    """, (business_date,))
    sells = {(r[0], r[1]): r[2] for r in cursor.fetchall()}

    all_keys = set(buys.keys()) | set(sells.keys())
    rows = []
    for pid, ticker in all_keys:
        net = buys.get((pid, ticker), 0) - sells.get((pid, ticker), 0)
        spec = TICKERS.get(ticker, {"min_price": 10, "max_price": 100})
        avg_price = round(random.uniform(spec["min_price"], spec["max_price"]), 2)
        rows.append((pid, ticker, net, avg_price, business_date))

    if rows:
        cursor.executemany(
            """INSERT INTO raw_participant_positions
               (participant_id, ticker, net_quantity, avg_price, business_date)
               VALUES (%s, %s, %s, %s, %s)""",
            rows,
        )
    conn.commit()
    return len(rows)


def generate_settlements(conn, business_date):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM raw_settlement_instructions WHERE business_date = %s", (business_date,))

    cursor.execute("""
        SELECT trade_id, ticker, quantity, buyer_participant, seller_participant
        FROM raw_trades WHERE business_date = %s
    """, (business_date,))
    trades = cursor.fetchall()

    bd = datetime.strptime(business_date, "%Y-%m-%d").date()
    settlement_date = bd + timedelta(days=2)

    rows = []
    for trade_id, ticker, qty, buyer, seller in trades:
        rows.append((
            str(uuid.uuid4()), trade_id, ticker, qty,
            settlement_date, "MATCHED", buyer, seller, business_date,
        ))

    if rows:
        cursor.executemany(
            """INSERT INTO raw_settlement_instructions
               (settlement_id, trade_id, ticker, quantity,
                settlement_date, status, buyer_participant, seller_participant, business_date)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            rows,
        )
    conn.commit()
    return len(rows)


def generate_all(conn, business_date):
    """Generate all market data for a business date."""
    trades = generate_trades(conn, business_date)
    orders = generate_orders(conn, business_date)
    positions = generate_positions(conn, business_date)
    settlements = generate_settlements(conn, business_date)
    return {
        "trades": trades,
        "orders": orders,
        "positions": positions,
        "settlements": settlements,
    }
