"""
quality/checks.py - Data quality check functions for the Data Pipeline POC.

Each function returns a list of check result dicts with the schema:
{
    "check_name": str,
    "check_type": str,
    "target_table": str,
    "severity": str,       # "critical", "warning", "info"
    "passed": bool,
    "expected": str,
    "actual": str,
    "details": str,
}
"""


def check_completeness(conn, business_date):
    """
    Completeness checks:
    - closing_price NOT NULL for all tickers that traded
    - No NULL required fields in raw_trades
    """
    cursor = conn.cursor(dictionary=True)
    results = []

    # Check 1: closing_price NOT NULL for all tickers
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM raw_close_prices
        WHERE business_date = %s AND closing_price IS NULL
        """,
        (business_date,),
    )
    row = cursor.fetchone()
    null_closing = row["cnt"]
    results.append({
        "check_name": "closing_price_not_null",
        "check_type": "completeness",
        "target_table": "raw_close_prices",
        "severity": "critical",
        "passed": null_closing == 0,
        "expected": "0 NULL closing prices",
        "actual": f"{null_closing} NULL closing prices",
        "details": f"Found {null_closing} tickers with NULL closing_price for {business_date}",
    })

    # Check 2: No NULL required fields in raw_trades
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM raw_trades
        WHERE business_date = %s
          AND (trade_id IS NULL OR ticker IS NULL OR price IS NULL OR quantity IS NULL)
        """,
        (business_date,),
    )
    row = cursor.fetchone()
    null_fields = row["cnt"]
    results.append({
        "check_name": "trades_required_fields_not_null",
        "check_type": "completeness",
        "target_table": "raw_trades",
        "severity": "critical",
        "passed": null_fields == 0,
        "expected": "0 rows with NULL required fields",
        "actual": f"{null_fields} rows with NULL required fields",
        "details": f"Found {null_fields} trades with NULL required fields for {business_date}",
    })

    cursor.close()
    return results


def check_uniqueness(conn, business_date):
    """
    Uniqueness checks:
    - trade_id unique per business_date in raw_trades
    """
    cursor = conn.cursor(dictionary=True)
    results = []

    cursor.execute(
        """
        SELECT trade_id, COUNT(*) AS cnt
        FROM raw_trades
        WHERE business_date = %s
        GROUP BY trade_id
        HAVING cnt > 1
        """,
        (business_date,),
    )
    duplicates = cursor.fetchall()
    dup_count = len(duplicates)
    results.append({
        "check_name": "trade_id_unique_per_date",
        "check_type": "uniqueness",
        "target_table": "raw_trades",
        "severity": "critical",
        "passed": dup_count == 0,
        "expected": "0 duplicate trade_ids",
        "actual": f"{dup_count} duplicate trade_ids",
        "details": (
            f"Found {dup_count} duplicate trade_id values for {business_date}"
            + (f": {[d['trade_id'] for d in duplicates[:5]]}" if duplicates else "")
        ),
    })

    cursor.close()
    return results


def check_consistency(conn, business_date):
    """
    Consistency checks:
    - Every settlement instruction has a matching trade
    """
    cursor = conn.cursor(dictionary=True)
    results = []

    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM raw_settlement_instructions si
        LEFT JOIN raw_trades t
            ON t.trade_id = si.trade_id
            AND t.business_date = si.business_date
        WHERE si.business_date = %s
          AND t.trade_id IS NULL
        """,
        (business_date,),
    )
    row = cursor.fetchone()
    orphan_count = row["cnt"]
    results.append({
        "check_name": "settlement_instructions_have_matching_trade",
        "check_type": "consistency",
        "target_table": "raw_settlement_instructions",
        "severity": "critical",
        "passed": orphan_count == 0,
        "expected": "0 orphan settlement instructions",
        "actual": f"{orphan_count} orphan settlement instructions",
        "details": f"Found {orphan_count} settlement instructions without a matching trade for {business_date}",
    })

    cursor.close()
    return results


def check_reasonableness(conn, business_date):
    """
    Reasonableness checks:
    - price > 0 for all trades
    - quantity > 0 for all trades
    - daily_volume within expected range (100 - 10,000,000)
    """
    cursor = conn.cursor(dictionary=True)
    results = []

    # Check 1: price > 0
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM raw_trades
        WHERE business_date = %s AND price <= 0
        """,
        (business_date,),
    )
    row = cursor.fetchone()
    bad_price = row["cnt"]
    results.append({
        "check_name": "trade_price_positive",
        "check_type": "reasonableness",
        "target_table": "raw_trades",
        "severity": "critical",
        "passed": bad_price == 0,
        "expected": "0 trades with price <= 0",
        "actual": f"{bad_price} trades with price <= 0",
        "details": f"Found {bad_price} trades with non-positive price for {business_date}",
    })

    # Check 2: quantity > 0
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM raw_trades
        WHERE business_date = %s AND quantity <= 0
        """,
        (business_date,),
    )
    row = cursor.fetchone()
    bad_qty = row["cnt"]
    results.append({
        "check_name": "trade_quantity_positive",
        "check_type": "reasonableness",
        "target_table": "raw_trades",
        "severity": "critical",
        "passed": bad_qty == 0,
        "expected": "0 trades with quantity <= 0",
        "actual": f"{bad_qty} trades with quantity <= 0",
        "details": f"Found {bad_qty} trades with non-positive quantity for {business_date}",
    })

    # Check 3: daily_volume within expected range
    cursor.execute(
        """
        SELECT ticker, daily_volume
        FROM raw_close_prices
        WHERE business_date = %s
          AND (daily_volume < 100 OR daily_volume > 10000000)
        """,
        (business_date,),
    )
    outliers = cursor.fetchall()
    outlier_count = len(outliers)
    results.append({
        "check_name": "daily_volume_in_range",
        "check_type": "reasonableness",
        "target_table": "raw_close_prices",
        "severity": "warning",
        "passed": outlier_count == 0,
        "expected": "All daily_volume between 100 and 10,000,000",
        "actual": f"{outlier_count} tickers outside range",
        "details": (
            f"Found {outlier_count} tickers with daily_volume outside [100, 10000000] for {business_date}"
            + (f": {[(o['ticker'], o['daily_volume']) for o in outliers[:5]]}" if outliers else "")
        ),
    })

    cursor.close()
    return results


def check_timeliness(conn, business_date):
    """
    Timeliness check - placeholder that always passes.
    SLA monitoring is handled by controlm-sim.
    """
    return [{
        "check_name": "timeliness_sla_check",
        "check_type": "timeliness",
        "target_table": "raw_trades",
        "severity": "info",
        "passed": True,
        "expected": "SLA met",
        "actual": "SLA met (monitored by controlm-sim)",
        "details": f"Timeliness SLA for {business_date} is monitored externally by controlm-sim",
    }]
