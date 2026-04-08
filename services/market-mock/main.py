import os
import sys
import click
import mysql.connector

from generators import (
    generate_trades,
    generate_orders,
    generate_close_prices,
    generate_positions,
    generate_settlements,
)
from faults import (
    # Real Exchange DQ fault types (Caso 1, 2, 3)
    duplicate_trade_mvmt,
    null_settlement_price,
    zero_sum_position,
    overflow,
    row_count_diff,
    # Legacy MySQL-only faults (backward compat)
    duplicate_trade_id,
    null_closing_price,
    late_settlement_file,
    volume_spike,
    slow_reconciliation_query,
)


def get_connection():
    return mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST", "mysql"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", "demopoc2026"),
        database=os.environ.get("MYSQL_DATABASE", "exchange"),
    )


FAULT_REGISTRY = {
    # ── Real Exchange scenarios ──────────────────────────────────────────────────
    # inject-fault --fault-type duplicate_trade_mvmt
    # Adds 4 duplicate rows in ASTADRVT_TRADE_MVMT (Oracle) and
    # ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO (SQL Server DW).
    # Duplicates survive the ETL and are detected by quality_gate_d1 (Caso 1).
    "duplicate_trade_mvmt": duplicate_trade_mvmt,

    # inject-fault --fault-type null_settlement_price
    # Sets settlement_price = NULL for 5% of ASTANO_FGBE_DRVT_PSTN rows (Oracle).
    # Propagates to ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL (Caso 2).
    "null_settlement_price": null_settlement_price,

    # inject-fault --fault-type zero_sum_position
    # Creates positions where long_value + short_value = 0 in ASTACASH_MRKT_PSTN
    # and ADWPM_POSICAO_MERCADO_A_VISTA (Caso 3).
    "zero_sum_position": zero_sum_position,

    # inject-fault --fault-type overflow
    # Changes a decimal notional value to exceed column precision (18,4).
    # Simulates a source system type/scale change breaking the DW.
    "overflow": overflow,

    # inject-fault --fault-type row_count_diff
    # Deletes 10% of Oracle ASTADRVT_TRADE_MVMT source rows after DW is loaded.
    # DW row count > source count triggers quality_gate Check 4.
    "row_count_diff": row_count_diff,

    # ── Legacy MySQL-only faults ──────────────────────────────────────────
    "duplicate_trade_id": duplicate_trade_id,
    "null_closing_price": null_closing_price,
    "late_settlement_file": late_settlement_file,
    "volume_spike": volume_spike,
    "slow_reconciliation_query": slow_reconciliation_query,
}


@click.group()
def cli():
    """Market Data Mock - generates realistic Brazilian stock market data."""
    pass


@cli.command()
@click.option(
    "--business-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Business date in YYYY-MM-DD format.",
)
@click.option("--num-trades", default=500, help="Number of trades to generate.")
@click.option("--num-orders", default=800, help="Number of orders to generate.")
def generate_day(business_date, num_trades, num_orders):
    """Generate a full day of market data for a given business date."""
    date_str = business_date.strftime("%Y-%m-%d")
    click.echo(f"[market-mock] generating full day for business_date={date_str}")

    conn = get_connection()
    try:
        generate_orders(conn, date_str, num_orders=num_orders)
        generate_trades(conn, date_str, num_trades=num_trades)
        generate_close_prices(conn, date_str)
        generate_positions(conn, date_str)
        generate_settlements(conn, date_str)
        click.echo(f"[market-mock] day generation complete for {date_str}")
    except Exception as e:
        click.echo(f"[market-mock] ERROR: {e}", err=True)
        sys.exit(1)
    finally:
        conn.close()


@cli.command()
@click.option(
    "--fault-type",
    required=True,
    type=click.Choice(list(FAULT_REGISTRY.keys())),
    help="Type of fault to inject.",
)
@click.option(
    "--business-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Business date in YYYY-MM-DD format.",
)
def inject_fault(fault_type, business_date):
    """Inject a specific fault into the market data for demo purposes."""
    date_str = business_date.strftime("%Y-%m-%d")
    click.echo(f"[market-mock] injecting fault={fault_type} for business_date={date_str}")

    conn = get_connection()
    try:
        fault_fn = FAULT_REGISTRY[fault_type]
        fault_fn(conn, date_str)
        click.echo(f"[market-mock] fault injection complete: {fault_type}")
    except Exception as e:
        click.echo(f"[market-mock] ERROR: {e}", err=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    cli()
