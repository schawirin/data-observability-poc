"""controlm-sim CLI — simulates Control-M orchestration for the Data Pipeline POC."""

import json
import logging
import os
import sys

import click

from executor import execute_pipeline, execute_job
from controlm_metrics import emit_smoke_metrics
from log_correlation import install_file_logging

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)


install_file_logging()


_FAULT_ENV = {
    "oracle_timeout": {"_FAULT_ORACLE_TIMEOUT": "5"},
    "gate_fail_hard": {"DQ_GATE_FAIL_HARD": "1"},
    "s3_down": {"_FAULT_S3_DOWN": "5"},
    "db_blocking": {"_FAULT_DB_BLOCKING": "1"},
    "db_deadlock": {"_FAULT_DB_DEADLOCK": "1"},
    "db_slow_query": {"_FAULT_DB_SLOW_QUERY": "1"},
    "all_hard": {"DQ_GATE_FAIL_HARD": "1"},
    "db_all": {
        "_FAULT_DB_BLOCKING": "1",
        "_FAULT_DB_DEADLOCK": "1",
        "_FAULT_DB_SLOW_QUERY": "1",
    },
}

_NO_SEED_FAULTS = {
    "oracle_timeout",
    "s3_down",
    "db_blocking",
    "db_deadlock",
    "db_slow_query",
}


def _prepare_cli_fault(inject_fault):
    """Set hard-fault env vars for CLI/cron runs and return seed fault name."""
    updates = _FAULT_ENV.get(inject_fault, {})
    previous = {key: os.environ.get(key) for key in updates}
    for key, value in updates.items():
        os.environ[key] = value

    if inject_fault in ("gate_fail_hard", "all_hard", "db_all"):
        seed_fault = "all"
    elif inject_fault in _NO_SEED_FAULTS:
        seed_fault = None
    else:
        seed_fault = inject_fault
    return seed_fault, previous


def _restore_env(previous):
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _flush_tracer():
    try:
        from ddtrace import tracer
        tracer.shutdown(timeout=5)
    except Exception:
        pass


@click.group()
def cli():
    """Control-M Simulator — DAG execution, retries, SLA checks, and OpenLineage emission."""
    pass


@cli.command("run-pipeline")
@click.option(
    "--business-date",
    required=True,
    type=str,
    help="Business date in YYYY-MM-DD format.",
)
@click.option(
    "--inject-fault",
    default=None,
    type=str,
    help="Inject a fault (duplicate_trades, null_settlement_price, zero_sum_positions, "
         "oracle_timeout, gate_fail_hard, s3_down, all_hard, db_blocking, db_deadlock, db_slow_query).",
)
def run_pipeline_cmd(business_date, inject_fault):
    """Run the full b3_d1_pipeline DAG (all 4 jobs in dependency order)."""
    click.echo(f"[controlm-sim] starting pipeline for business_date={business_date} fault={inject_fault or 'none'}")
    seed_fault, previous_env = _prepare_cli_fault(inject_fault)
    try:
        summary = execute_pipeline(business_date, inject_fault=seed_fault)
    finally:
        _restore_env(previous_env)
    click.echo(json.dumps(summary, indent=2, default=str))
    _flush_tracer()

    if summary["status"] == "failed":
        sys.exit(1)


@cli.command("run-job")
@click.option(
    "--job-name",
    required=True,
    type=str,
    help="Name of the job to run (e.g. close_market_eod).",
)
@click.option(
    "--business-date",
    required=True,
    type=str,
    help="Business date in YYYY-MM-DD format.",
)
def run_job_cmd(job_name, business_date):
    """Run a single job by name."""
    click.echo(f"[controlm-sim] running job={job_name} for business_date={business_date}")
    outcome = execute_job(job_name=job_name, business_date=business_date)
    click.echo(json.dumps(outcome, indent=2, default=str))
    _flush_tracer()

    if outcome["status"] == "failed":
        sys.exit(1)


@cli.command("emit-smoke-metrics")
@click.option(
    "--job-name",
    default="leitura_dados",
    show_default=True,
    help="Synthetic Control-M job name to emit for dashboard validation.",
)
@click.option(
    "--business-date",
    default=None,
    help="Business date in YYYY-MM-DD format. Defaults to today.",
)
def emit_smoke_metrics_cmd(job_name, business_date):
    """Emit one Control-M metric/APM sample without touching the databases."""
    result = emit_smoke_metrics(job_name=job_name, business_date=business_date)
    click.echo(json.dumps(result, indent=2, default=str))
    _flush_tracer()


if __name__ == "__main__":
    cli()
