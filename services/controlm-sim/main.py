"""controlm-sim CLI — simulates Control-M orchestration for the Data Pipeline POC."""

import json
import logging
import os
import sys

import click

from executor import execute_pipeline, execute_job

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)


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
def run_pipeline_cmd(business_date):
    """Run the full market-d1 DAG (all 4 jobs in dependency order)."""
    click.echo(f"[controlm-sim] starting pipeline for business_date={business_date}")
    summary = execute_pipeline(business_date)
    click.echo(json.dumps(summary, indent=2, default=str))

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

    if outcome["status"] == "failed":
        sys.exit(1)


if __name__ == "__main__":
    cli()
