"""Emit OpenLineage events to Datadog (Agent proxy + direct API fallback)."""

import json
import logging
import os
from datetime import datetime, timezone

import requests

logger = logging.getLogger("controlm-sim.openlineage")

# Primary: Agent proxy
DD_AGENT_HOST = os.environ.get("DD_AGENT_HOST", "datadog-agent")
DD_TRACE_AGENT_PORT = os.environ.get("DD_TRACE_AGENT_PORT", "8126")
AGENT_ENDPOINT = f"http://{DD_AGENT_HOST}:{DD_TRACE_AGENT_PORT}/openlineage/api/v1/lineage"

# Fallback: Direct Datadog API
DD_SITE = os.environ.get("DD_SITE", "datadoghq.com")
DD_API_KEY = os.environ.get("DD_API_KEY", "")
DIRECT_ENDPOINT = f"https://data-obs-intake.{DD_SITE}/api/v1/lineage"

JOB_NAMESPACE = "exchange-poc"
MYSQL_NAMESPACE = "mysql://demo-mysql"
S3_NAMESPACE = "s3://mock-exchange"

# ---------- dataset I/O mappings per job ----------

JOB_IO = {
    "close_market_eod": {
        "inputs": [
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.raw_trades"},
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.raw_orders"},
        ],
        "outputs": [
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.raw_close_prices"},
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.curated_market_close_snapshot"},
        ],
    },
    "reconcile_d1_positions": {
        "inputs": [
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.raw_trades"},
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.raw_participant_positions"},
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.raw_settlement_instructions"},
        ],
        "outputs": [
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.staging_settlement_recon"},
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.staging_position_recon"},
        ],
    },
    "quality_gate_d1": {
        "inputs": [
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.staging_settlement_recon"},
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.staging_position_recon"},
        ],
        "outputs": [
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.ops_quality_results"},
        ],
    },
    "publish_d1_reports": {
        "inputs": [
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.curated_market_close_snapshot"},
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.staging_settlement_recon"},
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.staging_position_recon"},
        ],
        "outputs": [
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.curated_d1_settlement_report"},
            {"namespace": MYSQL_NAMESPACE, "name": "exchange.curated_participant_exposure_report"},
            {"namespace": S3_NAMESPACE, "name": "exports/d1_settlement_report.parquet"},
            {"namespace": S3_NAMESPACE, "name": "exports/participant_exposure_report.csv"},
        ],
    },
}


def _get_io(job_name):
    """Return (inputs, outputs) for a job, falling back to provided lists."""
    mapping = JOB_IO.get(job_name, {"inputs": [], "outputs": []})
    return mapping["inputs"], mapping["outputs"]


def _build_event(event_type, job_name, run_id, parent_run_id, inputs, outputs, business_date, error=None):
    """Build an OpenLineage event dict."""
    now = datetime.now(timezone.utc).isoformat()

    if inputs is None or outputs is None:
        default_inputs, default_outputs = _get_io(job_name)
        inputs = inputs if inputs is not None else default_inputs
        outputs = outputs if outputs is not None else default_outputs

    run_facets = {
        "business_date": {
            "_producer": "https://github.com/exchange-poc/controlm-sim",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/CustomFacet.json",
            "business_date": business_date,
        },
    }

    if parent_run_id:
        run_facets["parent"] = {
            "_producer": "https://github.com/exchange-poc/controlm-sim",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
            "run": {"runId": parent_run_id},
            "job": {"namespace": JOB_NAMESPACE, "name": "market-d1"},
        }

    if error and event_type == "FAIL":
        run_facets["errorMessage"] = {
            "_producer": "https://github.com/exchange-poc/controlm-sim",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
            "message": str(error),
            "programmingLanguage": "python",
        }

    event = {
        "eventType": event_type,
        "eventTime": now,
        "run": {
            "runId": run_id,
            "facets": run_facets,
        },
        "job": {
            "namespace": JOB_NAMESPACE,
            "name": job_name,
            "facets": {},
        },
        "inputs": [{"namespace": d["namespace"], "name": d["name"], "facets": {}} for d in inputs],
        "outputs": [{"namespace": d["namespace"], "name": d["name"], "facets": {}} for d in outputs],
        "producer": "https://github.com/exchange-poc/controlm-sim",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
    }

    return event


def _post(event):
    """Post an event to both Agent proxy and direct Datadog API."""
    job = event["job"]["name"]
    etype = event["eventType"]
    run = event["run"]["runId"]

    # 1) Agent proxy
    try:
        resp = requests.post(
            AGENT_ENDPOINT,
            json=event,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        logger.info("OpenLineage %s for %s (run=%s) -> Agent HTTP %s", etype, job, run, resp.status_code)
    except requests.RequestException as exc:
        logger.warning("Agent proxy send failed: %s", exc)

    # 2) Direct API (ensures delivery even if Agent proxy doesn't forward)
    if DD_API_KEY:
        try:
            resp = requests.post(
                DIRECT_ENDPOINT,
                json=event,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {DD_API_KEY}",
                },
                timeout=10,
            )
            logger.info("OpenLineage %s for %s (run=%s) -> Direct API HTTP %s", etype, job, run, resp.status_code)
            if resp.status_code >= 400:
                logger.warning("Direct API error: %s %s", resp.status_code, resp.text[:500])
        except requests.RequestException as exc:
            logger.warning("Direct API send failed: %s", exc)


# ---------- public API ----------


def emit_start(job_name, run_id, parent_run_id=None, inputs=None, outputs=None, business_date=""):
    """Emit a START event."""
    event = _build_event("START", job_name, run_id, parent_run_id, inputs, outputs, business_date)
    _post(event)


def emit_complete(job_name, run_id, parent_run_id=None, inputs=None, outputs=None, business_date=""):
    """Emit a COMPLETE event."""
    event = _build_event("COMPLETE", job_name, run_id, parent_run_id, inputs, outputs, business_date)
    _post(event)


def emit_fail(job_name, run_id, parent_run_id=None, inputs=None, outputs=None, business_date="", error=None):
    """Emit a FAIL event."""
    event = _build_event("FAIL", job_name, run_id, parent_run_id, inputs, outputs, business_date, error=error)
    _post(event)
