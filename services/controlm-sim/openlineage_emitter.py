"""Emit OpenLineage events to Datadog (Agent proxy + direct API fallback)."""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import requests

from lineage_contract import (
    JOB_IO,
    JOB_NAMESPACE,
    PIPELINE_NAME,
    ORACLE_NS,
    SQLSERVER_NS,
    MYSQL_NS,
    MINIO_NS,
)

logger = logging.getLogger("controlm-sim.openlineage")

# Primary: Agent proxy
DD_AGENT_HOST = os.environ.get("DD_AGENT_HOST", "datadog-agent")
DD_TRACE_AGENT_PORT = os.environ.get("DD_TRACE_AGENT_PORT", "8126")
AGENT_ENDPOINT = f"http://{DD_AGENT_HOST}:{DD_TRACE_AGENT_PORT}/openlineage/api/v1/lineage"

# Fallback: Direct Datadog API
DD_SITE = os.environ.get("DD_SITE", "datadoghq.com")
DD_API_KEY = os.environ.get("DD_API_KEY", "")
DIRECT_ENDPOINT = f"https://data-obs-intake.{DD_SITE}/api/v1/lineage"

POSTGRES_NS     = "postgresql://demo-postgres/demo"
S3_NS           = MINIO_NS

# Map target_db short name → OpenLineage namespace
DB_NAMESPACE = {
    "oracle":    ORACLE_NS,
    "sqlserver": SQLSERVER_NS,
    "mysql":     MYSQL_NS,
    "postgres":  POSTGRES_NS,
}


def _event_time():
    """UTC timestamp in Datadog docs format.

    The Lineage UI is currently Preview and has shown timezone-window quirks
    with `+00:00`; the docs examples use a trailing `Z`.
    """
    shift_hours = float(os.environ.get("OL_EVENT_TIME_SHIFT_HOURS", "0"))
    now = datetime.now(timezone.utc) + timedelta(hours=shift_hours)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _get_io(job_name):
    """Return (inputs, outputs) for a job, falling back to empty lists."""
    mapping = JOB_IO.get(job_name, {"inputs": [], "outputs": []})
    return mapping["inputs"], mapping["outputs"]


def io_for_custom_job(job: dict) -> tuple[list, list]:
    """Derive inputs/outputs from a custom job spec (catalog record).

    Heuristics by job type:
      - etl_template:  src_table on Oracle → tgt_table on SQL Server
      - sql:           tables in FROM clause(s) → inputs; no outputs (read-only)
      - shell/python:  cannot infer datasets safely → return empty
    """
    import re
    jtype = job.get("type", "shell")
    inputs, outputs = [], []

    if jtype == "etl_template":
        src = job.get("src_table")
        tgt = job.get("tgt_table")
        if src:
            inputs.append({"namespace": ORACLE_NS, "name": src})
        if tgt:
            outputs.append({"namespace": SQLSERVER_NS, "name": tgt})

    elif jtype == "sql":
        db = (job.get("target_db") or "").lower()
        ns = DB_NAMESPACE.get(db)
        if ns and job.get("command"):
            q = job["command"]
            # naive FROM/JOIN extraction — good enough for demo lineage
            tables = set()
            for m in re.finditer(r"\b(?:FROM|JOIN)\s+([A-Za-z0-9_.]+)", q, re.IGNORECASE):
                tables.add(m.group(1))
            for t in sorted(tables):
                inputs.append({"namespace": ns, "name": t})

    return inputs, outputs


PRODUCER_URI = "https://github.com/b3-poc/controlm-sim"

# Hardcoded row counts per output dataset — matches the realistic sample data
# emitted by etl_oracle_to_sqlserver.py (500 trades, 50 derivative positions,
# 40 cash positions). Used in outputStatistics facet so Datadog renders Row Count.
_ROW_COUNTS = {
    "demopoc.dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO": 500,
    "demopoc.dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL": 50,
    "demopoc.dbo.ADWPM_POSICAO_MERCADO_A_VISTA": 40,
    "demopoc.dbo.ADWPM_DQ_RESULTS": 5,
    "XEPDB1.DEMOPOC.ASTADRVT_TRADE_MVMT": 500,
    "XEPDB1.DEMOPOC.ASTANO_FGBE_DRVT_PSTN": 50,
    "XEPDB1.DEMOPOC.ASTACASH_MRKT_PSTN": 40,
    "derivatives/ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO.parquet": 500,
    "derivatives/ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL.parquet": 50,
    "spot/ADWPM_POSICAO_MERCADO_A_VISTA.csv": 40,
}

# Minimal schema (column list) per dataset — enables column-level lineage hints.
# Keys are the dataset name; values are a list of (column_name, sql_type) tuples.
_SCHEMAS = {
    "XEPDB1.DEMOPOC.ASTADRVT_TRADE_MVMT": [
        ("trade_id", "varchar"), ("instrument_code", "varchar"),
        ("buy_participant", "varchar"), ("sell_participant", "varchar"),
        ("quantity", "decimal"), ("closing_price", "decimal"),
        ("gross_value", "decimal"), ("trade_dt", "date"),
        ("settlement_dt", "date"), ("status", "varchar"),
        ("business_date", "date"),
    ],
    "XEPDB1.DEMOPOC.ASTANO_FGBE_DRVT_PSTN": [
        ("position_id", "varchar"), ("position_date", "date"),
        ("participant_code", "varchar"), ("instrument_code", "varchar"),
        ("net_quantity", "decimal"), ("long_quantity", "decimal"),
        ("short_quantity", "decimal"), ("market_value", "decimal"),
        ("business_date", "date"),
    ],
    "XEPDB1.DEMOPOC.ASTACASH_MRKT_PSTN": [
        ("position_id", "varchar"), ("position_date", "date"),
        ("participant_code", "varchar"), ("instrument_code", "varchar"),
        ("net_quantity", "decimal"), ("settlement_value", "decimal"),
        ("business_date", "date"),
    ],
    "demopoc.dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO": [
        ("trade_id", "varchar"), ("ticker", "varchar"),
        ("notional", "decimal"), ("quantity", "decimal"),
        ("trade_dt", "date"), ("settlement_dt", "date"),
        ("counterparty_id", "varchar"), ("trader_id", "varchar"),
        ("desk_code", "varchar"), ("status", "varchar"),
    ],
    "demopoc.dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL": [
        ("position_id", "varchar"), ("instrument_id", "varchar"),
        ("participant_code", "varchar"), ("position_date", "date"),
        ("long_qty", "decimal"), ("short_qty", "decimal"),
        ("net_qty", "decimal"), ("settlement_price", "decimal"),
        ("margin_value", "decimal"),
    ],
    "demopoc.dbo.ADWPM_POSICAO_MERCADO_A_VISTA": [
        ("position_id", "varchar"), ("ticker", "varchar"),
        ("participant_code", "varchar"), ("position_date", "date"),
        ("long_value", "decimal"), ("short_value", "decimal"),
        ("net_value", "decimal"),
    ],
    "demopoc.dbo.ADWPM_DQ_RESULTS": [
        ("result_id", "varchar"), ("run_id", "varchar"),
        ("business_date", "date"), ("check_name", "varchar"),
        ("check_type", "varchar"), ("target_table", "varchar"),
        ("severity", "varchar"), ("passed", "boolean"),
    ],
}


def _schema_facet(dataset_name):
    """Returns a schema facet for a dataset, or None if not in catalog."""
    fields = _SCHEMAS.get(dataset_name)
    if not fields:
        return None
    return {
        "_producer": PRODUCER_URI,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
        "fields": [{"name": n, "type": t} for n, t in fields],
    }


def _output_statistics_facet(dataset_name):
    """Returns outputStatistics facet so Datadog renders Row Count for the dataset."""
    row_count = _ROW_COUNTS.get(dataset_name)
    if row_count is None:
        return None
    return {
        "_producer": PRODUCER_URI,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json",
        "rowCount": row_count,
        "size": row_count * 256,
    }


def _build_input_facets(dataset_name):
    facets = {}
    schema = _schema_facet(dataset_name)
    if schema:
        facets["schema"] = schema
    return facets


def _build_output_facets(dataset_name):
    facets = {}
    schema = _schema_facet(dataset_name)
    if schema:
        facets["schema"] = schema
    return facets


def _build_output_dataset_facets(dataset_name):
    """outputFacets sit on the output dataset itself (not in `facets`).
    Datadog reads outputStatistics here for Row Count."""
    facets = {}
    stats = _output_statistics_facet(dataset_name)
    if stats:
        facets["outputStatistics"] = stats
    return facets


def _aggregate_pipeline_io():
    """For the parent pipeline event, return the union of all child jobs'
    *external* inputs and *external* outputs (datasets that are NOT also
    produced/consumed by another job in the same pipeline).

    This makes the pipeline anchor show: external sources (Oracle) as upstream
    and final sinks (S3, DQ_RESULTS) as downstream — just like a Spark Application.
    """
    all_inputs  = set()
    all_outputs = set()
    for io in JOB_IO.values():
        for d in io.get("inputs", []):
            all_inputs.add((d["namespace"], d["name"]))
        for d in io.get("outputs", []):
            all_outputs.add((d["namespace"], d["name"]))

    external_inputs  = all_inputs  - all_outputs   # datasets read but never written
    external_outputs = all_outputs - all_inputs    # datasets written but never read
    return (
        [{"namespace": ns, "name": n} for ns, n in sorted(external_inputs)],
        [{"namespace": ns, "name": n} for ns, n in sorted(external_outputs)],
    )


def _build_event(event_type, job_name, run_id, parent_run_id, inputs, outputs, business_date, error=None):
    """Build an OpenLineage event dict."""
    now = _event_time()

    if inputs is None or outputs is None:
        default_inputs, default_outputs = _get_io(job_name)
        inputs  = inputs  if inputs  is not None else default_inputs
        outputs = outputs if outputs is not None else default_outputs

    run_facets = {
        "business_date": {
            "_producer": PRODUCER_URI,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/CustomFacet.json",
            "business_date": business_date,
        },
        "controlm": {
            "_producer": PRODUCER_URI,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/CustomFacet.json",
            "orchestrator": "control-m",
            "pipeline": PIPELINE_NAME,
            "pipelineRunId": parent_run_id,
            "wrapper": "python",
        },
    }

    if error and event_type == "FAIL":
        run_facets["errorMessage"] = {
            "_producer": PRODUCER_URI,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
            "message": str(error),
            "programmingLanguage": "python",
        }

    # Datadog Custom Jobs requires the jobType Job facet. Keep each Python job as
    # a standalone JOB so the lineage graph shows the real execution steps
    # instead of collapsing everything under the pipeline parent.
    job_facets = {
        "jobType": {
            "_producer": PRODUCER_URI,
            "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json",
            "processingType": "BATCH",
            "integration": "custom",
            "jobType": "JOB",
        },
    }

    def _build_input(d):
        return {
            "namespace": d["namespace"],
            "name": d["name"],
            "facets": _build_input_facets(d["name"]),
        }

    def _build_output(d):
        entry = {
            "namespace": d["namespace"],
            "name": d["name"],
            "facets": _build_output_facets(d["name"]),
        }
        out_facets = _build_output_dataset_facets(d["name"])
        if out_facets:
            entry["outputFacets"] = out_facets
        return entry

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
            "facets": job_facets,
        },
        "inputs":  [_build_input(d)  for d in inputs],
        "outputs": [_build_output(d) for d in outputs],
        "producer": PRODUCER_URI,
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
    }

    return event


def _post(event):
    """Post an event to both Agent proxy and direct Datadog API."""
    job = event["job"]["name"]
    etype = event["eventType"]
    run = event["run"]["runId"]
    agent_status = direct_status = None

    # 1) Agent proxy
    try:
        resp = requests.post(
            AGENT_ENDPOINT,
            json=event,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        agent_status = resp.status_code
        logger.info("OpenLineage %s for %s (run=%s) -> Agent HTTP %s", etype, job, run, agent_status)
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
            direct_status = resp.status_code
            logger.info("OpenLineage %s for %s (run=%s) -> Direct API HTTP %s", etype, job, run, direct_status)
            if resp.status_code >= 400:
                logger.warning("Direct API error: %s %s", resp.status_code, resp.text[:500])
        except requests.RequestException as exc:
            logger.warning("Direct API send failed: %s", exc)

    # 3) Emit a DogStatsD counter so the user can confirm delivery in <2 min
    # via Metrics Explorer instead of waiting ~15min for Lineage UI to index.
    # Tags: job_name, namespace, event_type, run_id_hash (short).
    _emit_freshness_metric(job, JOB_NAMESPACE, etype, run, agent_status, direct_status)


# ── DogStatsD freshness/health metric ────────────────────────────────────────
# Emits b3.ol.events_total{...}. Metrics index in ~1 min in the Datadog SaaS,
# so this gives much faster feedback than the Lineage UI (~15-30 min).
try:
    from datadog import statsd as _statsd  # already initialized by controlm_metrics
except ImportError:
    _statsd = None


def _emit_freshness_metric(job_name, namespace, event_type, run_id, agent_status, direct_status):
    if _statsd is None:
        return
    run_hash = (run_id or "")[:8]
    tags = [
        f"job_name:{job_name}",
        f"ol_namespace:{namespace}",
        f"event_type:{event_type}",
        f"run_id_hash:{run_hash}",
        f"agent_ok:{1 if agent_status == 201 else 0}",
        f"direct_ok:{1 if direct_status == 201 else 0}",
    ]
    try:
        _statsd.increment("b3.ol.events_total", tags=tags)
    except Exception as exc:
        logger.warning("freshness metric emit failed: %s", exc)


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
