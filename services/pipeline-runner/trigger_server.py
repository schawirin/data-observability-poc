"""
trigger_server.py — Minimal HTTP server to trigger pipeline jobs.
Listens on 0.0.0.0:5001, accepts:
  POST /run/<job_name>?date=YYYY-MM-DD

Called by run_job.py in the Control-M Workbench container.

Connection strategy:
  - Primary: SQL Server DW (demopoc) — used by all ADWPM* jobs
  - Fallback: MySQL staging layer — used if SQL Server is unreachable
"""

import importlib
import json
import os
import time
import traceback
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

# ── Datadog APM Tracer (ddtrace 4.6.1 installed) ────────────────────────────
try:
    from ddtrace import tracer as dd_tracer, config as dd_config, patch
    from ddtrace.propagation.http import HTTPPropagator

    dd_config.service = "demo-pipeline-runner"
    dd_config.env     = os.environ.get("DD_ENV",     "demo")
    dd_config.version = os.environ.get("DD_VERSION", "1.0.0")

    # Auto-instrument MySQL and requests
    patch(mysql=True, requests=True)
    _TRACER_OK = True
except Exception:
    _TRACER_OK = False
    HTTPPropagator = None

JOBS = {
    "close_market_eod",
    "reconcile_d1_positions",
    "quality_gate_d1",
    "publish_d1_reports",
}

# ── OpenLineage emission (Agent proxy → direct API fallback) ──────────────────

_DD_AGENT_HOST  = os.environ.get("DD_AGENT_HOST", "dd-agent")
_DD_TRACE_PORT  = os.environ.get("DD_TRACE_AGENT_PORT", "8126")
_DD_SITE        = os.environ.get("DD_SITE", "datadoghq.com")
_DD_API_KEY     = os.environ.get("DD_API_KEY", "")
_OL_NS          = os.environ.get("OL_NAMESPACE", "exchange-poc")
_OL_PIPELINE    = os.environ.get("OL_PIPELINE",  "market_d1_pipeline")
_OL_PRODUCER    = "https://github.com/exchange-poc/pipeline-runner"
_OL_SCHEMA      = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
_AGENT_OL_URL   = f"http://{_DD_AGENT_HOST}:{_DD_TRACE_PORT}/openlineage/api/v1/lineage"
_DIRECT_OL_URL  = f"https://data-obs-intake.{_DD_SITE}/api/v1/lineage"

# Dataset namespaces
_ORACLE_NS  = f"oracle://{os.environ.get('ORACLE_HOST','demo-oracle')}:1521/{os.environ.get('ORACLE_SERVICE','XEPDB1')}"
_SS_NS      = f"sqlserver://{os.environ.get('SQLSERVER_HOST','demo-sqlserver')}:1433/{os.environ.get('SQLSERVER_DATABASE','demopoc')}"
_S3_NS      = "s3://mock-exchange"

_JOB_IO = {
    "close_market_eod":       {"inputs": [(_ORACLE_NS, "DEMOPOC.ASTADRVT_TRADE_MVMT")],         "outputs": [(_SS_NS, "dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO")]},
    "reconcile_d1_positions": {"inputs": [(_ORACLE_NS, "DEMOPOC.ASTANO_FGBE_DRVT_PSTN"), (_ORACLE_NS, "DEMOPOC.ASTACASH_MRKT_PSTN")], "outputs": [(_SS_NS, "dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL"), (_SS_NS, "dbo.ADWPM_POSICAO_MERCADO_A_VISTA")]},
    "quality_gate_d1":        {"inputs": [(_SS_NS, "dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO")], "outputs": [(_SS_NS, "dbo.ADWPM_DQ_RESULTS")]},
    "publish_d1_reports":     {"inputs": [(_SS_NS, "dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO")], "outputs": [(_S3_NS, "derivatives/ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO")]},
}

_pipeline_run_id = str(uuid.uuid4())  # reset per server start


def _ol_emit(event: dict) -> None:
    """Fire-and-forget OL emission — never blocks job execution."""
    import requests as _req
    payload = json.dumps(event, default=str).encode()
    headers = {"Content-Type": "application/json"}
    try:
        r = _req.post(_AGENT_OL_URL, data=payload, headers=headers, timeout=3)
        if r.status_code not in (200, 201, 202) and _DD_API_KEY:
            _req.post(_DIRECT_OL_URL, data=payload,
                      headers={**headers, "Authorization": f"Bearer {_DD_API_KEY}"}, timeout=5)
    except Exception:
        if _DD_API_KEY:
            try:
                _req.post(_DIRECT_OL_URL, data=payload,
                          headers={**headers, "Authorization": f"Bearer {_DD_API_KEY}"}, timeout=5)
            except Exception:
                pass


def _ol_job_start(job_name: str, run_id: str, business_date: str) -> None:
    io = _JOB_IO.get(job_name, {})
    _ol_emit({
        "eventType": "START",
        "eventTime": _now(),
        "producer": _OL_PRODUCER,
        "schemaURL": _OL_SCHEMA,
        "run": {"runId": run_id, "facets": {
            "parent": {"_producer": _OL_PRODUCER,
                       "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
                       "run": {"runId": _pipeline_run_id},
                       "job": {"namespace": _OL_NS, "name": _OL_PIPELINE}},
            "nominalTime": {"_producer": _OL_PRODUCER,
                            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/NominalTimeRunFacet.json",
                            "nominalStartTime": f"{business_date}T00:00:00+00:00"},
        }},
        "job": {"namespace": _OL_NS, "name": job_name,
                "facets": {"jobType": {"_producer": _OL_PRODUCER,
                                       "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/JobTypeJobFacet.json",
                                       "processingType": "BATCH", "integration": "CONTROL_M", "jobType": "JOB"}}},
        "inputs":  [{"namespace": ns, "name": name, "facets": {}} for ns, name in io.get("inputs", [])],
        "outputs": [{"namespace": ns, "name": name, "facets": {}} for ns, name in io.get("outputs", [])],
    })


def _ol_job_end(job_name: str, run_id: str, business_date: str, success: bool,
                duration_s: float, result: dict) -> None:
    io = _JOB_IO.get(job_name, {})
    event_type = "COMPLETE" if success else "FAIL"
    run_facets: dict = {
        "parent": {"_producer": _OL_PRODUCER,
                   "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
                   "run": {"runId": _pipeline_run_id},
                   "job": {"namespace": _OL_NS, "name": _OL_PIPELINE}},
    }
    if not success and result.get("error"):
        run_facets["errorMessage"] = {
            "_producer": _OL_PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
            "message": str(result.get("error", ""))[:500],
            "programmingLanguage": "Python",
        }
    # Output stats facet for row counts
    output_facets = {}
    if result.get("rows_written") is not None:
        output_facets["outputStatistics"] = {
            "_producer": _OL_PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json",
            "rowCount": result.get("rows_written", 0),
        }
    _ol_emit({
        "eventType": event_type,
        "eventTime": _now(),
        "producer": _OL_PRODUCER,
        "schemaURL": _OL_SCHEMA,
        "run": {"runId": run_id, "facets": run_facets},
        "job": {"namespace": _OL_NS, "name": job_name,
                "facets": {"jobType": {"_producer": _OL_PRODUCER,
                                       "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/JobTypeJobFacet.json",
                                       "processingType": "BATCH", "integration": "CONTROL_M", "jobType": "JOB"}}},
        "inputs":  [{"namespace": ns, "name": name, "facets": {}} for ns, name in io.get("inputs", [])],
        "outputs": [{"namespace": ns, "name": name, "facets": output_facets} for ns, name in io.get("outputs", [])],
    })


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

# ── SQL Server DW connection settings ────────────────────────────────────────
SQLSERVER_HOST     = os.environ.get("SQLSERVER_HOST",     "demoorg-sqlserver")
SQLSERVER_PORT     = int(os.environ.get("SQLSERVER_PORT", "1433"))
SQLSERVER_USER     = os.environ.get("SQLSERVER_USER",     "sa")
SQLSERVER_PASSWORD = os.environ.get("SQLSERVER_PASSWORD", "SqlDemo12345!")
SQLSERVER_DATABASE = os.environ.get("SQLSERVER_DATABASE", "demopoc")

# ── MySQL fallback connection settings ───────────────────────────────────────
MYSQL_HOST     = os.environ.get("MYSQL_HOST",     "demo-mysql")
MYSQL_PORT     = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER     = os.environ.get("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "demopoc2026")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "exchange")


def _get_sqlserver_conn():
    """Establish and return a SQL Server connection using env vars."""
    import pymssql
    return pymssql.connect(
        server=SQLSERVER_HOST,
        port=SQLSERVER_PORT,
        user=SQLSERVER_USER,
        password=SQLSERVER_PASSWORD,
        database=SQLSERVER_DATABASE,
    )


def _get_mysql_conn():
    """Establish and return a MySQL connection (fallback) using env vars."""
    import mysql.connector
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def _get_conn():
    """
    Return a SQL Server connection (primary).
    Falls back to MySQL if SQL Server is unreachable.
    Returns (conn, conn_type) where conn_type is 'sqlserver' or 'mysql_fallback'.
    """
    try:
        conn = _get_sqlserver_conn()
        print("[trigger] connected to SQL Server DW", flush=True)
        return conn, "sqlserver"
    except Exception as exc:
        print(
            f"[trigger] SQL Server unavailable ({exc}), falling back to MySQL",
            flush=True,
        )
        conn = _get_mysql_conn()
        print("[trigger] connected to MySQL (fallback)", flush=True)
        return conn, "mysql_fallback"


def _run_job(job_name, business_date, parent_ctx=None):
    """
    Import the job module, establish a SQL Server (or MySQL fallback) connection,
    and call mod.run(conn, business_date).  Returns a JSON-serialisable dict.
    Wraps execution in a ddtrace span when available, continuing the parent trace
    propagated from run_job.py (Control-M wrapper).
    """
    mod = importlib.import_module(f"jobs.{job_name}")
    conn, conn_type = _get_conn()

    def _execute():
        result = mod.run(conn, business_date)
        return {
            "job": job_name,
            "date": business_date,
            "exit_code": 0,
            "conn_type": conn_type,
            "result": result,
        }

    try:
        if _TRACER_OK and parent_ctx is not None:
            # Continue the distributed trace from the Control-M Python wrapper
            with dd_tracer.start_span(
                "pipeline.job.execute",
                service="demo-pipeline-runner",
                resource=job_name,
                child_of=parent_ctx,
            ) as span:
                span.set_tag("job.name", job_name)
                span.set_tag("business.date", business_date)
                span.set_tag("db.conn_type", conn_type)
                span.set_tag("env", os.environ.get("DD_ENV", "demo"))
                return _execute()
        else:
            return _execute()
    finally:
        try:
            conn.close()
        except Exception:
            pass


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # silence default logging
        pass

    def do_POST(self):
        parsed = urlparse(self.path)
        parts = parsed.path.strip("/").split("/")
        if len(parts) != 2 or parts[0] != "run":
            self._respond(404, "Not found")
            return

        job_name = parts[1]
        if job_name not in JOBS:
            self._respond(400, f"Unknown job: {job_name}")
            return

        qs = parse_qs(parsed.query)
        business_date = qs.get("date", [None])[0]
        if not business_date:
            self._respond(400, "Missing ?date=YYYY-MM-DD")
            return

        print(f"[trigger] running {job_name} date={business_date}", flush=True)

        # Extract distributed trace context propagated by run_job.py (Control-M wrapper)
        parent_ctx = None
        if _TRACER_OK and HTTPPropagator is not None:
            try:
                headers_dict = {k: v for k, v in self.headers.items()}
                parent_ctx = HTTPPropagator.extract(headers_dict)
            except Exception:
                pass

        run_id = str(uuid.uuid4())
        t0 = time.time()
        _ol_job_start(job_name, run_id, business_date)

        try:
            result = _run_job(job_name, business_date, parent_ctx=parent_ctx)
            duration = round(time.time() - t0, 2)
            _ol_job_end(job_name, run_id, business_date, True, duration, result.get("result") or {})
            print(f"[trigger] {job_name} OK — {result}", flush=True)
            self._respond(200, json.dumps(result))
        except Exception:
            duration = round(time.time() - t0, 2)
            tb = traceback.format_exc()
            _ol_job_end(job_name, run_id, business_date, False, duration, {"error": tb[-300:]})
            print(f"[trigger] {job_name} ERROR:\n{tb}", flush=True)
            payload = json.dumps({"exit_code": 1, "error": tb[-800:]})
            self._respond(500, payload)

    def _respond(self, code, body):
        payload = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    port = int(os.environ.get("TRIGGER_PORT", "5001"))
    print(f"[trigger] listening on 0.0.0.0:{port}", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
