"""
run_job.py — Control-M Workbench job wrapper
Emits OpenLineage START / COMPLETE / FAIL to Datadog Agent, then executes
the real pipeline job inside the pipeline-runner container.

Called by Control-M as:
  python /scripts/run_job.py --job <name> --date <YYYY-MM-DD>

Example (Control-M job command):
  python /scripts/run_job.py --job close_market_eod --date %%ODATE%%
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone

import requests

# ── Service identity ────────────────────────────────────────────────────────
DD_SERVICE  = os.environ.get("DD_SERVICE",  "pythonwrapper")
DD_ENV      = os.environ.get("DD_ENV",      "demo")
DD_VERSION  = os.environ.get("DD_VERSION",  "1.0.0")

# ── Datadog APM Tracer ──────────────────────────────────────────────────────
try:
    from ddtrace import tracer as dd_tracer
    from ddtrace import config as dd_config
    dd_config.service = DD_SERVICE
    dd_config.env     = DD_ENV
    dd_config.version = DD_VERSION
    _TRACER_AVAILABLE = True
except ImportError:
    _TRACER_AVAILABLE = False


class _DDJsonFormatter(logging.Formatter):
    """JSON log formatter with APM trace correlation for Datadog Log Management."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "message":   record.getMessage(),
            "logger":    record.name,
            # Datadog unified service tagging
            "service": DD_SERVICE,
            "env":     DD_ENV,
            "version": DD_VERSION,
        }

        # APM trace/span correlation — injected when a trace is active
        if _TRACER_AVAILABLE:
            span = dd_tracer.current_span()
            if span:
                log_entry["dd.trace_id"] = str(span.trace_id)
                log_entry["dd.span_id"]  = str(span.span_id)
                log_entry["dd.service"]  = DD_SERVICE
                log_entry["dd.env"]      = DD_ENV
                log_entry["dd.version"]  = DD_VERSION

        # Propagate any extra fields attached to the log record (Python 3.6 safe)
        try:
            for key, val in record.__dict__.items():
                if key.startswith("dd_") or key.startswith("job_") or key.startswith("ol_"):
                    log_entry[key] = val
        except Exception:
            pass

        if record.exc_info:
            log_entry["error.stack"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False)


# ── Datadog HTTP Logs handler (bypasses CTM stdout capture) ─────────────────
_LOG_BUFFER: list = []  # accumulates records to flush in batch at exit

class _DatadogHttpLogsHandler(logging.Handler):
    """Sends log records DIRECTLY to Datadog HTTP logs intake.
    CTM captures job stdout, so stdout never reaches the DD agent.
    This handler bypasses that by POSTing directly to the intake API.
    Records are buffered and flushed at the end of the job for efficiency.
    """
    def emit(self, record: logging.LogRecord) -> None:
        try:
            _LOG_BUFFER.append(json.loads(self.format(record)))
        except Exception:
            pass

    @staticmethod
    def flush_to_datadog() -> None:
        """Call once at job exit to send all buffered log records."""
        if not _LOG_BUFFER or not os.environ.get("DD_API_KEY"):
            return
        site = os.environ.get("DD_SITE", "datadoghq.com")
        url  = f"https://http-intake.logs.{site}/api/v2/logs"
        try:
            import requests as _req
            body = json.dumps(_LOG_BUFFER, ensure_ascii=True).encode("utf-8")
            resp = _req.post(
                url,
                data=body,
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "DD-API-KEY":   os.environ["DD_API_KEY"],
                },
                timeout=10,
            )
            sys.stdout.write("[wrapper] Sent {} log records -> Datadog HTTP {} \n".format(
                len(_LOG_BUFFER), resp.status_code))
            sys.stdout.flush()
        except Exception as exc:
            sys.stdout.write("[wrapper] Log flush err: {}\n".format(str(exc)[:120]))
            sys.stdout.flush()

# Root logger: stdout (for CTM output) + Datadog HTTP intake
import io as _io
_stdout_handler = logging.StreamHandler(
    _io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stdout, "buffer") else sys.stdout
)
_stdout_handler.setFormatter(_DDJsonFormatter())

_dd_http_handler = _DatadogHttpLogsHandler()
_dd_http_handler.setFormatter(_DDJsonFormatter())

logging.root.handlers = [_stdout_handler, _dd_http_handler]
logging.root.setLevel(logging.INFO)
log = logging.getLogger("pythonwrapper")

# ── Datadog Agent endpoint ──────────────────────────────────────────────────
DD_AGENT_HOST = os.environ.get("DD_AGENT_HOST", "datadog-agent")
DD_TRACE_PORT = os.environ.get("DD_TRACE_AGENT_PORT", "8126")
AGENT_URL = f"http://{DD_AGENT_HOST}:{DD_TRACE_PORT}/openlineage/api/v1/lineage"

# Fallback: direct Datadog intake
DD_SITE = os.environ.get("DD_SITE", "datadoghq.com")
DD_API_KEY = os.environ.get("DD_API_KEY", "")
DIRECT_URL = f"https://data-obs-intake.{DD_SITE}/api/v1/lineage"

JOB_NAMESPACE = "exchange-poc"
ORACLE_NS    = "oracle://demoorg-oracle:1521/FREEPDB1"    # Oracle 23c Free PDB (ASTA* tables)
SQLSERVER_NS = "sqlserver://demoorg-sqlserver:1433/demopoc"  # SQL Server DW (ADWPM* tables)
MYSQL_NS     = "mysql://demo-mysql:3306/exchange"                 # MySQL staging layer (fallback)
MINIO_NS     = "s3://mock-exchange"                             # MinIO = S3 simulation

# HTTP trigger server in the pipeline-runner container
PIPELINE_TRIGGER_HOST = os.environ.get("PIPELINE_TRIGGER_HOST", "demo-pipeline-runner")
PIPELINE_TRIGGER_PORT = os.environ.get("PIPELINE_TRIGGER_PORT", "5002")  # trigger server port

# ── Dataset I/O map ─────────────────────────────────────────────────────────
# Reflects real exchange table names in Oracle (ASTA*) and SQL Server DW (ADWPM*).
# OpenLineage lineage always shows the real table names regardless of fallback.
JOB_IO = {
    "close_market_eod": {
        "inputs": [
            {"namespace": ORACLE_NS, "name": "DEMOPOC.ASTADRVT_TRADE_MVMT"},
        ],
        "outputs": [
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO"},
        ],
    },
    "reconcile_d1_positions": {
        "inputs": [
            {"namespace": ORACLE_NS, "name": "DEMOPOC.ASTANO_FGBE_DRVT_PSTN"},
            {"namespace": ORACLE_NS, "name": "DEMOPOC.ASTACASH_MRKT_PSTN"},
        ],
        "outputs": [
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL"},
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_POSICAO_MERCADO_A_VISTA"},
        ],
    },
    "quality_gate_d1": {
        "inputs": [
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO"},
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL"},
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_POSICAO_MERCADO_A_VISTA"},
        ],
        "outputs": [
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_DQ_RESULTS"},
        ],
    },
    "publish_d1_reports": {
        "inputs": [
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO"},
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL"},
            {"namespace": SQLSERVER_NS, "name": "dbo.ADWPM_POSICAO_MERCADO_A_VISTA"},
        ],
        "outputs": [
            {"namespace": MINIO_NS, "name": "derivatives/ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO"},
            {"namespace": MINIO_NS, "name": "derivatives/ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL"},
            {"namespace": MINIO_NS, "name": "spot/ADWPM_POSICAO_MERCADO_A_VISTA"},
        ],
    },
}


PIPELINE_NAME = "market_d1_pipeline"


def _build_event(event_type, job_name, run_id, business_date, parent_run_id=None, error=None):
    io = JOB_IO.get(job_name, {"inputs": [], "outputs": []})
    run_facets = {
        "business_date": {
            "_producer": "https://github.com/exchange-poc/controlm-workbench",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/CustomFacet.json",
            "business_date": business_date,
        }
    }
    # Parent run facet — groups all 4 jobs under the same pipeline run in Datadog DJM
    if parent_run_id:
        run_facets["parent"] = {
            "_producer": "https://github.com/exchange-poc/controlm-workbench",
            "_schemaURL": "https://openlineage.io/spec/facets/2-0-2/ParentRunFacet.json",
            "run": {"runId": parent_run_id},
            "job": {"namespace": JOB_NAMESPACE, "name": PIPELINE_NAME},
        }
    if error:
        run_facets["errorMessage"] = {
            "_producer": "https://github.com/exchange-poc/controlm-workbench",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
            "message": str(error),
            "programmingLanguage": "python",
        }
    job_facets = {
        "jobType": {
            "_producer": "https://github.com/exchange-poc/controlm-workbench",
            "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/JobTypeJobFacet.json",
            "processingType": "BATCH",
            "integration": "CONTROL_M",
            "jobType": "JOB",
        }
    }
    return {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {"runId": run_id, "facets": run_facets},
        "job": {"namespace": JOB_NAMESPACE, "name": job_name, "facets": job_facets},
        "inputs":  [{"namespace": d["namespace"], "name": d["name"], "facets": {}} for d in io["inputs"]],
        "outputs": [{"namespace": d["namespace"], "name": d["name"], "facets": {}} for d in io["outputs"]],
        "producer": "https://github.com/exchange-poc/controlm-workbench",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
    }


def _emit(event):
    """Send OpenLineage event — non-blocking, never raises."""
    job = event["job"]["name"]
    etype = event["eventType"]

    # 1) Agent proxy
    try:
        r = requests.post(AGENT_URL, json=event, headers={"Content-Type": "application/json"}, timeout=5)
        log.info("OL %s for %s -> Agent HTTP %s", etype, job, r.status_code)
    except Exception as exc:
        log.warning("Agent proxy unavailable: %s", exc)

    # 2) Direct API fallback
    if DD_API_KEY:
        try:
            r = requests.post(
                DIRECT_URL,
                json=event,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {DD_API_KEY}"},
                timeout=5,
            )
            log.info("OL %s for %s -> Direct API HTTP %s", etype, job, r.status_code)
        except Exception as exc:
            log.warning("Direct API unavailable: %s", exc)


def _run_job(job_name, business_date):
    """Trigger job execution via HTTP to the pipeline-runner trigger server.
    Injects Datadog distributed trace headers so the pipeline-runner can
    continue the same trace (correlated in APM and Log Management).
    """
    url = "http://{}:{}/run/{}?date={}".format(
        PIPELINE_TRIGGER_HOST, PIPELINE_TRIGGER_PORT, job_name, business_date
    )
    log.info("Triggering job via HTTP: %s", url)

    # Inject trace propagation headers if ddtrace is active
    trace_headers = {}
    if _TRACER_AVAILABLE:
        try:
            from ddtrace.propagation.http import HTTPPropagator
            HTTPPropagator.inject(dd_tracer.current_trace_context(), trace_headers)
        except Exception:
            pass

    try:
        req = requests.post(url, headers=trace_headers, timeout=900)
        log.info("Trigger response: HTTP %s - %s", req.status_code, req.text[:200])
        return 0 if req.status_code == 200 else 1
    except Exception as exc:
        log.error("Trigger request failed: %s", exc)
        return 1


def main():
    parser = argparse.ArgumentParser(description="Control-M job wrapper with OpenLineage")
    parser.add_argument("--job",  required=True, help="Job name (e.g. close_market_eod)")
    parser.add_argument("--date", required=True, help="Business date YYYY-MM-DD (%%ODATE%% in CTM)")
    args = parser.parse_args()

    job_name = args.job
    business_date = args.date

    if job_name not in JOB_IO:
        log.error("Unknown job: %s. Valid jobs: %s", job_name, list(JOB_IO.keys()))
        sys.exit(1)

    import hashlib
    # run_id: unique per job execution
    run_id = str(hashlib.md5(f"{job_name}-{business_date}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M')}".encode()).hexdigest())
    run_id = f"{run_id[:8]}-{run_id[8:12]}-{run_id[12:16]}-{run_id[16:20]}-{run_id[20:32]}"

    # parent_run_id: same for all 4 jobs in the same date+hour → groups them as one pipeline run
    parent_run_id = str(hashlib.md5(f"{PIPELINE_NAME}-{business_date}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H')}".encode()).hexdigest())
    parent_run_id = f"{parent_run_id[:8]}-{parent_run_id[8:12]}-{parent_run_id[12:16]}-{parent_run_id[16:20]}-{parent_run_id[20:32]}"

    # Attach job context to every subsequent log record via LoggerAdapter
    extra = {
        "job_name":       job_name,
        "business_date":  business_date,
        "ol_run_id":      run_id,
        "ol_parent_run_id": parent_run_id,
        "ol_namespace":   JOB_NAMESPACE,
        "ol_pipeline":    PIPELINE_NAME,
    }
    _base_log = logging.getLogger("pythonwrapper")
    log_ctx = logging.LoggerAdapter(_base_log, extra)
    log_ctx.info("CTM job started")

    # ── APM trace wrapping the full job ──────────────────────────────────────
    def _run_with_trace():
        if _TRACER_AVAILABLE:
            with dd_tracer.trace(
                "ctm.job.run",
                service="demo-controlm",
                resource=job_name,
                span_type="worker",
            ) as span:
                span.set_tag("job.name", job_name)
                span.set_tag("business.date", business_date)
                span.set_tag("ol.run_id", run_id)
                span.set_tag("ol.parent_run_id", parent_run_id)
                span.set_tag("ol.namespace", JOB_NAMESPACE)
                span.set_tag("ol.pipeline", PIPELINE_NAME)
                return _run_job_traced(span, job_name, business_date, run_id, parent_run_id)
        else:
            return _run_job_simple(job_name, business_date, run_id, parent_run_id)

    def _run_job_traced(span, jn, bd, rid, pid):
        _emit(_build_event("START", jn, rid, bd, parent_run_id=pid))
        exit_code = _run_job(jn, bd)
        if exit_code == 0:
            _emit(_build_event("COMPLETE", jn, rid, bd, parent_run_id=pid))
            log.info("=== %s COMPLETE ===", jn)
        else:
            span.error = 1
            span.set_tag("error.msg", f"Job exited with code {exit_code}")
            _emit(_build_event("FAIL", jn, rid, bd, parent_run_id=pid, error=f"exit {exit_code}"))
            log.error("=== %s FAILED (exit %s) ===", jn, exit_code)
        return exit_code

    def _run_job_simple(jn, bd, rid, pid):
        _emit(_build_event("START", jn, rid, bd, parent_run_id=pid))
        exit_code = _run_job(jn, bd)
        if exit_code == 0:
            _emit(_build_event("COMPLETE", jn, rid, bd, parent_run_id=pid))
            log.info("=== %s COMPLETE ===", jn)
        else:
            _emit(_build_event("FAIL", jn, rid, bd, parent_run_id=pid, error=f"exit {exit_code}"))
            log.error("=== %s FAILED (exit %s) ===", jn, exit_code)
        return exit_code

    exit_code = _run_with_trace()

    # Flush all buffered log records to Datadog HTTP logs intake
    _DatadogHttpLogsHandler.flush_to_datadog()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
