"""
pipeline_finalizer.py — Emits a pipeline-level OpenLineage COMPLETE or FAIL event
at the end of execute_pipeline(), consolidating all job statuses + DQ results.

Called by executor.py after all jobs finish.
Reuses openlineage_emitter for dual-delivery (Agent proxy → direct API fallback).
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import requests

logger = logging.getLogger("controlm-sim.pipeline_finalizer")

DD_AGENT_HOST     = os.environ.get("DD_AGENT_HOST", "datadog-agent")
DD_TRACE_PORT     = os.environ.get("DD_TRACE_AGENT_PORT", "8126")
DD_SITE           = os.environ.get("DD_SITE", "datadoghq.com")
DD_API_KEY        = os.environ.get("DD_API_KEY", "")
AGENT_URL         = f"http://{DD_AGENT_HOST}:{DD_TRACE_PORT}/openlineage/api/v1/lineage"
DIRECT_URL        = f"https://data-obs-intake.{DD_SITE}/api/v1/lineage"

PIPELINE_NAME     = os.environ.get("OL_PIPELINE", "market_d1_pipeline")
PIPELINE_NS       = os.environ.get("OL_NAMESPACE", "exchange-poc")
OL_PRODUCER       = "https://github.com/exchange-poc/controlm-sim"
OL_SCHEMA         = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"


def _emit(event: dict) -> bool:
    """Send OL event — Agent proxy first, direct API as fallback."""
    payload = json.dumps(event, default=str)
    ok = False

    try:
        r = requests.post(
            AGENT_URL,
            data=payload.encode(),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        if r.status_code in (200, 201, 202):
            logger.info("OL pipeline %s → Agent proxy HTTP %s", event["eventType"], r.status_code)
            ok = True
        else:
            logger.warning("OL pipeline Agent: HTTP %s — %s", r.status_code, r.text[:100])
    except Exception as exc:
        logger.warning("OL pipeline Agent unavailable (%s), trying direct", exc)

    if not ok and DD_API_KEY:
        try:
            r = requests.post(
                DIRECT_URL,
                data=payload.encode(),
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {DD_API_KEY}"},
                timeout=10,
            )
            if r.status_code in (200, 201, 202):
                logger.info("OL pipeline %s → Direct API HTTP %s", event["eventType"], r.status_code)
                ok = True
            else:
                logger.warning("OL pipeline direct: HTTP %s", r.status_code)
        except Exception as exc:
            logger.warning("OL pipeline direct error: %s", exc)

    return ok


def emit_pipeline_complete(pipeline_result: dict) -> bool:
    """
    Emit a pipeline-level COMPLETE or FAIL OpenLineage event.

    Args:
        pipeline_result: dict returned by execute_pipeline(), containing:
            - status: 'success' | 'failed'
            - run_id: pipeline run UUID
            - business_date: YYYY-MM-DD
            - duration_seconds: float
            - jobs: { job_name: { status, duration_seconds, retries, sla_miss, result } }
    """
    try:
        status        = pipeline_result.get("status", "failed")
        run_id        = pipeline_result.get("run_id") or str(uuid.uuid4())
        business_date = pipeline_result.get("business_date", datetime.now().strftime("%Y-%m-%d"))
        jobs          = pipeline_result.get("jobs", {})
        now           = datetime.now(timezone.utc).isoformat()

        event_type = "COMPLETE" if status == "success" else "FAIL"

        # Build per-job summary for run facets
        jobs_summary = {}
        for job_name, job_data in jobs.items():
            jobs_summary[job_name] = {
                "status":           job_data.get("status", "unknown"),
                "duration_seconds": job_data.get("duration_seconds"),
                "retries":          job_data.get("retries", 0),
                "sla_miss":         job_data.get("sla_miss", False),
            }

        # Extract DQ results if available
        qg_result = jobs.get("quality_gate_d1", {}).get("result") or {}
        dq_summary = {
            "gate_status":       qg_result.get("gate_status", "UNKNOWN"),
            "checks_run":        qg_result.get("checks_run", 0),
            "checks_failed":     qg_result.get("checks_failed", 0),
            "critical_failures": qg_result.get("critical_failures", 0),
        }

        run_facets = {
            "nominalTime": {
                "_producer":   OL_PRODUCER,
                "_schemaURL":  "https://openlineage.io/spec/facets/1-0-0/NominalTimeRunFacet.json",
                "nominalStartTime": f"{business_date}T00:00:00+00:00",
            },
            "pipelineExecution": {
                "_producer":   OL_PRODUCER,
                "_schemaURL":  OL_PRODUCER + "/facets/PipelineExecutionFacet.json",
                "business_date":    business_date,
                "total_duration_s": pipeline_result.get("duration_seconds"),
                "jobs":             jobs_summary,
                "dq":               dq_summary,
            },
        }

        if event_type == "FAIL":
            # Find first failed job for error message
            failed_job = next(
                (name for name, j in jobs.items() if j.get("status") == "failed"),
                "unknown"
            )
            run_facets["errorMessage"] = {
                "_producer":    OL_PRODUCER,
                "_schemaURL":   "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
                "message":      f"Pipeline failed at job: {failed_job}",
                "programmingLanguage": "Python",
            }

        event = {
            "eventType":  event_type,
            "eventTime":  now,
            "producer":   OL_PRODUCER,
            "schemaURL":  OL_SCHEMA,
            "run": {
                "runId":  run_id,
                "facets": run_facets,
            },
            "job": {
                "namespace": PIPELINE_NS,
                "name":      PIPELINE_NAME,
                "facets": {
                    "jobType": {
                        "_producer":   OL_PRODUCER,
                        "_schemaURL":  "https://openlineage.io/spec/facets/1-1-1/JobTypeJobFacet.json",
                        "processingType": "BATCH",
                        "integration":    "CONTROL_M",
                        "jobType":        "PIPELINE",
                    }
                },
            },
            "inputs":  [],
            "outputs": [],
        }

        result = _emit(event)
        if result:
            logger.info(
                "Pipeline OL %s emitted: %s | gate=%s | checks_failed=%d | duration=%ss",
                event_type,
                PIPELINE_NAME,
                dq_summary["gate_status"],
                dq_summary["checks_failed"],
                pipeline_result.get("duration_seconds", "?"),
            )
        return result

    except Exception as exc:
        logger.warning("pipeline_finalizer emit error: %s", exc)
        return False
