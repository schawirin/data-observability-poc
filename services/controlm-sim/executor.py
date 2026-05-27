"""
executor.py — Job executor with retry logic, SLA tracking, tracing,
OpenLineage emission, and Control-M DogStatsD metrics.

Orchestrates the D+1 pipeline:
  Oracle ASTA* → [Control-M jobs] → SQL Server ADWPM* → MinIO exports

Emits:
  - OpenLineage events (START/COMPLETE/FAIL) for Data Jobs Monitoring
  - Control-M metrics via DogStatsD (job duration, status, SLA, retries)
  - APM traces via ddtrace
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

from ddtrace import tracer

from dag import PIPELINE_DAG, get_execution_order, get_job
from openlineage_emitter import emit_start, emit_complete, emit_fail
from controlm_metrics import (
    emit_job_started,
    emit_job_ended_ok,
    emit_job_ended_not_ok,
    emit_job_wait_time,
    emit_pipeline_started,
    emit_pipeline_completed,
    emit_dq_check_result,
)
from etl_oracle_to_sqlserver import run_job as run_etl_job
from log_correlation import datadog_correlation_fields

try:
    from pipeline_finalizer import emit_pipeline_complete as _emit_pipeline_complete
except ImportError:
    _emit_pipeline_complete = None

logger = logging.getLogger("controlm-sim.executor")

MAX_RETRIES = 2


def _notify_progress(callback, event_type, **fields):
    if not callback:
        return
    try:
        callback({"event": event_type, **fields})
    except Exception as exc:
        logger.debug("progress callback failed: %s", exc)


def _append_jsonl(path, record):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=str) + "\n")
    except Exception as exc:
        logger.debug("failed to append structured log %s: %s", path, exc)


def _log_structured(**fields):
    """Emit a structured JSON log line."""
    fields.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    fields.setdefault("service", os.environ.get("DD_SERVICE", "controlm-sim"))
    fields.setdefault("env", os.environ.get("DD_ENV", "demo"))
    fields.setdefault("version", os.environ.get("DD_VERSION", "2.0.0"))
    fields.setdefault("pipeline", "b3_d1_pipeline")
    fields.setdefault("component", "controlm")
    fields.setdefault("log_type", "job_execution")
    fields.update(datadog_correlation_fields())
    fields.setdefault("trace_id", fields.get("dd.trace_id", "0"))
    fields.setdefault("span_id", fields.get("dd.span_id", "0"))
    job_name = fields.get("job_name")
    status = fields.get("status")
    if job_name:
        fields.setdefault("ctm_job", job_name)
        fields.setdefault("job", {})
        fields["job"].setdefault("name", job_name)
        if status:
            fields["job"].setdefault("status", status)
    if "business_date" in fields:
        fields.setdefault("ctm_order_date", fields["business_date"])
    if "run_id" in fields:
        fields.setdefault("ctm_run_id", fields["run_id"])
    if "sla_miss" in fields:
        fields.setdefault("sla", {})
        fields["sla"].setdefault("miss", fields["sla_miss"])
    print(json.dumps(fields, default=str), flush=True)
    _append_jsonl("/data/logs/jobs.jsonl", fields)


def execute_job(job_name, business_date, run_id=None, parent_run_id=None):
    """Execute a single ETL job with retries, SLA tracking, metrics, and OL events.

    Returns a dict with status, duration, retries, sla_miss, result.
    """
    job_def = get_job(job_name)
    sla_seconds = job_def["sla_seconds"]
    run_id = run_id or str(uuid.uuid4())
    job_started_at = time.time()
    run_span = tracer.trace(
        "controlm.job.run",
        service="controlm-sim",
        resource=job_name,
        span_type="worker",
    )
    run_span.set_tags(
        {
            "env": "demo",
            "team": "data-platform",
            "domain": "capital-markets",
            "pipeline": "b3_d1_pipeline",
            "orchestrator": "controlm-sim",
            "business_date": business_date,
            "job.name": job_name,
            "job.status": "running",
            "ol.namespace": os.environ.get("OL_NAMESPACE", "b3-poc-demo-v3"),
            "ol.run_id": run_id,
            "ol.pipeline_run_id": parent_run_id or "",
        }
    )

    # Emit OpenLineage START
    emit_start(
        job_name=job_name,
        run_id=run_id,
        parent_run_id=parent_run_id,
        business_date=business_date,
    )

    # Emit Control-M job started metric + event
    emit_job_started(job_name, business_date, run_id)

    # Simulate queue wait time (realistic for Control-M)
    wait_time = round(0.5 + (hash(job_name) % 3) * 0.5, 1)
    emit_job_wait_time(job_name, business_date, wait_time, run_id)

    retries = 0
    last_error = None
    result = None

    while retries <= MAX_RETRIES:
        start_time = time.time()
        try:
            with tracer.trace(
                f"controlm.job.{job_name}",
                service="controlm-sim",
                resource=job_name,
                span_type="worker",
            ) as span:
                span.set_tags(
                    {
                        "env": "demo",
                        "team": "data-platform",
                        "domain": "capital-markets",
                        "pipeline": "b3_d1_pipeline",
                        "orchestrator": "controlm-sim",
                        "business_date": business_date,
                        "stage": job_name,
                        "run_id": run_id,
                        "retry": retries,
                    }
                )

                # Execute the ETL job (Oracle → SQL Server)
                result = run_etl_job(job_name, business_date)

                # Production-realistic padding: stream periodic progress updates
                # so Datadog sees the job as long-running (matters for DJM
                # aggregation + lineage UI Duration / job_runs widgets).
                padding = job_def.get("padding_seconds", 0)
                if padding > 0:
                    logger.info("Padding %s with %ds of realistic duration", job_name, padding)
                    # Emit one heartbeat span tag per 10s to look "live" in APM
                    elapsed_padding = 0
                    step = 10
                    while elapsed_padding < padding:
                        time.sleep(min(step, padding - elapsed_padding))
                        elapsed_padding += step
                        span.set_tag(f"progress_pct", min(100, int(elapsed_padding / padding * 100)))

                duration = time.time() - start_time
                sla_miss = duration > sla_seconds
                output_rows = result.get("rows_loaded", 0) if result else 0

                span.set_tag("duration_seconds", round(duration, 2))
                span.set_tag("sla_miss", sla_miss)
                span.set_tag("status", "success")
                span.set_tag("output_rows", output_rows)

                _log_structured(
                    job_name=job_name,
                    run_id=run_id,
                    business_date=business_date,
                    status="success",
                    duration_seconds=round(duration, 2),
                    retries=retries,
                    sla_miss=sla_miss,
                    output_rows=output_rows,
                )

                # Emit OpenLineage COMPLETE
                emit_complete(
                    job_name=job_name,
                    run_id=run_id,
                    parent_run_id=parent_run_id,
                    business_date=business_date,
                )

                # Emit Control-M job ended OK metrics
                emit_job_ended_ok(
                    job_name, business_date, run_id,
                    duration_seconds=round(duration, 2),
                    retries=retries,
                    sla_miss=sla_miss,
                    output_rows=output_rows,
                )

                # If this was the quality gate, emit per-check DQ metrics
                if job_name == "quality_gate_d1" and result and "results" in result:
                    for check in result["results"]:
                        emit_dq_check_result(
                            check_name=check["check_name"],
                            check_type=check["check_type"],
                            target_table=check["target_table"],
                            passed=check["passed"],
                            severity=check["severity"],
                            business_date=business_date,
                            actual_value=check.get("actual_value"),
                            expected_value=check.get("expected_value"),
                        )

                run_span.set_tags(
                    {
                        "job.status": "success",
                        "duration_seconds": round(time.time() - job_started_at, 2),
                        "retries": retries,
                        "sla_miss": sla_miss,
                        "output_rows": output_rows,
                    }
                )
                run_span.finish()
                return {
                    "status": "success",
                    "duration_seconds": round(duration, 2),
                    "retries": retries,
                    "sla_miss": sla_miss,
                    "result": result,
                }

        except Exception as exc:
            duration = time.time() - start_time
            sla_miss = duration > sla_seconds
            last_error = exc

            _log_structured(
                job_name=job_name,
                run_id=run_id,
                business_date=business_date,
                status="retry" if retries < MAX_RETRIES else "failed",
                duration_seconds=round(duration, 2),
                retries=retries,
                sla_miss=sla_miss,
                error=str(exc),
            )

            retries += 1
            if retries <= MAX_RETRIES:
                logger.warning(
                    "Job %s failed (attempt %d/%d): %s — retrying...",
                    job_name,
                    retries,
                    MAX_RETRIES + 1,
                    exc,
                )
                time.sleep(2)

    # All retries exhausted
    duration = time.time() - start_time
    sla_miss = duration > sla_seconds

    _log_structured(
        job_name=job_name,
        run_id=run_id,
        business_date=business_date,
        status="failed",
        duration_seconds=round(duration, 2),
        retries=retries - 1,
        sla_miss=sla_miss,
        error=str(last_error),
    )

    emit_fail(
        job_name=job_name,
        run_id=run_id,
        parent_run_id=parent_run_id,
        business_date=business_date,
        error=str(last_error),
    )

    # Emit Control-M job ended NOT OK metrics
    emit_job_ended_not_ok(
        job_name, business_date, run_id,
        duration_seconds=round(duration, 2),
        retries=retries - 1,
        sla_miss=sla_miss,
        error=str(last_error),
    )

    run_span.error = 1
    run_span.set_tags(
        {
            "job.status": "failed",
            "duration_seconds": round(time.time() - job_started_at, 2),
            "retries": retries - 1,
            "sla_miss": sla_miss,
            "error.msg": str(last_error),
            "error.type": type(last_error).__name__ if last_error else "Exception",
        }
    )
    run_span.finish()
    return {
        "status": "failed",
        "duration_seconds": round(duration, 2),
        "retries": retries - 1,
        "sla_miss": sla_miss,
        "error": str(last_error),
    }


def execute_pipeline(business_date, inject_fault=None, progress_callback=None):
    """Execute the full DAG in topological order.

    Optionally seeds Oracle with fault injection before running.
    Stops on first job failure. Returns a summary dict.
    """
    pipeline_run_id = str(uuid.uuid4())
    pipeline_name = PIPELINE_DAG["name"]
    order = get_execution_order()

    logger.info(
        "Starting pipeline %s (run_id=%s, business_date=%s, jobs=%d)",
        pipeline_name,
        pipeline_run_id,
        business_date,
        len(order),
    )

    # Emit Control-M pipeline metrics, but do not emit OpenLineage datasets for
    # the pipeline itself. Lineage should show the real Python jobs in the middle
    # of the graph, not a collapsed Oracle -> pipeline -> S3 shortcut.
    emit_pipeline_started(pipeline_name, business_date, pipeline_run_id, len(order))

    # Seed Oracle data with optional fault injection. This is represented as a
    # lineage-only Python job so Oracle source datasets get freshness/row-count
    # and the graph starts from an executable job instead of a floating dataset.
    seed_run_id = str(uuid.uuid4())
    seed_started_at = time.time()
    seed_span = tracer.trace(
        "controlm.job.run",
        service="controlm-sim",
        resource="market_data_ingest",
        span_type="worker",
    )
    seed_span.set_tags(
        {
            "env": "demo",
            "team": "data-platform",
            "domain": "capital-markets",
            "pipeline": pipeline_name,
            "orchestrator": "controlm-sim",
            "business_date": business_date,
            "job.name": "market_data_ingest",
            "job.status": "running",
            "ol.namespace": os.environ.get("OL_NAMESPACE", "b3-poc-demo-v3"),
            "ol.run_id": seed_run_id,
            "ol.pipeline_run_id": pipeline_run_id,
        }
    )
    emit_start(
        job_name="market_data_ingest",
        run_id=seed_run_id,
        parent_run_id=pipeline_run_id,
        business_date=business_date,
    )
    try:
        from etl_oracle_to_sqlserver import seed_oracle_data
        seed_result = seed_oracle_data(business_date, inject_fault=inject_fault)
        logger.info("Oracle data seeded: %s", seed_result)
        emit_complete(
            job_name="market_data_ingest",
            run_id=seed_run_id,
            parent_run_id=pipeline_run_id,
            business_date=business_date,
        )
        seed_output_rows = sum(
            seed_result.get(k, 0)
            for k in ("trades", "derivative_positions", "cash_positions")
        )
        _log_structured(
            job_name="market_data_ingest",
            run_id=seed_run_id,
            business_date=business_date,
            status="success",
            duration_seconds=round(time.time() - seed_started_at, 2),
            retries=0,
            sla_miss=False,
            output_rows=seed_output_rows,
            fault_injected=seed_result.get("fault_injected"),
        )
        seed_span.set_tags(
            {
                "job.status": "success",
                "duration_seconds": round(time.time() - seed_started_at, 2),
                "output_rows": seed_output_rows,
            }
        )
        seed_span.finish()
    except Exception as exc:
        logger.warning("Oracle seeding failed (non-blocking): %s", exc)
        emit_fail(
            job_name="market_data_ingest",
            run_id=seed_run_id,
            parent_run_id=pipeline_run_id,
            business_date=business_date,
            error=str(exc),
        )
        _log_structured(
            job_name="market_data_ingest",
            run_id=seed_run_id,
            business_date=business_date,
            status="failed",
            duration_seconds=round(time.time() - seed_started_at, 2),
            retries=0,
            sla_miss=False,
            error=str(exc),
        )
        seed_span.error = 1
        seed_span.set_tags(
            {
                "job.status": "failed",
                "duration_seconds": round(time.time() - seed_started_at, 2),
                "error.msg": str(exc),
                "error.type": type(exc).__name__,
            }
        )
        seed_span.finish()

    results = {}
    pipeline_start = time.time()
    jobs_ok = 0
    jobs_notok = 0

    for job_def in order:
        job_name = job_def["name"]
        job_run_id = str(uuid.uuid4())

        logger.info("Running job: %s", job_name)
        _notify_progress(
            progress_callback,
            "job-start",
            job_name=job_name,
            run_id=job_run_id,
            pipeline_run_id=pipeline_run_id,
            business_date=business_date,
        )
        outcome = execute_job(
            job_name=job_name,
            business_date=business_date,
            run_id=job_run_id,
            parent_run_id=pipeline_run_id,
        )
        results[job_name] = outcome

        if outcome["status"] == "failed":
            jobs_notok += 1
            logger.error("Job %s failed — aborting pipeline.", job_name)
            # Emit Control-M pipeline failed
            emit_pipeline_completed(
                pipeline_name, business_date, pipeline_run_id,
                duration_seconds=round(time.time() - pipeline_start, 2),
                jobs_ok=jobs_ok, jobs_notok=jobs_notok, total_jobs=len(order),
            )
            final = {
                "pipeline": pipeline_name,
                "run_id": pipeline_run_id,
                "business_date": business_date,
                "status": "failed",
                "duration_seconds": round(time.time() - pipeline_start, 2),
                "jobs": results,
            }
            _persist_run(final)
            return final
        else:
            jobs_ok += 1

    pipeline_duration = time.time() - pipeline_start

    # Emit Control-M pipeline completed
    emit_pipeline_completed(
        pipeline_name, business_date, pipeline_run_id,
        duration_seconds=round(pipeline_duration, 2),
        jobs_ok=jobs_ok, jobs_notok=jobs_notok, total_jobs=len(order),
    )

    logger.info(
        "Pipeline %s completed in %.2fs (run_id=%s)",
        pipeline_name,
        pipeline_duration,
        pipeline_run_id,
    )

    final = {
        "pipeline": pipeline_name,
        "run_id": pipeline_run_id,
        "business_date": business_date,
        "status": "success",
        "duration_seconds": round(pipeline_duration, 2),
        "jobs": results,
    }

    # Emit consolidated pipeline-level OL event (includes DQ summary)
    if _emit_pipeline_complete:
        try:
            _emit_pipeline_complete(final)
        except Exception as exc:
            logger.warning("pipeline_finalizer failed (non-blocking): %s", exc)

    # Persist run summary to /data/runs.jsonl for the History tab in the UI.
    _persist_run(final)

    return final


def _persist_run(summary):
    """Append a one-line JSON record to /data/runs.jsonl for the History tab."""
    import datetime as _dt
    runs_file = "/data/runs.jsonl"
    record = {
        "timestamp":        _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "service":          os.environ.get("DD_SERVICE", "controlm-sim"),
        "env":              os.environ.get("DD_ENV", "demo"),
        "version":          os.environ.get("DD_VERSION", "2.0.0"),
        "component":        "controlm",
        "log_type":         "pipeline_history",
        "pipeline":         summary.get("pipeline"),
        "run_id":           summary.get("run_id"),
        "ctm_run_id":       summary.get("run_id"),
        "business_date":    summary.get("business_date"),
        "ctm_order_date":   summary.get("business_date"),
        "ctm_job":          summary.get("pipeline"),
        "status":           summary.get("status"),
        "duration_seconds": summary.get("duration_seconds"),
        "finished_at":      _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "job_count":        len(summary.get("jobs", {})),
        "job_summary": {
            jn: {"status": jr.get("status"), "duration_seconds": jr.get("duration_seconds")}
            for jn, jr in summary.get("jobs", {}).items()
        },
    }
    try:
        os.makedirs(os.path.dirname(runs_file), exist_ok=True)
        with open(runs_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=str) + "\n")
    except Exception as exc:
        logger.warning("persist_run failed (non-blocking): %s", exc)
