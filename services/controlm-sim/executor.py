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

try:
    from pipeline_finalizer import emit_pipeline_complete as _emit_pipeline_complete
except ImportError:
    _emit_pipeline_complete = None

logger = logging.getLogger("controlm-sim.executor")

MAX_RETRIES = 2


def _log_structured(**fields):
    """Emit a structured JSON log line."""
    print(json.dumps(fields, default=str), flush=True)


def execute_job(job_name, business_date, run_id=None, parent_run_id=None):
    """Execute a single ETL job with retries, SLA tracking, metrics, and OL events.

    Returns a dict with status, duration, retries, sla_miss, result.
    """
    job_def = get_job(job_name)
    sla_seconds = job_def["sla_seconds"]
    run_id = run_id or str(uuid.uuid4())

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
                        "pipeline": "market-d1",
                        "orchestrator": "controlm-sim",
                        "business_date": business_date,
                        "stage": job_name,
                        "run_id": run_id,
                        "retry": retries,
                    }
                )

                # Execute the ETL job (Oracle → SQL Server)
                result = run_etl_job(job_name, business_date)

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

    return {
        "status": "failed",
        "duration_seconds": round(duration, 2),
        "retries": retries - 1,
        "sla_miss": sla_miss,
        "error": str(last_error),
    }


def execute_pipeline(business_date, inject_fault=None):
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

    # Emit pipeline-level START (OpenLineage + Control-M)
    emit_start(
        job_name=pipeline_name,
        run_id=pipeline_run_id,
        business_date=business_date,
    )
    emit_pipeline_started(pipeline_name, business_date, pipeline_run_id, len(order))

    # Seed Oracle data with optional fault injection
    try:
        from etl_oracle_to_sqlserver import seed_oracle_data
        seed_result = seed_oracle_data(business_date, inject_fault=inject_fault)
        logger.info("Oracle data seeded: %s", seed_result)
    except Exception as exc:
        logger.warning("Oracle seeding failed (non-blocking): %s", exc)

    results = {}
    pipeline_start = time.time()
    jobs_ok = 0
    jobs_notok = 0

    for job_def in order:
        job_name = job_def["name"]
        job_run_id = str(uuid.uuid4())

        logger.info("Running job: %s", job_name)
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
            emit_fail(
                job_name=pipeline_name,
                run_id=pipeline_run_id,
                business_date=business_date,
                error=f"Job {job_name} failed: {outcome.get('error', 'unknown')}",
            )
            # Emit Control-M pipeline failed
            emit_pipeline_completed(
                pipeline_name, business_date, pipeline_run_id,
                duration_seconds=round(time.time() - pipeline_start, 2),
                jobs_ok=jobs_ok, jobs_notok=jobs_notok, total_jobs=len(order),
            )
            return {
                "pipeline": pipeline_name,
                "run_id": pipeline_run_id,
                "business_date": business_date,
                "status": "failed",
                "duration_seconds": round(time.time() - pipeline_start, 2),
                "jobs": results,
            }
        else:
            jobs_ok += 1

    pipeline_duration = time.time() - pipeline_start

    # Emit pipeline-level COMPLETE (OpenLineage)
    emit_complete(
        job_name=pipeline_name,
        run_id=pipeline_run_id,
        business_date=business_date,
    )

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

    return final
