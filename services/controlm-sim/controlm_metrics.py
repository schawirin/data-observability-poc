"""
controlm_metrics.py — Emits Control-M execution metrics to Datadog via DogStatsD.

All count-like metrics use statsd.gauge() with Python-side counters to ensure
integer values in Datadog widgets (statsd.increment creates rate metrics that
show fractional values in query_value widgets).
"""

import logging
import os
import threading

from datadog import initialize, statsd

logger = logging.getLogger("controlm-sim.metrics")

_dd_options = {
    "statsd_host": os.environ.get("DD_AGENT_HOST", "datadog-agent"),
    "statsd_port": int(os.environ.get("DD_DOGSTATSD_PORT", "8125")),
    "api_key": os.environ.get("DD_API_KEY", ""),
    "app_key": os.environ.get("DD_APP_KEY", ""),
}
initialize(**_dd_options)

CTM_SERVER = os.environ.get("CTM_SERVER", "demo-poc-ctm")
CTM_APPLICATION = os.environ.get("CTM_APPLICATION", "exchange-data-platform")
CTM_SUB_APPLICATION = os.environ.get("CTM_SUB_APPLICATION", "derivatives")
CTM_HOST = os.environ.get("CTM_HOST", "workbench")
CTM_FOLDER = "Market_D1_Pipeline"

_BASE_TAGS = [
    f"ctm_server:{CTM_SERVER}",
    f"ctm_application:{CTM_APPLICATION}",
    f"ctm_sub_application:{CTM_SUB_APPLICATION}",
    f"ctm_host:{CTM_HOST}",
    f"ctm_folder:{CTM_FOLDER}",
    "env:demo",
    "team:data-platform",
    "orchestrator:controlm",
]

# ── Thread-safe counters for gauge-based counting ─────────────────────────────
_counters_lock = threading.Lock()
_counters = {}


def _count(metric, tags_key, increment=1):
    """Increment an in-memory counter and emit as gauge (always integer)."""
    key = f"{metric}|{tags_key}"
    with _counters_lock:
        _counters[key] = _counters.get(key, 0) + increment
        return _counters[key]


def _job_tags(job_name, business_date, run_id=None, status=None, extra_tags=None):
    tags = list(_BASE_TAGS)
    tags.append(f"ctm_job:{job_name}")
    tags.append(f"ctm_order_date:{business_date}")
    if run_id:
        tags.append(f"ctm_run_id:{run_id}")
    if status:
        tags.append(f"ctm_status:{status}")
    if extra_tags:
        tags.extend(extra_tags)
    return tags


# ── Job-level metrics ─────────────────────────────────────────────────────────


def emit_job_started(job_name, business_date, run_id=None):
    tags = _job_tags(job_name, business_date, run_id, status="executing")
    statsd.event(
        title=f"[Control-M] Job Started: {job_name}",
        message=f"Job `{job_name}` started.\n- **Folder:** {CTM_FOLDER}\n- **Order Date:** {business_date}\n- **Run ID:** {run_id or 'N/A'}",
        tags=tags, alert_type="info", source_type_name="controlm",
        aggregation_key=f"ctm-{job_name}-{business_date}",
    )
    logger.info("CTM metric: job_started %s (date=%s)", job_name, business_date)


def emit_job_ended_ok(job_name, business_date, run_id=None, duration_seconds=0,
                      retries=0, sla_miss=False, output_rows=0):
    tags = _job_tags(job_name, business_date, run_id, status="ended_ok")
    job_key = job_name

    statsd.gauge("controlm.job.duration", duration_seconds, tags=tags)
    statsd.gauge("controlm.job.retries", retries, tags=tags)
    statsd.gauge("controlm.job.output_rows", output_rows, tags=tags)

    # Count metrics as gauges (always integer)
    val = _count("controlm.job.ended_ok", job_key)
    statsd.gauge("controlm.job.ended_ok", val, tags=tags)

    if sla_miss:
        val = _count("controlm.job.sla.violation", job_key)
        statsd.gauge("controlm.job.sla.violation", val, tags=tags)
        statsd.event(
            title=f"[Control-M] SLA Violation: {job_name}",
            message=f"Job `{job_name}` exceeded SLA.\n- **Duration:** {duration_seconds:.1f}s\n- **Order Date:** {business_date}",
            tags=tags, alert_type="warning", source_type_name="controlm",
            aggregation_key=f"ctm-sla-{job_name}-{business_date}",
        )

    statsd.event(
        title=f"[Control-M] Job Ended OK: {job_name}",
        message=f"Job `{job_name}` completed.\n- **Duration:** {duration_seconds:.1f}s\n- **Retries:** {retries}\n- **Rows:** {output_rows}\n- **SLA Miss:** {sla_miss}",
        tags=tags, alert_type="success", source_type_name="controlm",
        aggregation_key=f"ctm-{job_name}-{business_date}",
    )
    logger.info("CTM metric: job_ended_ok %s (%.1fs, retries=%d, rows=%d)", job_name, duration_seconds, retries, output_rows)


def emit_job_ended_not_ok(job_name, business_date, run_id=None, duration_seconds=0,
                          retries=0, sla_miss=False, error=""):
    tags = _job_tags(job_name, business_date, run_id, status="ended_not_ok")
    job_key = job_name

    statsd.gauge("controlm.job.duration", duration_seconds, tags=tags)
    statsd.gauge("controlm.job.retries", retries, tags=tags)

    val = _count("controlm.job.ended_not_ok", job_key)
    statsd.gauge("controlm.job.ended_not_ok", val, tags=tags)

    if sla_miss:
        val = _count("controlm.job.sla.violation", job_key)
        statsd.gauge("controlm.job.sla.violation", val, tags=tags)

    statsd.event(
        title=f"[Control-M] Job Ended NOT OK: {job_name}",
        message=f"Job `{job_name}` **FAILED**.\n- **Duration:** {duration_seconds:.1f}s\n- **Retries:** {retries}\n- **Error:** {error[:500]}",
        tags=tags, alert_type="error", source_type_name="controlm",
        aggregation_key=f"ctm-{job_name}-{business_date}",
    )
    logger.info("CTM metric: job_ended_not_ok %s (%.1fs, error=%s)", job_name, duration_seconds, error[:100])


def emit_job_wait_time(job_name, business_date, wait_seconds, run_id=None):
    tags = _job_tags(job_name, business_date, run_id)
    statsd.gauge("controlm.job.wait_time", wait_seconds, tags=tags)


# ── Pipeline/Folder-level metrics ────────────────────────────────────────────


def emit_pipeline_started(pipeline_name, business_date, run_id, total_jobs):
    tags = _job_tags(pipeline_name, business_date, run_id, status="executing")
    tags.append(f"ctm_folder:{pipeline_name}")
    statsd.gauge("controlm.folder.jobs.total", total_jobs, tags=tags)
    statsd.event(
        title=f"[Control-M] Pipeline Started: {pipeline_name}",
        message=f"Pipeline `{pipeline_name}` started with **{total_jobs} jobs**.\n- **Order Date:** {business_date}\n- **Run ID:** {run_id}",
        tags=tags, alert_type="info", source_type_name="controlm",
        aggregation_key=f"ctm-pipeline-{pipeline_name}-{business_date}",
    )


def emit_pipeline_completed(pipeline_name, business_date, run_id,
                            duration_seconds, jobs_ok, jobs_notok, total_jobs):
    status = "ended_ok" if jobs_notok == 0 else "ended_not_ok"
    tags = _job_tags(pipeline_name, business_date, run_id, status=status)
    tags.append(f"ctm_folder:{pipeline_name}")

    statsd.gauge("controlm.folder.duration", duration_seconds, tags=tags)
    statsd.gauge("controlm.folder.jobs.total", total_jobs, tags=tags)
    statsd.gauge("controlm.folder.jobs.ok", jobs_ok, tags=tags)
    statsd.gauge("controlm.folder.jobs.notok", jobs_notok, tags=tags)

    if jobs_notok == 0:
        val = _count("controlm.folder.ended_ok", "pipeline")
        statsd.gauge("controlm.folder.ended_ok", val, tags=tags)
        alert, title = "success", f"[Control-M] Pipeline Completed: {pipeline_name}"
    else:
        val = _count("controlm.folder.ended_not_ok", "pipeline")
        statsd.gauge("controlm.folder.ended_not_ok", val, tags=tags)
        alert, title = "error", f"[Control-M] Pipeline Failed: {pipeline_name}"

    statsd.event(
        title=title,
        message=f"Pipeline `{pipeline_name}` finished.\n- **Status:** {status.upper()}\n- **Duration:** {duration_seconds:.1f}s\n- **Jobs OK:** {jobs_ok}/{total_jobs}\n- **Jobs NOT OK:** {jobs_notok}/{total_jobs}",
        tags=tags, alert_type=alert, source_type_name="controlm",
        aggregation_key=f"ctm-pipeline-{pipeline_name}-{business_date}",
    )
    logger.info("CTM metric: pipeline_%s %s (%.1fs, ok=%d, notok=%d)", status, pipeline_name, duration_seconds, jobs_ok, jobs_notok)


# ── DQ-specific metrics ──────────────────────────────────────────────────────


def emit_dq_check_result(check_name, check_type, target_table, passed,
                         severity, business_date, actual_value=None, expected_value=None):
    tags = list(_BASE_TAGS) + [
        f"check_name:{check_name}",
        f"check_type:{check_type}",
        f"target_table:{target_table}",
        f"severity:{severity}",
        f"passed:{'true' if passed else 'false'}",
        f"ctm_order_date:{business_date}",
    ]

    check_key = check_name
    val = _count("controlm.dq.checks_run", check_key)
    statsd.gauge("controlm.dq.checks_run", val, tags=tags)

    if not passed:
        val = _count("controlm.dq.checks_failed", check_key)
        statsd.gauge("controlm.dq.checks_failed", val, tags=tags)

    if actual_value is not None:
        statsd.gauge("controlm.dq.actual_value", float(actual_value), tags=tags)
