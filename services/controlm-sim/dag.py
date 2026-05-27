"""DAG definition for the b3_d1_pipeline."""

import os as _os

# Production-realistic durations. The actual ETL is fast (~2s per job in our
# mock), but Datadog DJM expects job runs of dozens of seconds to minutes to
# properly aggregate Duration histograms and pinpoint failures within a run.
# So we pad each job with a sleep matching a realistic Control-M B3 pipeline.
# Override globally via env JOB_PADDING_FACTOR (default 1.0 = full duration).
PADDING_FACTOR = float(_os.environ.get("JOB_PADDING_FACTOR", "1.0"))

PIPELINE_DAG = {
    "name": "b3_d1_pipeline",
    "jobs": [
        # EOD consolidation (book close + VWAP) — typically 1 min in prod
        {"name": "close_market_eod",       "depends_on": [],                          "sla_seconds": 600, "padding_seconds": int(60 * PADDING_FACTOR)},
        # D+1 reconciliation — heavy SQL join, typically 2 min in prod
        {"name": "reconcile_d1_positions", "depends_on": ["close_market_eod"],        "sla_seconds": 900, "padding_seconds": int(120 * PADDING_FACTOR)},
        # Quality gate — fast checks, ~30s
        {"name": "quality_gate_d1",        "depends_on": ["reconcile_d1_positions"],  "sla_seconds": 300, "padding_seconds": int(30 * PADDING_FACTOR)},
        # Publish to S3 — 1 min for parquet + CSV exports
        {"name": "publish_d1_reports",     "depends_on": ["quality_gate_d1"],         "sla_seconds": 600, "padding_seconds": int(60 * PADDING_FACTOR)},
    ],
}

# Pre-indexed lookup
_JOBS_BY_NAME = {job["name"]: job for job in PIPELINE_DAG["jobs"]}


def get_execution_order():
    """Return jobs in topological order respecting dependencies.

    Uses Kahn's algorithm so the ordering stays correct even if
    the DAG is later extended with parallel branches.
    """
    jobs = PIPELINE_DAG["jobs"]
    in_degree = {j["name"]: len(j["depends_on"]) for j in jobs}
    dependents = {j["name"]: [] for j in jobs}
    for j in jobs:
        for dep in j["depends_on"]:
            dependents[dep].append(j["name"])

    queue = [name for name, deg in in_degree.items() if deg == 0]
    order = []

    while queue:
        name = queue.pop(0)
        order.append(_JOBS_BY_NAME[name])
        for child in dependents[name]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(order) != len(jobs):
        raise RuntimeError("Cycle detected in DAG")

    return order


def get_job(name):
    """Return a single job definition by name, or raise KeyError."""
    if name not in _JOBS_BY_NAME:
        raise KeyError(f"Job '{name}' not found in DAG. Available: {list(_JOBS_BY_NAME.keys())}")
    return _JOBS_BY_NAME[name]
