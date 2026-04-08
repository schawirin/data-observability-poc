"""DAG definition for the market-d1 pipeline."""

PIPELINE_DAG = {
    "name": "market-d1",
    "jobs": [
        {"name": "close_market_eod", "depends_on": [], "sla_seconds": 600},
        {"name": "reconcile_d1_positions", "depends_on": ["close_market_eod"], "sla_seconds": 900},
        {"name": "quality_gate_d1", "depends_on": ["reconcile_d1_positions"], "sla_seconds": 300},
        {"name": "publish_d1_reports", "depends_on": ["quality_gate_d1"], "sla_seconds": 600},
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
