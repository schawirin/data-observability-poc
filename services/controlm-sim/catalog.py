"""Persistence layer for custom job definitions.

Loads/saves jobs to a JSON file mounted under /data so they survive container
restarts. Built-in pipeline jobs (close_market_eod, reconcile_d1_positions,
quality_gate_d1, publish_d1_reports) are protected and live in code.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from pathlib import Path

# Built-in jobs (cannot be deleted, only overridden at runtime)
BUILTIN_JOBS = {
    "close_market_eod": {
        "id": "close_market_eod",
        "label": "close_market_eod",
        "folder": "PRD-MKT-D1",
        "application": "Capital Markets",
        "description": "ETL: Oracle ASTADRVT_TRADE_MVMT → SQL Server ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
        "type": "builtin",
        "command": None,
        "depends_on": [],
        "sla_seconds": 600,
        "retries": 3,
        "builtin": True,
    },
    "reconcile_d1_positions": {
        "id": "reconcile_d1_positions",
        "label": "reconcile_d1_positions",
        "folder": "PRD-MKT-D1",
        "application": "Capital Markets",
        "description": "ETL: Oracle ASTANO/ASTACASH → SQL Server ADWPM_POSICAO_*",
        "type": "builtin",
        "command": None,
        "depends_on": ["close_market_eod"],
        "sla_seconds": 900,
        "retries": 3,
        "builtin": True,
    },
    "quality_gate_d1": {
        "id": "quality_gate_d1",
        "label": "quality_gate_d1",
        "folder": "PRD-MKT-D1",
        "application": "Capital Markets",
        "description": "DQ: 5 checks (duplicatas, nulos, soma-zero, row-count, overflow) → ADWPM_DQ_RESULTS",
        "type": "builtin",
        "command": None,
        "depends_on": ["reconcile_d1_positions"],
        "sla_seconds": 300,
        "retries": 1,
        "builtin": True,
    },
    "publish_d1_reports": {
        "id": "publish_d1_reports",
        "label": "publish_d1_reports",
        "folder": "PRD-MKT-D1",
        "application": "Capital Markets",
        "description": "Export: SQL Server DW → MinIO Parquet/CSV",
        "type": "builtin",
        "command": None,
        "depends_on": ["quality_gate_d1"],
        "sla_seconds": 600,
        "retries": 3,
        "builtin": True,
    },
}

ALLOWED_TYPES = {"shell", "python", "sql", "etl_template", "builtin"}
ALLOWED_DBS = {"oracle", "sqlserver", "mysql", "postgres"}

_CATALOG_PATH = Path(os.environ.get("CTM_CATALOG_PATH", "/data/jobs_catalog.json"))
_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_dir():
    _CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def _load_custom() -> dict:
    if not _CATALOG_PATH.exists():
        return {}
    try:
        with _CATALOG_PATH.open() as f:
            data = json.load(f)
        return data.get("jobs", {})
    except (json.JSONDecodeError, OSError):
        return {}


def _save_custom(jobs: dict) -> None:
    _ensure_dir()
    payload = {"version": 1, "jobs": jobs, "saved_at": _now_iso()}
    tmp = _CATALOG_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(payload, f, indent=2)
    tmp.replace(_CATALOG_PATH)


def list_jobs() -> dict:
    """Return all jobs (built-in + custom) merged."""
    with _lock:
        return {**BUILTIN_JOBS, **_load_custom()}


def get_job(job_id: str) -> dict | None:
    return list_jobs().get(job_id)


def validate_job(payload: dict) -> tuple[bool, str | None]:
    name = (payload.get("id") or payload.get("label") or "").strip()
    if not name:
        return False, "id/label is required"
    if not name.replace("_", "").replace("-", "").isalnum():
        return False, "id can only contain alphanumeric, underscore and dash"
    if len(name) > 64:
        return False, "id too long (>64)"

    jtype = payload.get("type", "shell")
    if jtype not in ALLOWED_TYPES - {"builtin"}:
        return False, f"type must be one of {sorted(ALLOWED_TYPES - {'builtin'})}"

    cmd = (payload.get("command") or "").strip()
    if jtype in ("shell", "python") and not cmd:
        return False, "command is required for shell/python jobs"

    if jtype == "sql":
        if not cmd:
            return False, "query is required for sql jobs"
        db = payload.get("target_db", "").lower()
        if db not in ALLOWED_DBS:
            return False, f"target_db must be one of {sorted(ALLOWED_DBS)}"

    if jtype == "etl_template":
        if not payload.get("src_table"):
            return False, "src_table is required for etl_template"
        if not payload.get("tgt_table"):
            return False, "tgt_table is required for etl_template"

    deps = payload.get("depends_on") or []
    if not isinstance(deps, list):
        return False, "depends_on must be a list"
    catalog = list_jobs()
    for d in deps:
        if d not in catalog:
            return False, f"unknown dependency: {d}"
        if d == name:
            return False, "job cannot depend on itself"

    try:
        sla = int(payload.get("sla_seconds", 300))
        retries = int(payload.get("retries", 0))
    except (TypeError, ValueError):
        return False, "sla_seconds and retries must be integers"
    if sla < 1 or sla > 86400:
        return False, "sla_seconds must be in [1, 86400]"
    if retries < 0 or retries > 10:
        return False, "retries must be in [0, 10]"

    return True, None


def upsert_job(payload: dict) -> dict:
    """Create or update a custom job. Built-in jobs cannot be modified here."""
    ok, err = validate_job(payload)
    if not ok:
        raise ValueError(err)

    job_id = (payload.get("id") or payload.get("label")).strip()
    if job_id in BUILTIN_JOBS:
        raise PermissionError(f"cannot modify built-in job: {job_id}")

    with _lock:
        custom = _load_custom()
        now = _now_iso()
        existing = custom.get(job_id, {})

        record = {
            "id": job_id,
            "label": payload.get("label", job_id),
            "folder": payload.get("folder", "CUSTOM"),
            "application": payload.get("application", "Custom"),
            "description": payload.get("description", ""),
            "type": payload.get("type", "shell"),
            "command": (payload.get("command") or "").strip(),
            "working_dir": payload.get("working_dir") or "/app",
            "depends_on": list(payload.get("depends_on") or []),
            "sla_seconds": int(payload.get("sla_seconds", 300)),
            "retries": int(payload.get("retries", 0)),
            "target_db": (payload.get("target_db") or "").lower() or None,
            "src_table": payload.get("src_table") or None,
            "tgt_table": payload.get("tgt_table") or None,
            "expected": payload.get("expected") or None,  # for sql jobs
            "created_at": existing.get("created_at", now),
            "updated_at": now,
            "builtin": False,
        }
        custom[job_id] = record
        _save_custom(custom)
        return record


def delete_job(job_id: str) -> bool:
    if job_id in BUILTIN_JOBS:
        raise PermissionError(f"cannot delete built-in job: {job_id}")
    with _lock:
        custom = _load_custom()
        if job_id not in custom:
            return False
        # Reject if other jobs depend on it
        for other in custom.values():
            if job_id in (other.get("depends_on") or []):
                raise ValueError(f"job '{other['id']}' depends on '{job_id}'")
        del custom[job_id]
        _save_custom(custom)
        return True


def is_builtin(job_id: str) -> bool:
    return job_id in BUILTIN_JOBS
