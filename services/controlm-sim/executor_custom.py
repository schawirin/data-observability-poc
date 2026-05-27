"""Generic executor for custom (non-builtin) jobs.

Job types:
- shell:         bash -c <command> with ODATE/BUSINESS_DATE env vars
- python:        python <command> or python -c (if inline:true)
- sql:           connect to target_db, run query, optionally validate count
- etl_template:  thin wrapper over etl_oracle_to_sqlserver pipeline params
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
import uuid
from datetime import datetime

try:
    from openlineage_emitter import (
        emit_start as _ol_emit_start,
        emit_complete as _ol_emit_complete,
        emit_fail as _ol_emit_fail,
        io_for_custom_job,
    )
    _OL_ENABLED = True
except Exception:  # pragma: no cover
    _OL_ENABLED = False

try:
    from controlm_metrics import (
        emit_job_started as _ctm_started,
        emit_job_ended_ok as _ctm_ok,
        emit_job_ended_not_ok as _ctm_not_ok,
    )
    _METRICS_ENABLED = True
except Exception:  # pragma: no cover
    _METRICS_ENABLED = False


def _log_json(level: str, **fields):
    """Structured JSON log line — picked up by the Datadog Agent's log parser
    so the ctm_job / business_date / status fields become Logs facets."""
    import json
    fields.setdefault("level", level)
    fields.setdefault("logger", "controlm-sim.custom")
    print(json.dumps(fields, default=str))

logger = logging.getLogger("controlm-sim.executor_custom")


def _now() -> float:
    return time.time()


def _base_env(business_date: str, extra: dict | None = None) -> dict:
    env = {
        **os.environ,
        "ODATE": business_date,
        "BUSINESS_DATE": business_date,
        "DD_ENV": os.environ.get("DD_ENV", "demo"),
    }
    if extra:
        env.update({k: str(v) for k, v in extra.items()})
    return env


def _run_shell(job: dict, business_date: str) -> dict:
    """bash -c <command>. Captures stdout/stderr, kills on SLA timeout."""
    cmd = job["command"]
    sla = int(job.get("sla_seconds", 300))
    workdir = job.get("working_dir") or "/app"
    if not os.path.isdir(workdir):
        workdir = "/app"

    start = _now()
    try:
        proc = subprocess.run(
            ["bash", "-c", cmd],
            cwd=workdir,
            env=_base_env(business_date),
            capture_output=True,
            text=True,
            timeout=sla,
        )
        duration = round(_now() - start, 2)
        if proc.returncode == 0:
            return {
                "status": "success",
                "duration_seconds": duration,
                "exit_code": 0,
                "stdout_tail": (proc.stdout or "")[-2000:],
                "sla_miss": duration > sla,
            }
        return {
            "status": "failed",
            "duration_seconds": duration,
            "exit_code": proc.returncode,
            "error": (proc.stderr or proc.stdout or "non-zero exit")[-1000:],
            "sla_miss": False,
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "failed",
            "duration_seconds": sla,
            "exit_code": -1,
            "error": f"SLA timeout exceeded ({sla}s)",
            "sla_miss": True,
        }
    except Exception as exc:
        return {
            "status": "failed",
            "duration_seconds": round(_now() - start, 2),
            "exit_code": -2,
            "error": f"{type(exc).__name__}: {exc}",
            "sla_miss": False,
        }


def _run_python(job: dict, business_date: str) -> dict:
    """python <command>. If command starts with 'inline:' run via -c."""
    cmd = job["command"].strip()
    if cmd.startswith("inline:"):
        args = ["python", "-c", cmd[len("inline:"):].lstrip()]
    else:
        # otherwise treat as 'path/to/script.py [args...]'
        args = ["python"] + cmd.split()

    sla = int(job.get("sla_seconds", 300))
    workdir = job.get("working_dir") or "/app"
    if not os.path.isdir(workdir):
        workdir = "/app"

    start = _now()
    try:
        proc = subprocess.run(
            args,
            cwd=workdir,
            env=_base_env(business_date),
            capture_output=True,
            text=True,
            timeout=sla,
        )
        duration = round(_now() - start, 2)
        if proc.returncode == 0:
            return {
                "status": "success",
                "duration_seconds": duration,
                "exit_code": 0,
                "stdout_tail": (proc.stdout or "")[-2000:],
                "sla_miss": duration > sla,
            }
        return {
            "status": "failed",
            "duration_seconds": duration,
            "exit_code": proc.returncode,
            "error": (proc.stderr or proc.stdout or "non-zero exit")[-1000:],
            "sla_miss": False,
        }
    except subprocess.TimeoutExpired:
        return {"status": "failed", "duration_seconds": sla, "exit_code": -1, "error": f"SLA timeout ({sla}s)", "sla_miss": True}
    except Exception as exc:
        return {"status": "failed", "duration_seconds": round(_now() - start, 2), "exit_code": -2, "error": f"{type(exc).__name__}: {exc}", "sla_miss": False}


def _connect(target_db: str):
    """Return (conn, cursor) for the requested DB. Caller closes."""
    if target_db == "oracle":
        import oracledb
        dsn = os.environ.get(
            "ORACLE_DSN",
            f"{os.environ.get('ORACLE_HOST', 'oracle')}:{os.environ.get('ORACLE_PORT', '1521')}/{os.environ.get('ORACLE_SERVICE', 'XEPDB1')}",
        )
        conn = oracledb.connect(
            user=os.environ.get("ORACLE_USER", "demopoc"),
            password=os.environ.get("ORACLE_PASSWORD", "OracleDemo123!"),
            dsn=dsn,
        )
        return conn, conn.cursor()
    if target_db == "sqlserver":
        import pymssql
        conn = pymssql.connect(
            server=os.environ.get("SQLSERVER_HOST", "sqlserver"),
            port=int(os.environ.get("SQLSERVER_PORT", "1433")),
            user=os.environ.get("SQLSERVER_USER", "sa"),
            password=os.environ.get("SQLSERVER_PASSWORD", "SqlDemo12345!"),
            database=os.environ.get("SQLSERVER_DATABASE", "demopoc"),
        )
        return conn, conn.cursor()
    if target_db == "mysql":
        import mysql.connector
        conn = mysql.connector.connect(
            host=os.environ.get("MYSQL_HOST", "mysql"),
            port=int(os.environ.get("MYSQL_PORT", "3306")),
            user=os.environ.get("MYSQL_USER", "root"),
            password=os.environ.get("MYSQL_PASSWORD", "demopoc2026"),
            database=os.environ.get("MYSQL_DATABASE", "exchange"),
        )
        return conn, conn.cursor()
    if target_db == "postgres":
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ.get("POSTGRES_USER", "demo"),
            password=os.environ.get("POSTGRES_PASSWORD", "demopoc2026"),
            dbname=os.environ.get("POSTGRES_DB", "demo"),
        )
        return conn, conn.cursor()
    raise ValueError(f"unsupported target_db: {target_db}")


def _run_sql(job: dict, business_date: str) -> dict:
    """Run SQL query on target_db. Optionally validate row count."""
    query = job["command"].replace("%%ODATE%%", business_date).replace("$ODATE", business_date)
    db = job["target_db"]
    expected = job.get("expected") or {}

    start = _now()
    try:
        conn, cur = _connect(db)
        try:
            cur.execute(query)
            rows = cur.fetchall() if cur.description else []
            row_count = len(rows)
            duration = round(_now() - start, 2)

            sample = [list(r) for r in rows[:3]]
            result = {
                "status": "success",
                "duration_seconds": duration,
                "row_count": row_count,
                "sample": sample,
                "sla_miss": duration > int(job.get("sla_seconds", 300)),
            }

            # Validation
            check = expected.get("check")
            if check == "count_gt":
                if row_count <= int(expected.get("value", 0)):
                    result["status"] = "failed"
                    result["error"] = f"DQ check failed: row_count={row_count}, expected > {expected['value']}"
            elif check == "count_eq":
                if row_count != int(expected.get("value", 0)):
                    result["status"] = "failed"
                    result["error"] = f"DQ check failed: row_count={row_count}, expected == {expected['value']}"
            elif check == "value_eq":
                target = expected.get("value")
                if not rows or str(rows[0][0]) != str(target):
                    actual = rows[0][0] if rows else None
                    result["status"] = "failed"
                    result["error"] = f"DQ check failed: first cell={actual}, expected == {target}"
            return result
        finally:
            cur.close()
            conn.close()
    except Exception as exc:
        return {
            "status": "failed",
            "duration_seconds": round(_now() - start, 2),
            "error": f"{type(exc).__name__}: {exc}",
            "sla_miss": False,
        }


def _run_etl_template(job: dict, business_date: str) -> dict:
    """Wrap a generic Oracle→SQLServer ETL using src/tgt table names.

    SELECT * FROM src WHERE business_date = :bd → INSERT INTO tgt.
    Idempotent: deletes by business_date before insert. Uses pymssql (%s
    placeholders) for the target. Source assumes a column named business_date.
    """
    src = job["src_table"]
    tgt = job["tgt_table"]
    start = _now()
    try:
        o_conn, o_cur = _connect("oracle")
        s_conn, s_cur = _connect("sqlserver")
        try:
            o_cur.execute(
                f"SELECT * FROM {src} WHERE TO_CHAR(business_date,'YYYY-MM-DD') = :bd",
                {"bd": business_date},
            )
            cols = [c[0] for c in o_cur.description]
            rows = o_cur.fetchall()

            placeholders = ",".join(["%s"] * len(cols))
            col_list = ",".join(cols)
            s_cur.execute(f"DELETE FROM {tgt} WHERE business_date = %s", (business_date,))
            for r in rows:
                s_cur.execute(f"INSERT INTO {tgt} ({col_list}) VALUES ({placeholders})", tuple(r))
            s_conn.commit()

            return {
                "status": "success",
                "duration_seconds": round(_now() - start, 2),
                "rows_copied": len(rows),
                "src": src,
                "tgt": tgt,
                "sla_miss": False,
            }
        finally:
            o_cur.close(); o_conn.close()
            s_cur.close(); s_conn.close()
    except Exception as exc:
        return {
            "status": "failed",
            "duration_seconds": round(_now() - start, 2),
            "error": f"{type(exc).__name__}: {exc}",
            "sla_miss": False,
        }


def execute_custom(job: dict, business_date: str) -> dict:
    """Dispatch to the right runner based on job['type'].

    Emits OpenLineage START/COMPLETE/FAIL events around the run when the
    emitter is available. Inputs/outputs are inferred from job spec (see
    openlineage_emitter.io_for_custom_job).
    """
    jtype = job.get("type", "shell")
    job_name = job.get("id") or job.get("label") or "unknown"
    run_id = str(uuid.uuid4())

    # ─── Structured "job started" log (becomes a Logs facet record) ──────────
    _log_json("info", message=f"[{job_name}] STARTED", ctm_job=job_name,
              business_date=business_date, ctm_run_id=run_id, job_type=jtype,
              status="started")

    # ─── DogStatsD: same metrics as built-in jobs ────────────────────────────
    if _METRICS_ENABLED:
        try:
            _ctm_started(job_name, business_date, run_id=run_id)
        except Exception as exc:
            logger.warning("metrics emit_job_started failed: %s", exc)

    # Derive lineage I/O (best-effort; empty list is OK)
    inputs, outputs = ([], [])
    if _OL_ENABLED:
        try:
            inputs, outputs = io_for_custom_job(job)
            _ol_emit_start(job_name, run_id, inputs=inputs, outputs=outputs, business_date=business_date)
        except Exception as exc:
            logger.warning("OpenLineage START emit failed for %s: %s", job_name, exc)

    # Retry loop
    retries = int(job.get("retries", 0))
    last = None
    for attempt in range(retries + 1):
        if jtype == "shell":
            last = _run_shell(job, business_date)
        elif jtype == "python":
            last = _run_python(job, business_date)
        elif jtype == "sql":
            last = _run_sql(job, business_date)
        elif jtype == "etl_template":
            last = _run_etl_template(job, business_date)
        else:
            last = {"status": "failed", "error": f"unknown job type: {jtype}"}
            break

        last["retries"] = attempt
        last["attempt"] = attempt + 1
        if last.get("status") == "success":
            break
        if attempt < retries:
            time.sleep(min(2 ** attempt, 10))

    # Emit final lineage event
    if _OL_ENABLED and last is not None:
        try:
            if last.get("status") == "success":
                _ol_emit_complete(job_name, run_id, inputs=inputs, outputs=outputs, business_date=business_date)
            else:
                _ol_emit_fail(job_name, run_id, inputs=inputs, outputs=outputs, business_date=business_date, error=last.get("error", "failed"))
        except Exception as exc:
            logger.warning("OpenLineage final emit failed for %s: %s", job_name, exc)

    if last is None:
        last = {"status": "failed", "error": "no execution result"}
    last.setdefault("run_id", run_id)

    # ─── Final DogStatsD + structured log ───────────────────────────────────
    duration = last.get("duration_seconds") or 0
    sla = int(job.get("sla_seconds", 300))
    sla_miss = bool(last.get("sla_miss")) or (duration > sla)
    final_status = last.get("status") == "success"

    if _METRICS_ENABLED:
        try:
            if final_status:
                _ctm_ok(job_name, business_date, run_id=run_id,
                        duration_seconds=duration, retries=last.get("retries", 0),
                        sla_seconds=sla, sla_miss=sla_miss)
            else:
                _ctm_not_ok(job_name, business_date, run_id=run_id,
                            duration_seconds=duration, retries=last.get("retries", 0),
                            sla_seconds=sla, sla_miss=sla_miss,
                            error=str(last.get("error", "failed")))
        except Exception as exc:
            logger.warning("metrics emit_job_ended failed: %s", exc)

    _log_json(
        "info" if final_status else "error",
        message=f"[{job_name}] {'ENDED OK' if final_status else 'ENDED NOT OK'} — {duration}s",
        ctm_job=job_name,
        business_date=business_date,
        ctm_run_id=run_id,
        job_type=jtype,
        status="ended_ok" if final_status else "ended_not_ok",
        duration_seconds=duration,
        sla_seconds=sla,
        sla_miss=sla_miss,
        retries=last.get("retries", 0),
        row_count=last.get("row_count"),
        rows_copied=last.get("rows_copied"),
        error=last.get("error") if not final_status else None,
    )

    return last
