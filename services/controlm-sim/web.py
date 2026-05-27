"""Flask web UI for the Data Pipeline POC — real-time console with SSE.

Orchestrates Control-M simulation with Oracle → SQL Server ETL flow.
"""

import json
import logging
import os
import queue
import sys
import threading
import time
import uuid
from datetime import datetime

from flask import Flask, Response, render_template, request, jsonify, stream_with_context

from executor import execute_pipeline, execute_job
import catalog
from executor_custom import execute_custom
from log_correlation import install_file_logging

app = Flask(__name__)

_log_queues: dict[str, queue.Queue] = {}
_log_queues_lock = threading.Lock()
_pipeline_state = {"status": "idle", "result": None}


install_file_logging()


def _broadcast(msg_type, text, **extra):
    ts = datetime.now().strftime("%H:%M:%S")
    data = {"type": msg_type, "text": text, "timestamp": ts, **extra}
    with _log_queues_lock:
        for q in _log_queues.values():
            q.put(data)


# ── Fault injection options for the 3 DQ cases ────────────────────────────────

FAULT_OPTIONS = {
    "duplicate_trades": "Caso 1: 4 linhas duplicadas em ASTADRVT_TRADE_MVMT → ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
    "null_settlement_price": "Caso 2: Coluna settlement_price vazia em ASTANO_FGBE_DRVT_PSTN → ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
    "zero_sum_positions": "Caso 3: Soma long_value + short_value = 0 em ASTACASH_MRKT_PSTN → ADWPM_POSICAO_MERCADO_A_VISTA",
    "all": "Todos os 3 casos de falha injetados simultaneamente",
    "oracle_timeout": "HARD FAIL: close_market_eod falha com ORA-12170 TNS timeout",
    "gate_fail_hard": "HARD FAIL: quality_gate_d1 aborta pipeline se encontrar critical failures",
    "s3_down": "HARD FAIL: publish_d1_reports falha com S3/MinIO connection refused",
    "all_hard": "Todos os 3 DQ casos + quality gate hard fail (pipeline ENDED NOT OK)",
    "db_blocking": "DB: Blocking query (exclusive lock 10s em ADWPM_DQ_RESULTS)",
    "db_deadlock": "DB: Deadlock entre ADWPM_MOVIMENTO e ADWPM_POSICAO",
    "db_slow_query": "DB: Query lenta (cross join pesado)",
    "db_all": "DB: Blocking + Deadlock + Slow Query + DQ faults",
}


def _run_pipeline_thread(business_date, inject_fault=None):
    """Run the full Oracle→SQL Server ETL pipeline in a background thread."""
    global _pipeline_state
    _pipeline_state = {
        "status": "running",
        "result": None,
        "business_date": business_date,
        "inject_fault": inject_fault,
        "started_at": datetime.utcnow().isoformat() + "Z",
        "active_job": None,
        "jobs": {},
    }

    try:
        fault_desc = FAULT_OPTIONS.get(inject_fault, "Nenhuma") if inject_fault else "Nenhuma"
        _broadcast("info", f"[controlm-sim] Pipeline 'b3_d1_pipeline' iniciando para {business_date}")
        _broadcast("info", f"[controlm-sim] Falha injetada: {fault_desc}")

        # Set env vars for hard faults (these cause actual job failures)
        if inject_fault in ("oracle_timeout",):
            os.environ["_FAULT_ORACLE_TIMEOUT"] = "5"  # survives all 3 retries
        if inject_fault in ("gate_fail_hard", "all_hard"):
            os.environ["DQ_GATE_FAIL_HARD"] = "1"
        if inject_fault in ("s3_down",):
            os.environ["_FAULT_S3_DOWN"] = "5"  # survives all 3 retries
        if inject_fault in ("db_blocking", "db_all"):
            os.environ["_FAULT_DB_BLOCKING"] = "1"
        if inject_fault in ("db_deadlock", "db_all"):
            os.environ["_FAULT_DB_DEADLOCK"] = "1"
        if inject_fault in ("db_slow_query", "db_all"):
            os.environ["_FAULT_DB_SLOW_QUERY"] = "1"

        # Map compound faults to Oracle seed faults
        oracle_fault = inject_fault
        if inject_fault in ("gate_fail_hard", "all_hard"):
            oracle_fault = "all"
        elif inject_fault == "db_all":
            oracle_fault = "all"  # also inject DQ data faults
        elif inject_fault in ("oracle_timeout", "gate_fail_hard", "s3_down",
                              "db_blocking", "db_deadlock", "db_slow_query"):
            oracle_fault = None  # no DQ data fault, just DB contention

        # Run the ETL pipeline (Oracle → SQL Server)
        _broadcast("info", "[controlm-sim] Iniciando pipeline ETL Oracle → SQL Server...")
        if inject_fault:
            _broadcast("warn", f"[fault-inject] Falha programada: {fault_desc}")

        # Intercept structured logs to broadcast to SSE
        import builtins
        original_print = builtins.print

        def progress_update(event):
            if event.get("event") != "job-start":
                return
            job = event.get("job_name")
            if not job:
                return
            job_state = _pipeline_state.setdefault("jobs", {}).setdefault(job, {})
            job_state.update({
                "status": "running",
                "start": datetime.utcnow().isoformat() + "Z",
                "run_id": event.get("run_id"),
            })
            _pipeline_state["active_job"] = job
            _broadcast(
                "job-start",
                f"[{job}] STARTED — executando no wrapper Python",
                job=job,
                run_id=event.get("run_id"),
                pipeline_run_id=event.get("pipeline_run_id"),
            )

        def intercepted_print(*args, **kwargs):
            original_print(*args, **kwargs)
            if args:
                text = str(args[0])
                if text.startswith("{"):
                    try:
                        data = json.loads(text)
                        job = data.get("job_name", "")
                        status = data.get("status", "")
                        duration = data.get("duration_seconds", "?")
                        rows = data.get("output_rows", "")

                        if status == "success":
                            job_state = _pipeline_state.setdefault("jobs", {}).setdefault(job, {})
                            job_state.update({
                                "status": "success",
                                "end": datetime.utcnow().isoformat() + "Z",
                                "duration_seconds": duration,
                                "retries": data.get("retries", 0),
                                "sla_miss": data.get("sla_miss", False),
                            })
                            if _pipeline_state.get("active_job") == job:
                                _pipeline_state["active_job"] = None
                            msg = f"[{job}] ENDED OK — {duration}s"
                            if rows:
                                msg += f" ({rows} rows)"
                            _broadcast(
                                "success", msg,
                                job=job,
                                job_status="success",
                                finished=True,
                                duration_seconds=duration,
                                retries=data.get("retries", 0),
                                sla_miss=data.get("sla_miss", False),
                            )
                        elif status == "failed":
                            job_state = _pipeline_state.setdefault("jobs", {}).setdefault(job, {})
                            job_state.update({
                                "status": "failed",
                                "end": datetime.utcnow().isoformat() + "Z",
                                "duration_seconds": duration,
                                "retries": data.get("retries", 0),
                                "sla_miss": data.get("sla_miss", False),
                                "error": data.get("error", ""),
                            })
                            if _pipeline_state.get("active_job") == job:
                                _pipeline_state["active_job"] = None
                            _broadcast("error",
                                f"[{job}] ENDED NOT OK — {data.get('error', '')[:200]}",
                                job=job,
                                job_status="failed",
                                finished=True,
                                duration_seconds=duration,
                                retries=data.get("retries", 0),
                                sla_miss=data.get("sla_miss", False))
                        elif status == "retry":
                            _broadcast("warn",
                                f"[{job}] RETRY #{data.get('retries', '?')} — {data.get('error', '')[:100]}",
                                job=job)
                    except json.JSONDecodeError:
                        pass

        builtins.print = intercepted_print

        try:
            summary = execute_pipeline(
                business_date,
                inject_fault=oracle_fault,
                progress_callback=progress_update,
            )
        finally:
            builtins.print = original_print
            # Clean up fault env vars
            for var in ["_FAULT_ORACLE_TIMEOUT", "DQ_GATE_FAIL_HARD", "_FAULT_S3_DOWN",
                        "_FAULT_DB_BLOCKING", "_FAULT_DB_DEADLOCK", "_FAULT_DB_SLOW_QUERY"]:
                os.environ.pop(var, None)

        _pipeline_state = {"status": summary["status"], "result": summary}

        if summary["status"] == "success":
            # Extract DQ gate results if available
            qg = summary.get("jobs", {}).get("quality_gate_d1", {}).get("result", {})
            gate_status = qg.get("gate_status", "N/A") if isinstance(qg, dict) else "N/A"
            checks_failed = qg.get("checks_failed", 0) if isinstance(qg, dict) else 0

            _broadcast("pipeline-complete",
                f"[b3_d1_pipeline] PIPELINE CONCLUÍDO — "
                f"DQ Gate: {gate_status} ({checks_failed} checks falharam) — "
                f"{summary['duration_seconds']}s",
                result=summary)
        else:
            _broadcast("pipeline-failed",
                f"[b3_d1_pipeline] PIPELINE FALHOU — {summary.get('duration_seconds', '?')}s",
                result=summary)

    except Exception as exc:
        _pipeline_state = {"status": "error", "result": str(exc)}
        _broadcast("error", f"[sistema] Erro fatal: {exc}")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    dd_site = os.environ.get("DD_SITE", "datadoghq.com")
    return render_template("index.html", dd_site=dd_site)


@app.route("/api/run", methods=["POST"])
def api_run():
    if _pipeline_state["status"] == "running":
        return jsonify({"error": "Pipeline ja esta rodando"}), 409

    data = request.get_json(force=True)
    business_date = data.get("business_date", datetime.now().strftime("%Y-%m-%d"))
    inject_fault = data.get("inject_fault")

    thread = threading.Thread(
        target=_run_pipeline_thread,
        args=(business_date, inject_fault),
        daemon=True,
    )
    thread.start()
    return jsonify({"status": "started", "business_date": business_date, "inject_fault": inject_fault})


@app.route("/api/status")
def api_status():
    return jsonify(_pipeline_state)


@app.route("/api/lineage-health")
def api_lineage_health():
    """Check if recent OL events have been indexed by Datadog.

    Queries the custom metric `b3.ol.events_total` (emitted on every OL event).
    Metrics index in ~1 min, so this gives early confirmation that the event
    reached the Datadog SaaS — much faster than the Lineage UI (~15-30 min).
    Returns the count of events emitted by job_name in the requested window.
    """
    import os
    api_key = os.environ.get("DD_API_KEY")
    app_key = os.environ.get("DD_APP_KEY")
    dd_site = os.environ.get("DD_SITE", "datadoghq.com")
    ol_namespace = os.environ.get("OL_NAMESPACE", "b3-poc-demo-v3")
    if not api_key or not app_key:
        return jsonify({"error": "DD_API_KEY or DD_APP_KEY missing"}), 500

    minutes = int(request.args.get("minutes", 15))
    import time as _time
    to_ts = int(_time.time())
    from_ts = to_ts - minutes * 60

    query = f"sum:b3.ol.events_total{{ol_namespace:{ol_namespace}}} by {{job_name,event_type}}.as_count()"
    url = (
        f"https://api.{dd_site}/api/v1/query"
        f"?from={from_ts}&to={to_ts}&query={query}"
    )

    try:
        import urllib.parse, urllib.request
        url_safe = (
            f"https://api.{dd_site}/api/v1/query?"
            f"from={from_ts}&to={to_ts}&query={urllib.parse.quote(query)}"
        )
        req = urllib.request.Request(
            url_safe,
            headers={
                "DD-API-KEY": api_key,
                "DD-APPLICATION-KEY": app_key,
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    series = data.get("series", [])
    summary = []
    for s in series:
        scope_tags = dict(t.split(":", 1) for t in s.get("scope", "").split(",") if ":" in t)
        points = s.get("pointlist", [])
        total = sum(p[1] for p in points if p[1] is not None)
        last_ts = points[-1][0] / 1000 if points else None
        summary.append({
            "job_name":    scope_tags.get("job_name", "?"),
            "event_type":  scope_tags.get("event_type", "?"),
            "total":       int(total),
            "last_seen":   last_ts,
        })

    return jsonify({
        "window_minutes": minutes,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "query": query,
        "ol_namespace": ol_namespace,
        "series_count": len(series),
        "summary": summary,
    })


@app.route("/api/runs")
def api_runs():
    """Return the last N completed pipeline runs (from /data/runs.jsonl)."""
    import os
    limit = int(request.args.get("limit", 50))
    runs_file = "/data/runs.jsonl"
    if not os.path.exists(runs_file):
        return jsonify({"runs": []})
    try:
        with open(runs_file, "r", encoding="utf-8") as fh:
            lines = fh.readlines()
    except Exception:
        return jsonify({"runs": []})
    records = []
    for line in lines[-limit:][::-1]:  # newest first
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except Exception:
            continue
    return jsonify({"runs": records})


# ── Runtime overrides (date + fault injection per-job) ─────────────────────────
# Job catalog (built-in + custom) lives in catalog.py / /data/jobs_catalog.json

_job_overrides: dict = {}


@app.route("/api/jobs", methods=["GET"])
def api_jobs():
    """Return merged catalog (built-in + custom) with overrides."""
    merged = catalog.list_jobs()
    result = {}
    for name, defn in merged.items():
        result[name] = {**defn, "overrides": _job_overrides.get(name, {})}
    return jsonify(result)


@app.route("/api/jobs/<job_name>", methods=["PUT"])
def api_job_update(job_name):
    """Update runtime overrides (date / fault). Does NOT update the catalog."""
    if not catalog.get_job(job_name):
        return jsonify({"error": f"Unknown job: {job_name}"}), 404
    data = request.get_json(force=True)
    _job_overrides[job_name] = {
        "inject_fault": data.get("inject_fault"),
        "date_override": data.get("date_override"),
    }
    return jsonify({"status": "updated", "job": job_name, "overrides": _job_overrides[job_name]})


# ── CATALOG CRUD (custom jobs) ─────────────────────────────────────────────────

@app.route("/api/catalog", methods=["GET"])
def api_catalog_list():
    return jsonify(catalog.list_jobs())


@app.route("/api/catalog", methods=["POST"])
def api_catalog_create():
    data = request.get_json(force=True, silent=True) or {}
    try:
        record = catalog.upsert_job(data)
        _broadcast("info", f"[CTM] Job cadastrado: {record['id']} (type={record['type']})")
        return jsonify({"status": "created", "job": record}), 201
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/catalog/<job_id>", methods=["PUT"])
def api_catalog_update(job_id):
    data = request.get_json(force=True, silent=True) or {}
    data["id"] = job_id
    try:
        record = catalog.upsert_job(data)
        _broadcast("info", f"[CTM] Job atualizado: {record['id']}")
        return jsonify({"status": "updated", "job": record})
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/catalog/<job_id>", methods=["DELETE"])
def api_catalog_delete(job_id):
    try:
        ok = catalog.delete_job(job_id)
        if not ok:
            return jsonify({"error": f"job not found: {job_id}"}), 404
        _broadcast("warn", f"[CTM] Job removido: {job_id}")
        return jsonify({"status": "deleted", "job": job_id})
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/run/<job_name>", methods=["POST"])
def api_run_single(job_name):
    job = catalog.get_job(job_name)
    if not job:
        return jsonify({"error": f"Unknown job: {job_name}"}), 404

    data = request.get_json(force=True, silent=True) or {}
    business_date = (
        data.get("business_date")
        or _job_overrides.get(job_name, {}).get("date_override")
        or datetime.now().strftime("%Y-%m-%d")
    )

    is_builtin = catalog.is_builtin(job_name)

    def _run():
        _broadcast("job-start", f"[CTM] Disparando job: {job_name} ({business_date}) — type={job.get('type', 'builtin')}", job=job_name)
        try:
            if is_builtin:
                result = execute_job(job_name, business_date)
            else:
                result = execute_custom(job, business_date)

            ok = result.get("status") == "success"
            text_extra = ""
            if result.get("row_count") is not None:
                text_extra = f" ({result['row_count']} rows)"
            elif result.get("rows_copied") is not None:
                text_extra = f" ({result['rows_copied']} rows copied)"

            _broadcast(
                "success" if ok else "error",
                f"[{job_name}] {'ENDED OK' if ok else 'ENDED NOT OK'} — {result.get('duration_seconds', '?')}s{text_extra}",
                job=job_name,
                job_status="ended_ok" if ok else "ended_not_ok",
                finished=True,
                duration_seconds=result.get("duration_seconds"),
                retries=result.get("retries", 0),
                sla_miss=result.get("sla_miss", False),
                result=result,
            )

            # If the job has stdout to expose, dump tail to console
            tail = result.get("stdout_tail")
            if tail:
                for line in tail.strip().splitlines()[-20:]:
                    _broadcast("info", f"[{job_name}] {line}", job=job_name)
            err = result.get("error")
            if err and not ok:
                for line in str(err).strip().splitlines()[-10:]:
                    _broadcast("error", f"[{job_name}] {line}", job=job_name)
        except Exception as exc:
            _broadcast("error", f"[{job_name}] FALHOU: {exc}", job=job_name, job_status="ended_not_ok", finished=True)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "job": job_name, "business_date": business_date})


@app.route("/api/faults", methods=["GET"])
def api_faults():
    """Return available fault injection options."""
    return jsonify(FAULT_OPTIONS)


@app.route("/api/stream")
def api_stream():
    session_id = str(uuid.uuid4())
    q = queue.Queue()

    with _log_queues_lock:
        _log_queues[session_id] = q

    def generate():
        try:
            yield f"data: {json.dumps({'type': 'connected', 'text': '[console] Console pronto. Aguardando comando...', 'timestamp': datetime.now().strftime('%H:%M:%S')})}\n\n"
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield f"data: {json.dumps(msg, default=str)}\n\n"
                except queue.Empty:
                    yield f": keepalive\n\n"
        except GeneratorExit:
            pass
        finally:
            with _log_queues_lock:
                _log_queues.pop(session_id, None)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
