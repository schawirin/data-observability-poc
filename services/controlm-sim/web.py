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

app = Flask(__name__)

_log_queues: dict[str, queue.Queue] = {}
_log_queues_lock = threading.Lock()
_pipeline_state = {"status": "idle", "result": None}


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
    _pipeline_state = {"status": "running", "result": None}

    try:
        fault_desc = FAULT_OPTIONS.get(inject_fault, "Nenhuma") if inject_fault else "Nenhuma"
        _broadcast("info", f"[controlm-sim] Pipeline 'market-d1' iniciando para {business_date}")
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
        if inject_fault == "all_hard":
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
                            msg = f"[{job}] ENDED OK — {duration}s"
                            if rows:
                                msg += f" ({rows} rows)"
                            _broadcast("success", msg, job=job, job_status="ended_ok")
                        elif status == "failed":
                            _broadcast("error",
                                f"[{job}] ENDED NOT OK — {data.get('error', '')[:200]}",
                                job=job, job_status="ended_not_ok")
                        elif status == "retry":
                            _broadcast("warn",
                                f"[{job}] RETRY #{data.get('retries', '?')} — {data.get('error', '')[:100]}",
                                job=job)
                    except json.JSONDecodeError:
                        pass

        builtins.print = intercepted_print

        try:
            summary = execute_pipeline(business_date, inject_fault=oracle_fault)
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
                f"[market-d1] PIPELINE CONCLUÍDO — "
                f"DQ Gate: {gate_status} ({checks_failed} checks falharam) — "
                f"{summary['duration_seconds']}s",
                result=summary)
        else:
            _broadcast("pipeline-failed",
                f"[market-d1] PIPELINE FALHOU — {summary.get('duration_seconds', '?')}s",
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


# ── Job definitions ────────────────────────────────────────────────────────────

_JOB_DEFS = {
    "close_market_eod": {
        "description": "ETL: Oracle ASTADRVT_TRADE_MVMT → SQL Server ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
        "sla_seconds": 600,
        "depends_on": None,
    },
    "reconcile_d1_positions": {
        "description": "ETL: Oracle ASTANO/ASTACASH → SQL Server ADWPM_POSICAO_*",
        "sla_seconds": 900,
        "depends_on": "close_market_eod",
    },
    "quality_gate_d1": {
        "description": "DQ: 5 checks (duplicatas, nulos, soma-zero, row-count) → ADWPM_DQ_RESULTS",
        "sla_seconds": 300,
        "depends_on": "reconcile_d1_positions",
    },
    "publish_d1_reports": {
        "description": "Export: SQL Server DW → MinIO CSV/Parquet",
        "sla_seconds": 600,
        "depends_on": "quality_gate_d1",
    },
}

_job_overrides: dict = {}


@app.route("/api/jobs", methods=["GET"])
def api_jobs():
    result = {}
    for name, defn in _JOB_DEFS.items():
        result[name] = {**defn, "overrides": _job_overrides.get(name, {})}
    return jsonify(result)


@app.route("/api/jobs/<job_name>", methods=["PUT"])
def api_job_update(job_name):
    if job_name not in _JOB_DEFS:
        return jsonify({"error": f"Unknown job: {job_name}"}), 404
    data = request.get_json(force=True)
    _job_overrides[job_name] = {
        "inject_fault": data.get("inject_fault"),
        "date_override": data.get("date_override"),
    }
    return jsonify({"status": "updated", "job": job_name, "overrides": _job_overrides[job_name]})


@app.route("/api/run/<job_name>", methods=["POST"])
def api_run_single(job_name):
    if job_name not in _JOB_DEFS:
        return jsonify({"error": f"Unknown job: {job_name}"}), 404

    data = request.get_json(force=True, silent=True) or {}
    business_date = (
        data.get("business_date")
        or _job_overrides.get(job_name, {}).get("date_override")
        or datetime.now().strftime("%Y-%m-%d")
    )

    def _run():
        _broadcast("job-start", f"[CTM] Disparando job: {job_name} ({business_date})", job=job_name)
        try:
            result = execute_job(job_name, business_date)
            ok = result.get("status") == "success"
            _broadcast(
                "success" if ok else "error",
                f"[{job_name}] {'ENDED OK' if ok else 'ENDED NOT OK'} — {result.get('duration_seconds', '?')}s",
                job=job_name,
                job_status="ended_ok" if ok else "ended_not_ok",
                finished=True,
                duration_seconds=result.get("duration_seconds"),
                retries=result.get("retries", 0),
                sla_miss=result.get("sla_miss", False),
                result=result,
            )
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
