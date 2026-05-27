#!/usr/bin/env bash
# run_job.sh — POSIX wrapper for run_job.py.
#
# Use when B3 agents are Linux and Python lives in a venv. If VENV_PATH is set,
# this script activates it before invoking run_job.py. Otherwise it falls back
# to PYTHON_BIN (default: python3 on $PATH).
#
# Control-M Job:Script can call this directly:
#   FileName=run_job.sh  FilePath=/scripts  Arguments=--job X --date %%ODATE%%
#
# Variables honored (passed via Control-M variables or shell env):
#   VENV_PATH   — absolute path to venv root (e.g. /opt/b3-venv). Optional.
#   PYTHON_BIN  — python interpreter to use when no venv. Default: python3.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -n "${VENV_PATH:-}" ] && [ -f "${VENV_PATH}/bin/activate" ]; then
  # shellcheck disable=SC1091
  source "${VENV_PATH}/bin/activate"
  exec python "${SCRIPT_DIR}/run_job.py" "$@"
fi

PY="${PYTHON_BIN:-python3}"
exec "${PY}" "${SCRIPT_DIR}/run_job.py" "$@"
