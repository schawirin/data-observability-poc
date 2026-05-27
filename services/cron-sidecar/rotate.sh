#!/usr/bin/env bash
# rotate.sh — runs inside the demo-cron-sidecar container, loops indefinitely
# rotating through demo scenarios so the Datadog dashboard stays populated
# without needing launchd/cron on the host (which gets blocked by macOS TCC).

set -uo pipefail

REPO="${REPO:-/repo}"
INTERVAL="${INTERVAL_SECONDS:-900}"
STATE_DIR="${REPO}/logs"
STATE_FILE="${STATE_DIR}/cron.state"

mkdir -p "${STATE_DIR}"

exec > >(tee -a "${STATE_DIR}/demo-cron.log") 2>&1

# Rotação ampla — cobre sucesso + 3 níveis de falha:
#   1. Happy path                        : demo-happy
#   2. DQ failures (quality_gate FAIL)   : demo-caso1, demo-caso2, demo-caso3
#   3. Job-level FAIL (job individual)   : demo-hard-oracle-timeout, demo-hard-gate-fail, demo-hard-s3-down
#   4. DBM / performance issues          : demo-slow, demo-db-blocking, demo-db-deadlock
#   5. Worst case (pipeline ENDED NOT OK): demo-hard-all
# Total: 11 cenários, ciclo completo em ~2h45 (15 min × 11)
SCENARIOS=(
  demo-happy
  demo-caso1
  demo-hard-oracle-timeout
  demo-caso2
  demo-db-blocking
  demo-caso3
  demo-hard-s3-down
  demo-slow
  demo-hard-gate-fail
  demo-db-deadlock
  demo-hard-all
)
N=${#SCENARIOS[@]}

echo "════════════════════════════════════════════════════════════════"
echo " demo-cron-sidecar started"
echo " interval=${INTERVAL}s   scenarios=${SCENARIOS[*]}"
echo " repo=${REPO}"
echo "════════════════════════════════════════════════════════════════"

# Source .env so MYSQL_ROOT_PASSWORD and DD_* vars are available to make.
if [ -f "${REPO}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "${REPO}/.env"
  set +a
fi

cd "${REPO}" || exit 1

while true; do
  idx=0
  [ -f "${STATE_FILE}" ] && idx=$(cat "${STATE_FILE}")
  [ -z "${idx}" ] && idx=0

  scenario="${SCENARIOS[$(( idx % N ))]}"
  next_idx=$(( (idx + 1) % N ))
  echo "${next_idx}" > "${STATE_FILE}"

  # Spread business_date across the last 5 weekdays so DJM timeline is varied.
  day_offset=$(( idx % 5 ))
  BUSINESS_DATE=$(date -d "-${day_offset} days" +%Y-%m-%d 2>/dev/null || date +%Y-%m-%d)
  export BUSINESS_DATE

  echo "────────────────────────────────────────────────────────────────"
  echo " $(date '+%F %T')  scenario=${scenario}  business_date=${BUSINESS_DATE}  idx=${idx}"
  echo "────────────────────────────────────────────────────────────────"

  start=$(date +%s)
  # The Makefile uses `podman` and `podman compose` — but we mount the docker
  # socket so docker-cli (installed in the image) talks to the same daemon.
  # Override CONTAINER and COMPOSE so make calls docker-cli instead of podman.
  make "${scenario}" BUSINESS_DATE="${BUSINESS_DATE}" CONTAINER=docker COMPOSE="docker compose" 2>&1
  rc=$?
  elapsed=$(( $(date +%s) - start ))

  echo "$(date '+%F %T')  done scenario=${scenario} exit=${rc} elapsed=${elapsed}s — sleeping ${INTERVAL}s"
  sleep "${INTERVAL}"
done
