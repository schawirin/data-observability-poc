#!/usr/bin/env bash
# cron_rotate_demos.sh — runs one demo scenario per invocation so that
# scheduled executions (every 15 min) populate Datadog with a steady mix of
# happy paths, DQ failures, and slow-query traces.
#
# Driven by launchd: ~/Library/LaunchAgents/com.b3.pipeline-cron.plist
# Logs:   $REPO/logs/cron.out  /  $REPO/logs/cron.err
# Lock:   $REPO/logs/cron.lock   (skips invocation if a previous run is still going)

set -uo pipefail

REPO="/Users/pedro.schawirin/Documents/costumer/b3"
LOG_DIR="${REPO}/logs"
LOCK_FILE="${LOG_DIR}/cron.lock"
STATE_FILE="${LOG_DIR}/cron.state"     # remembers last scenario index
PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export PATH

mkdir -p "${LOG_DIR}"

# ── Single-instance lock — skip if a previous demo is still running ─────────
if [ -e "${LOCK_FILE}" ]; then
  prev_pid=$(cat "${LOCK_FILE}" 2>/dev/null || echo "")
  if [ -n "${prev_pid}" ] && kill -0 "${prev_pid}" 2>/dev/null; then
    echo "$(date '+%F %T')  SKIP — previous run still active (pid ${prev_pid})"
    exit 0
  fi
  rm -f "${LOCK_FILE}"
fi
echo $$ > "${LOCK_FILE}"
trap 'rm -f "${LOCK_FILE}"' EXIT

cd "${REPO}" || exit 1

# Load credentials and tunables from .env so `make demo-*` sees MYSQL_ROOT_PASSWORD
# (the Makefile uses ${MYSQL_ROOT_PASSWORD:-demopoc2026} but our .env overrides it).
if [ -f "${REPO}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "${REPO}/.env"
  set +a
fi

# ── Scenario rotation ───────────────────────────────────────────────────────
# Each invocation picks the next scenario in order. State persists in cron.state.
# Keep this aligned with services/cron-sidecar/rotate.sh.
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

idx=0
[ -f "${STATE_FILE}" ] && idx=$(cat "${STATE_FILE}")
[ -z "${idx}" ] && idx=0
scenario="${SCENARIOS[${idx} % N]}"
next_idx=$(( (idx + 1) % N ))
echo "${next_idx}" > "${STATE_FILE}"

# Rotate business_date to spread datapoints across the last 5 weekdays.
# (Datadog DJM groups by business_date facet — variety makes the timeline richer.)
day_offset=$(( idx % 5 ))
BUSINESS_DATE=$(date -v-"${day_offset}"d +%Y-%m-%d)
export BUSINESS_DATE

# ── Pre-checks ──────────────────────────────────────────────────────────────
if ! podman ps --filter "name=demo-datadog-agent" --format "{{.Status}}" | grep -q "healthy\|Up"; then
  echo "$(date '+%F %T')  ABORT — demo-datadog-agent is not running. Run 'make up' first."
  exit 1
fi

# ── Run scenario ────────────────────────────────────────────────────────────
echo "════════════════════════════════════════════════════════════════"
echo " $(date '+%F %T')  Running scenario: ${scenario}  (BUSINESS_DATE=${BUSINESS_DATE})"
echo "════════════════════════════════════════════════════════════════"

start=$(date +%s)
make "${scenario}" BUSINESS_DATE="${BUSINESS_DATE}"
rc=$?
elapsed=$(( $(date +%s) - start ))

echo "$(date '+%F %T')  Scenario ${scenario} exit=${rc}  elapsed=${elapsed}s"

exit "${rc}"
