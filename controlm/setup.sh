#!/usr/bin/env bash
# setup.sh — Deploy a Market_D1_Pipeline variant to Control-M Workbench / Helix.
#
# Usage:
#   ./controlm/setup.sh [--variant <script|command|embedded|database|all>] \
#                       [--host <CTM_HOST>] [--port <CTM_PORT>]
#
# Defaults:
#   --variant script
#   --host    localhost
#   --port    8443
#   CTM_USER  workbench   (env override)
#   CTM_PASS  workbench   (env override)
#
# Each variant maps to a generated JSON file in controlm/jobs/. Run
# `make ctm-generate` first if you edited manifest.yaml.

set -euo pipefail

VARIANT="script"
CTM_HOST="localhost"
CTM_PORT="8443"

# ── Arg parsing ─────────────────────────────────────────────────────────────
while [ $# -gt 0 ]; do
  case "$1" in
    --variant)
      VARIANT="$2"; shift 2 ;;
    --host)
      CTM_HOST="$2"; shift 2 ;;
    --port)
      CTM_PORT="$2"; shift 2 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -20
      exit 0 ;;
    *)
      # Backward compat: positional [host] [port]
      if [ "${CTM_HOST}" = "localhost" ] && [ -z "${POS_HOST_SET:-}" ]; then
        CTM_HOST="$1"; POS_HOST_SET=1; shift
      elif [ "${CTM_PORT}" = "8443" ] && [ -z "${POS_PORT_SET:-}" ]; then
        CTM_PORT="$1"; POS_PORT_SET=1; shift
      else
        echo "Unknown arg: $1"; exit 2
      fi ;;
  esac
done

CTM_USER="${CTM_USER:-workbench}"
CTM_PASS="${CTM_PASS:-workbench}"
CTM_BASE="https://${CTM_HOST}:${CTM_PORT}/automation-api"
JOBS_DIR="$(dirname "$0")/jobs"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# Map variant → JSON path. "all" deploys each in sequence.
case "${VARIANT}" in
  script|command|embedded|database)
    VARIANTS_TO_DEPLOY=("${VARIANT}") ;;
  all)
    VARIANTS_TO_DEPLOY=("script" "command" "embedded" "database") ;;
  *)
    log "ERROR: unknown --variant '${VARIANT}'. Valid: script | command | embedded | database | all"
    exit 2 ;;
esac

# ── 1. Wait for Workbench / Helix to be ready ──────────────────────────────
log "Waiting for Control-M API at ${CTM_BASE} ..."
for i in $(seq 1 30); do
  if curl -sk --max-time 3 "${CTM_BASE}/session/login" -o /dev/null; then
    log "API endpoint is responsive."
    break
  fi
  if [ "$i" -eq 30 ]; then
    log "ERROR: API did not respond after 150s."
    exit 1
  fi
  log "  attempt ${i}/30 — retrying in 5s..."
  sleep 5
done

# ── 2. Login ────────────────────────────────────────────────────────────────
log "Authenticating as ${CTM_USER}..."
TOKEN=$(curl -sk -X POST "${CTM_BASE}/session/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${CTM_USER}\",\"password\":\"${CTM_PASS}\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))")

if [ -z "$TOKEN" ]; then
  log "ERROR: Login failed. Check CTM_USER / CTM_PASS."
  exit 1
fi
log "Authenticated. Token: ${TOKEN:0:12}..."

# ── 3. Deploy each requested variant ───────────────────────────────────────
for V in "${VARIANTS_TO_DEPLOY[@]}"; do
  JSON_PATH="${JOBS_DIR}/market_d1_${V}.json"

  if [ ! -f "${JSON_PATH}" ]; then
    log "WARNING: ${JSON_PATH} not found — skipping. Run 'make ctm-generate' first."
    continue
  fi

  log "── Deploying variant '${V}' from $(basename "${JSON_PATH}") ──"
  DEPLOY_RESULT=$(curl -sk -X POST "${CTM_BASE}/deploy" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    --data-binary "@${JSON_PATH}")

  echo "$DEPLOY_RESULT" | python3 -m json.tool 2>/dev/null || echo "$DEPLOY_RESULT"

  if echo "$DEPLOY_RESULT" | python3 -c "import sys,json; r=json.load(sys.stdin); sys.exit(1 if r.get('errors') else 0)" 2>/dev/null; then
    log "  ✓ Variant '${V}' deployed successfully."
  else
    log "  ⚠ Variant '${V}' deploy returned errors — review output above."
  fi
done

# ── 4. Verify jobs are registered ──────────────────────────────────────────
log "Verifying deployed jobs..."
curl -sk -X GET "${CTM_BASE}/deploy/jobs?folder=Market_D1_Pipeline" \
  -H "Authorization: Bearer ${TOKEN}" \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
jobs = data if isinstance(data, list) else data.get('jobs', [])
print(f'  Registered jobs: {len(jobs)}')
for j in jobs:
    print(f'    - {j}' if isinstance(j, str) else f'    - {j.get(\"name\",j)}')
" 2>/dev/null || true

log "Done. Open Control-M UI: https://${CTM_HOST}:${CTM_PORT}"
log "To run the pipeline: make ctm-run-d1 BUSINESS_DATE=YYYY-MM-DD"
