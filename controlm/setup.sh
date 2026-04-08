#!/usr/bin/env bash
# setup.sh — Deploy Market_D1_Pipeline to Control-M Workbench
# Usage: ./controlm/setup.sh [CTM_HOST] [CTM_PORT]
#
# Defaults: CTM_HOST=localhost, CTM_PORT=8443
# Credentials: workbench / workbench (Workbench defaults)

set -euo pipefail

CTM_HOST="${1:-localhost}"
CTM_PORT="${2:-8443}"
CTM_USER="${CTM_USER:-workbench}"
CTM_PASS="${CTM_PASS:-workbench}"
CTM_BASE="https://${CTM_HOST}:${CTM_PORT}/automation-api"
PIPELINE_JSON="$(dirname "$0")/../controlm/jobs/market_d1_pipeline.json"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── 1. Wait for Workbench to be ready ──────────────────────────────────────
log "Waiting for Control-M Workbench at ${CTM_BASE} ..."
for i in $(seq 1 30); do
  if curl -sk --max-time 3 "${CTM_BASE}/session/login" -o /dev/null; then
    log "Workbench is up."
    break
  fi
  if [ "$i" -eq 30 ]; then
    log "ERROR: Workbench did not respond after 150s. Is it running?"
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

# ── 3. Deploy job definitions ───────────────────────────────────────────────
log "Deploying Market_D1_Pipeline from ${PIPELINE_JSON}..."
DEPLOY_RESULT=$(curl -sk -X POST "${CTM_BASE}/deploy" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  --data-binary "@${PIPELINE_JSON}")

echo "$DEPLOY_RESULT" | python3 -m json.tool 2>/dev/null || echo "$DEPLOY_RESULT"

# Check for errors
if echo "$DEPLOY_RESULT" | python3 -c "import sys,json; r=json.load(sys.stdin); sys.exit(1 if r.get('errors') else 0)" 2>/dev/null; then
  log "Pipeline deployed successfully."
else
  log "WARNING: Deploy may have errors — review output above."
fi

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
