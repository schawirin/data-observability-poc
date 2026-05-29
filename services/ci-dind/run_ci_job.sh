#!/bin/sh
set -eu

LOG_FILE="${CI_LOG_FILE:-/logs/dind-ci.log}"
PIPELINE_NAME="${CI_PIPELINE_NAME:-b3-dind-demo}"
PIPELINE_ID="${CI_PIPELINE_ID:-$(date +%Y%m%d%H%M%S)}"
BUILD_ID="${CI_BUILD_ID:-${PIPELINE_ID}}"
IMAGE_NAME="${CI_IMAGE_NAME:-b3-dind-sample:${BUILD_ID}}"
WORKDIR="/tmp/${PIPELINE_NAME}-${BUILD_ID}"

mkdir -p "$(dirname "$LOG_FILE")" "$WORKDIR"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

emit() {
  stage="$1"
  status="$2"
  message="$3"
  now="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf '{"timestamp":"%s","service":"ci-jenkins-job","env":"demo","component":"ci-cd","ci_provider":"jenkins","pipeline":"%s","pipeline_id":"%s","build_id":"%s","stage":"%s","status":"%s","message":"%s"}\n' \
    "$now" \
    "$(json_escape "$PIPELINE_NAME")" \
    "$(json_escape "$PIPELINE_ID")" \
    "$(json_escape "$BUILD_ID")" \
    "$(json_escape "$stage")" \
    "$(json_escape "$status")" \
    "$(json_escape "$message")" | tee -a "$LOG_FILE"
}

run_step() {
  stage="$1"
  shift
  emit "$stage" "started" "running: $*"
  started="$(date +%s)"
  set +e
  output="$("$@" 2>&1)"
  code="$?"
  set -e
  printf '%s\n' "$output" | while IFS= read -r line; do
    [ -n "$line" ] && emit "$stage" "log" "$line"
  done
  elapsed="$(( $(date +%s) - started ))"
  if [ "$code" -eq 0 ]; then
    emit "$stage" "success" "completed in ${elapsed}s"
  else
    emit "$stage" "failed" "exit_code=${code} after ${elapsed}s"
    exit "$code"
  fi
}

emit "pipeline" "started" "Jenkins-like DinD CI job started"

cat > "${WORKDIR}/Dockerfile" <<EOF
FROM alpine:3.20
ARG BUILD_ID
LABEL demo.pipeline="${PIPELINE_NAME}"
LABEL demo.ci_provider="jenkins"
LABEL demo.build_id="${BUILD_ID}"
RUN echo "artifact built by DinD build ${BUILD_ID}" > /artifact.txt
CMD ["sh", "-c", "echo nested_container_start build_id=${BUILD_ID}; cat /artifact.txt; sleep 2; echo nested_container_done"]
EOF

run_step "docker-info" docker info
run_step "docker-build" docker build --progress=plain --build-arg "BUILD_ID=${BUILD_ID}" -t "$IMAGE_NAME" "$WORKDIR"
run_step "docker-run" docker run --rm --name "b3-nested-${BUILD_ID}" \
  --label "com.datadoghq.tags.env=demo" \
  --label "com.datadoghq.tags.service=ci-nested-container" \
  --label "demo.pipeline=${PIPELINE_NAME}" \
  "$IMAGE_NAME"
run_step "docker-inventory" docker ps -a
run_step "docker-disk" docker system df

emit "pipeline" "success" "Jenkins-like DinD CI job finished"
