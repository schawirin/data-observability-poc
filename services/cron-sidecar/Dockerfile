# Sidecar that rotates demo scenarios every $INTERVAL_SECONDS to keep Datadog
# populated with continuous DJM/Lineage/DBM data.
#
# Communicates with the rest of the stack two ways:
#  1. docker socket on /var/run/docker.sock (to call `docker exec` against the
#     other compose services for `make demo-*`)
#  2. HTTP to the controlm-sim and pipeline-runner endpoints if needed
FROM alpine:3.20

RUN apk add --no-cache bash make curl docker-cli mysql-client coreutils

WORKDIR /repo

# rotate.sh is mounted from services/cron-sidecar at runtime so iteration is fast.
ENTRYPOINT ["/bin/bash", "/cron/rotate.sh"]
