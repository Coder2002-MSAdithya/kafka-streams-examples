#!/usr/bin/env bash
# List ACLs for a microservice principal (default: fraud-svc).

set -euo pipefail
source "$(dirname "$0")/_env.sh"

USER="${1:-fraud-svc}"
BS="${BOOTSTRAP_SERVERS}"

"${KAFKA_HOME}/bin/kafka-acls.sh" --bootstrap-server "${BS}" \
  --command-config "${ADMIN_CONFIG}" \
  --list --principal "User:${USER}"
