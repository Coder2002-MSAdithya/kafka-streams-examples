#!/usr/bin/env bash
# Start the dedicated KRaft controller (node 1). Run this first.

set -euo pipefail
source "$(dirname "$0")/_env.sh"

echo "KAFKA_HOME=${KAFKA_HOME}"
echo "Starting controller (node 1) on CONTROLLER://localhost:9093"
# Controller uses PLAINTEXT only; no broker JAAS required, but authorizer is enabled.
unset KAFKA_OPTS
exec "${KAFKA_HOME}/bin/kafka-server-start.sh" "${CONTROLLER_CONFIG}"
