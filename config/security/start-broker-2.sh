#!/usr/bin/env bash
# Start broker 2 (node 3, SASL client port 9094). Start after the controller is up.

set -euo pipefail
source "$(dirname "$0")/_env.sh"

echo "KAFKA_HOME=${KAFKA_HOME}"
echo "KAFKA_OPTS=${KAFKA_OPTS}"
echo "Starting broker 2 (node 3) on SASL_PLAINTEXT://localhost:9094"
exec "${KAFKA_HOME}/bin/kafka-server-start.sh" "${BROKER2_CONFIG}"
