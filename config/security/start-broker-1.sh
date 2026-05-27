#!/usr/bin/env bash
# Start broker 1 (node 2, SASL client port 9092). Start after the controller is up.

set -euo pipefail
source "$(dirname "$0")/_env.sh"

echo "KAFKA_HOME=${KAFKA_HOME}"
echo "KAFKA_OPTS=${KAFKA_OPTS}"
echo "Starting broker 1 (node 2) on SASL_PLAINTEXT://localhost:9092"
exec "${KAFKA_HOME}/bin/kafka-server-start.sh" "${BROKER1_CONFIG}"
