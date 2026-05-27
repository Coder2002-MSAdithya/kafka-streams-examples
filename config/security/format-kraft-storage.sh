#!/usr/bin/env bash
# Format storage for: 1 controller (node 1) + 2 brokers (nodes 2 and 3).
# Wipe log dirs before re-running. SCRAM users are written on the controller format.

set -euo pipefail
source "$(dirname "$0")/_env.sh"

CLUSTER_ID="$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid)"
echo "Cluster id: ${CLUSTER_ID}"
echo "Saving to ${SCRIPT_DIR}/.cluster-id"
echo "${CLUSTER_ID}" > "${SCRIPT_DIR}/.cluster-id"

SCRAM_ARGS=(
  --add-scram 'SCRAM-SHA-256=[name=admin,password=admin-secret]'
  --add-scram 'SCRAM-SHA-256=[name=orders-svc,password=orders-secret]'
  --add-scram 'SCRAM-SHA-256=[name=fraud-svc,password=fraud-secret]'
  --add-scram 'SCRAM-SHA-256=[name=inventory-svc,password=inventory-secret]'
  --add-scram 'SCRAM-SHA-256=[name=order-details-svc,password=order-details-secret]'
  --add-scram 'SCRAM-SHA-256=[name=validations-agg-svc,password=validations-agg-secret]'
  --add-scram 'SCRAM-SHA-256=[name=email-svc,password=email-secret]'
)

echo "Formatting controller (node 1)..."
"${KAFKA_HOME}/bin/kafka-storage.sh" format --standalone -t "${CLUSTER_ID}" \
  -c "${CONTROLLER_CONFIG}" \
  "${SCRAM_ARGS[@]}"

echo "Formatting broker 1 (node 2)..."
"${KAFKA_HOME}/bin/kafka-storage.sh" format --no-initial-controllers -t "${CLUSTER_ID}" \
  -c "${BROKER1_CONFIG}"

echo "Formatting broker 2 (node 3)..."
"${KAFKA_HOME}/bin/kafka-storage.sh" format --no-initial-controllers -t "${CLUSTER_ID}" \
  -c "${BROKER2_CONFIG}"

echo "Done. Start in order: start-controller.sh → start-broker-1.sh → start-broker-2.sh"
