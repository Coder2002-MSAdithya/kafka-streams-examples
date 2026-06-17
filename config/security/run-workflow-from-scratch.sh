#!/usr/bin/env bash
# Wipe, format, start cluster + microservices, seed inventory, post orders, print log summary.
set -euo pipefail
source "$(dirname "$0")/_env.sh"
cd "${SCRIPT_DIR}"
require_standalone_jar

LOG_ROOT="${LOG_ROOT:-/tmp/ms-workflow-logs}"
mkdir -p "${LOG_ROOT}"

kill_listeners() {
  echo "Stopping any prior demo processes on demo ports..."
  for port in 9092 9094 9093 8081 5432; do
    if command -v fuser >/dev/null 2>&1; then
      fuser -k "${port}/tcp" 2>/dev/null || true
    fi
  done
  pkill -f 'kafka-server-start.*broker.node' 2>/dev/null || true
  pkill -f 'kafka-server-start.*controller.properties' 2>/dev/null || true
  pkill -f 'schema-registry-start' 2>/dev/null || true
  pkill -f 'io.confluent.examples.streams.microservices' 2>/dev/null || true
  sleep 2
}

wait_for_port() {
  local port="$1"
  local label="$2"
  local tries="${3:-60}"
  for ((n = 1; n <= tries; n++)); do
    if ss -tln 2>/dev/null | grep -q ":${port} "; then
      echo "${label} listening on :${port}"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: ${label} did not start on :${port}" >&2
  return 1
}

wait_for_kafka() {
  local tries="${1:-90}"
  for ((n = 1; n <= tries; n++)); do
    if "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      --command-config "${ADMIN_CONFIG}" >/dev/null 2>&1; then
      echo "Kafka cluster is reachable at ${BOOTSTRAP_SERVERS}"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: Kafka cluster not reachable" >&2
  return 1
}

wait_for_orders_http() {
  local tries="${1:-90}"
  for ((n = 1; n <= tries; n++)); do
    if ss -tln 2>/dev/null | grep -q ':5432 '; then
      if grep -q 'Order Service listening at:' "${LOG_ROOT}/orders-service.log" 2>/dev/null; then
        echo "OrdersService is up"
        return 0
      fi
    fi
    sleep 2
  done
  echo "ERROR: OrdersService did not become ready" >&2
  tail -30 "${LOG_ROOT}/orders-service.log" 2>/dev/null || true
  return 1
}

start_bg() {
  local name="$1"
  shift
  local log="${LOG_ROOT}/${name}.log"
  echo "Starting ${name} -> ${log}"
  nohup "$@" >"${log}" 2>&1 &
  echo "${name}:$!" >> "${LOG_ROOT}/pids.txt"
}

kill_listeners

echo "=== Wiping and formatting cluster ==="
./wipe-cluster-logs.sh
STATE_DIR="${STATE_DIR:-/tmp/kafka-streams-examples}"
if [[ -d "${STATE_DIR}" ]]; then
  echo "Removing Kafka Streams state ${STATE_DIR}"
  rm -rf "${STATE_DIR}"
fi
./format-kraft-storage.sh

echo "=== Starting Kafka + Schema Registry ==="
start_bg controller "${KAFKA_HOME}/bin/kafka-server-start.sh" "${CONTROLLER_CONFIG}"
wait_for_port 9093 controller 60
start_bg broker-1 env KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}" \
  "${KAFKA_HOME}/bin/kafka-server-start.sh" "${BROKER1_CONFIG}"
start_bg broker-2 env KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}" \
  "${KAFKA_HOME}/bin/kafka-server-start.sh" "${BROKER2_CONFIG}"
wait_for_port 9092 broker-1 90
wait_for_port 9094 broker-2 90
wait_for_kafka 90

CONFLUENT_HOME="${CONFLUENT_HOME:-$(cd "${EXAMPLES_HOME}/../confluent-8.1.1" && pwd)}"
start_bg schema-registry "${CONFLUENT_HOME}/bin/schema-registry-start" "${SCRIPT_DIR}/schema-registry.properties"
wait_for_port 8081 schema-registry 90

echo "=== ACLs and topics ==="
./create-acls.sh | tee "${LOG_ROOT}/create-acls.log"
./create-topics.sh | tee "${LOG_ROOT}/create-topics.log"

echo "=== Starting microservices (DIFC order: orders -> validators -> aggregator) ==="
start_bg orders-service ./start-orders-service.sh
wait_for_orders_http 120

start_bg fraud-service ./start-fraud-service.sh
start_bg inventory-service ./start-inventory-service.sh
start_bg order-details-service ./start-order-details-service.sh
start_bg email-service ./start-email-service.sh
sleep 20

start_bg validations-aggregator ./start-validations-aggregator-service.sh
sleep 15

echo "=== Seeding warehouse inventory ==="
./start-add-inventory.sh | tee "${LOG_ROOT}/add-inventory.log"

echo "=== Posting multiple orders ==="
./post-multiple-orders.sh 13 | tee "${LOG_ROOT}/post-orders.log"

echo "=== Waiting for pipeline to process orders ==="
sleep 30

echo "=== Processing policy graphs and relational algebra ==="
./print-processing-policies.sh "${LOG_ROOT}/processing-policies.log"

echo "=== Log summary ==="
summarize() {
  local file="$1"
  local pattern="$2"
  echo ""
  echo "---- ${file} (${pattern}) ----"
  grep -E "${pattern}" "${LOG_ROOT}/${file}.log" 2>/dev/null | tail -20 || echo "(no matches)"
}

summarize orders-service '\[OrdersService\]|\[DIFC\]'
summarize fraud-service '\[FraudService\]|Aggregation decision|validation|DIFC'
summarize inventory-service '\[InventoryService\]|validation|DIFC'
summarize order-details-service '\[OrderDetailsService\]|validation|DIFC'
summarize email-service '\[EmailService\]|DIFC'
summarize validations-aggregator 'Aggregation decision|DIFC|VALIDATED|FAILED'
summarize broker-1 'DIFC|GRANT_CAP|ADD_CLIENT_PRIVS|Final message tags'
summarize broker-2 'DIFC|GRANT_CAP|ADD_CLIENT_PRIVS|Final message tags'

echo ""
echo "Full logs under ${LOG_ROOT}"
echo "Workflow run complete."
