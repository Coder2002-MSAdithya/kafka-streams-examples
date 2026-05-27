#!/usr/bin/env bash
# Quick checks when OrdersService logs REQUEST_TIMED_OUT on produce.
set -euo pipefail
source "$(dirname "$0")/_env.sh"

BS="${BOOTSTRAP_SERVERS}"
ADMIN="${ADMIN_CONFIG}"
ORDERS_CFG="${SCRIPT_DIR}/orders-service.properties"
TOPICS="${KAFKA_HOME}/bin/kafka-topics.sh"

echo "=== Bootstrap: ${BS} ==="

echo ""
echo "=== SCRAM auth (orders-svc) ==="
"${TOPICS}" --bootstrap-server "${BS}" --command-config "${ORDERS_CFG}" --list >/dev/null \
  && echo "orders-svc: OK" || { echo "orders-svc: FAILED"; exit 1; }

echo ""
echo "=== Topic 'orders' ==="
"${TOPICS}" --bootstrap-server "${BS}" --command-config "${ADMIN_CONFIG}" \
  --describe --topic orders 2>&1 || echo "Topic 'orders' missing — run ./create-topics.sh"

echo ""
echo "=== Broker API versions (both brokers should respond) ==="
for port in 9092 9094; do
  echo -n "localhost:${port}: "
  if timeout 5 "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server "localhost:${port}" \
    --command-config "${ADMIN_CONFIG}" >/dev/null 2>&1; then
    echo "OK"
  else
    echo "FAILED (is broker listening on ${port}?)"
  fi
done

echo ""
echo "=== Produce path ==="
echo "Use ./submit-order.sh or POST http://localhost:5432/v1/orders (Avro via OrdersService)."
echo "Do NOT use kafka-console-producer on 'orders' — plain text causes Unknown magic byte errors."
