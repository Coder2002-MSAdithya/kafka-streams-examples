#!/usr/bin/env bash
# Create business input/output topics for the microservices order workflow.
# Uses admin SCRAM credentials (see admin-client.properties).
#
# Kafka Streams also auto-creates internal topics (changelog, repartition, etc.)
# when each service starts, if the principal has CREATE permission (see create-acls.sh).
#
# Topics (from domain/Schemas.java):
#   orders              - main order events (REST + all validators)
#   order-validations   - PASS/FAIL from Fraud, Inventory, OrderDetails
#   warehouse-inventory - stock levels (AddInventory → InventoryService)
#   payments            - EmailService join input
#   customers           - EmailService global table input
#   orders-enriched     - EmailService output

set -euo pipefail
source "$(dirname "$0")/_env.sh"

BS="${BOOTSTRAP_SERVERS}"
PARTITIONS="${TOPIC_PARTITIONS:-6}"
REPLICATION="${TOPIC_REPLICATION_FACTOR:-2}"
TOPICS_BIN="${KAFKA_HOME}/bin/kafka-topics.sh"

create_topic() {
  local name="$1"
  local desc="$2"
  echo "Creating topic '${name}' (${desc}): partitions=${PARTITIONS} rf=${REPLICATION}"
  "${TOPICS_BIN}" --bootstrap-server "${BS}" \
    --command-config "${ADMIN_CONFIG}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}"
}

echo "Bootstrap: ${BS}"

create_topic "orders" \
  "input: POST order; output: validated/failed orders"

create_topic "order-validations" \
  "output: Fraud/Inventory/OrderDetails; input: ValidationsAggregator"

create_topic "warehouse-inventory" \
  "input: AddInventory; table source: InventoryService"

create_topic "payments" \
  "input: EmailService (join with orders)"

create_topic "customers" \
  "input: EmailService global table"

create_topic "orders-enriched" \
  "output: EmailService"

echo ""
echo "Listing microservices topics:"
"${TOPICS_BIN}" --bootstrap-server "${BS}" \
  --command-config "${ADMIN_CONFIG}" \
  --list | grep -E '^(orders|order-validations|warehouse-inventory|payments|customers|orders-enriched)$' || true

echo "Done."
