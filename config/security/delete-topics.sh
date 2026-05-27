#!/usr/bin/env bash
# Delete microservices business topics (destructive). Streams internal topics are not removed.

set -euo pipefail
source "$(dirname "$0")/_env.sh"

BS="${BOOTSTRAP_SERVERS}"
TOPICS_BIN="${KAFKA_HOME}/bin/kafka-topics.sh"

TOPICS=(
  orders
  order-validations
  warehouse-inventory
  payments
  customers
  orders-enriched
)

for topic in "${TOPICS[@]}"; do
  echo "Deleting topic: ${topic}"
  "${TOPICS_BIN}" --bootstrap-server "${BS}" \
    --command-config "${ADMIN_CONFIG}" \
    --delete --topic "${topic}" 2>/dev/null || echo "  (not found or delete disabled)"
done

echo "Done. Internal Streams topics (e.g. FraudService-*) may still exist; delete manually if needed."
