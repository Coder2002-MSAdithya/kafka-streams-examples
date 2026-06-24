#!/usr/bin/env bash
# Create or update SCRAM credentials on a running broker.
# Use this if storage was formatted without --add-scram, or to add email-svc later.

set -euo pipefail
source "$(dirname "$0")/_env.sh"

BS="${BOOTSTRAP_SERVERS}"
CONFIGS="${KAFKA_HOME}/bin/kafka-configs.sh"

create_user() {
  local name="$1"
  local password="$2"
  echo "Creating SCRAM user: ${name}"
  "${CONFIGS}" --bootstrap-server "${BS}" --command-config "${ADMIN_CONFIG}" \
    --alter --add-config "SCRAM-SHA-256=[password=${password}]" \
    --entity-type users --entity-name "${name}"
}

create_user admin admin-secret
create_user orders-svc orders-secret
create_user fraud-svc fraud-secret
create_user inventory-svc inventory-secret
create_user order-details-svc order-details-secret
create_user validations-agg-svc validations-agg-secret
create_user email-svc email-secret

echo "SCRAM users ready."
