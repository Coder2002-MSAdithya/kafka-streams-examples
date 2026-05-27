#!/usr/bin/env bash
# Smoke-test: each service config can authenticate (metadata request).
# Run while the SASL broker is up.

set -euo pipefail
source "$(dirname "$0")/_env.sh"

BS="${BOOTSTRAP_SERVERS}"
TOPICS="${KAFKA_HOME}/bin/kafka-topics.sh"

check_config() {
  local label="$1"
  local config="$2"
  echo -n "  ${label}: "
  if "${TOPICS}" --bootstrap-server "${BS}" --command-config "${config}" --list >/dev/null 2>&1; then
    echo "OK"
  else
    echo "FAILED"
    return 1
  fi
}

echo "Verifying SCRAM principals against ${BS}"
failed=0
check_config "admin" "${ADMIN_CONFIG}" || failed=1
check_config "orders-svc" "${SCRIPT_DIR}/orders-service.properties" || failed=1
check_config "fraud-svc" "${SCRIPT_DIR}/fraud-service.properties" || failed=1
check_config "inventory-svc" "${SCRIPT_DIR}/inventory-service.properties" || failed=1
check_config "order-details-svc" "${SCRIPT_DIR}/order-details-service.properties" || failed=1
check_config "validations-agg-svc" "${SCRIPT_DIR}/validations-aggregator-service.properties" || failed=1
check_config "email-svc" "${SCRIPT_DIR}/email-service.properties" || failed=1

if [[ "${failed}" -eq 0 ]]; then
  echo "All principals authenticated successfully."
else
  exit 1
fi
