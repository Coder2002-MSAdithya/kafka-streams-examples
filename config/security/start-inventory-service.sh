#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
require_standalone_jar
exec java -cp "${STANDALONE_JAR}" io.confluent.examples.streams.microservices.InventoryService \
  -b "${BOOTSTRAP_SERVERS}" \
  -s "${SCHEMA_REGISTRY_URL:-http://localhost:8081}" \
  -c "${SCRIPT_DIR}/inventory-service.properties"
