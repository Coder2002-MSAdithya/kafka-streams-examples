#!/usr/bin/env bash
# One-shot producer: seeds warehouse-inventory, then exits.
set -euo pipefail
source "$(dirname "$0")/_env.sh"
require_standalone_jar
exec java -cp "${STANDALONE_JAR}" io.confluent.examples.streams.microservices.AddInventory \
  -b "${BOOTSTRAP_SERVERS}" \
  -c "${SCRIPT_DIR}/add-inventory.properties"
