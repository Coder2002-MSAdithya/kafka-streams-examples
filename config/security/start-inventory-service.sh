#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
source "$(dirname "$0")/_policy_agent.sh"
require_standalone_jar
STATE_DIR="${STATE_DIR:-/tmp/kafka-streams-examples}"
readarray -t AGENT_OPTS < <(policy_agent_java_opts inventory-svc InventoryService "${STATE_DIR}")
exec java "${AGENT_OPTS[@]}" -cp "$(streams_agent_classpath)" io.confluent.examples.streams.microservices.InventoryService \
  -b "${BOOTSTRAP_SERVERS}" \
  -s "${SCHEMA_REGISTRY_URL:-http://localhost:8081}" \
  -c "${SCRIPT_DIR}/inventory-service.properties" \
  -t "${STATE_DIR}"
