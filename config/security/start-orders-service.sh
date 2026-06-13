#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/_env.sh"
source "$(dirname "$0")/_policy_agent.sh"
require_standalone_jar
STATE_DIR="${STATE_DIR:-/tmp/kafka-streams-examples}"
readarray -t AGENT_OPTS < <(policy_agent_java_opts orders-svc OrdersService "${STATE_DIR}")
readarray -t GRANTOR_TRUST_OPTS < <(grantor_policy_trust_java_opts "${STATE_DIR}")
# REST + KafkaProducer + KafkaStreams: agent captures app-level producer and Streams topology.
exec java "${GRANTOR_TRUST_OPTS[@]}" "${AGENT_OPTS[@]}" -cp "$(streams_agent_classpath)" io.confluent.examples.streams.microservices.OrdersService \
  -b "${BOOTSTRAP_SERVERS}" \
  -s "${SCHEMA_REGISTRY_URL:-http://localhost:8081}" \
  -c "${SCRIPT_DIR}/orders-service.properties" \
  -p "${ORDERS_PORT:-5432}" \
  -d "${STATE_DIR}"
