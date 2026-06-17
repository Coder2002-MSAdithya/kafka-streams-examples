#!/usr/bin/env bash
# Shared paths for microservices SASL scripts.
# Override KAFKA_HOME or EXAMPLES_HOME if your layout differs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLES_HOME="$(cd "${SCRIPT_DIR}/../.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-$(cd "${EXAMPLES_HOME}/../difc-for-kafka" && pwd)}"

CONTROLLER_CONFIG="${SCRIPT_DIR}/controller.properties"
BROKER1_CONFIG="${SCRIPT_DIR}/broker.node1.properties"
BROKER2_CONFIG="${SCRIPT_DIR}/broker.node2.properties"
# Legacy single-node combined broker+controller (see broker.standalone.properties)
BROKER_CONFIG="${SCRIPT_DIR}/broker.standalone.properties"

JAAS_CONFIG="${SCRIPT_DIR}/kafka_server_jaas.conf"
ADMIN_CONFIG="${SCRIPT_DIR}/admin-client.properties"

# Two-broker cluster bootstrap (SASL listeners)
export BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092,localhost:9094}"
export POLICY_AGENT_ALLOWED_EXTERNAL_HOSTS="${POLICY_AGENT_ALLOWED_EXTERNAL_HOSTS:-}"
export STATE_DIR="${STATE_DIR:-/tmp/kafka-streams-examples}"
export DIFC_ENABLED="${DIFC_ENABLED:-true}"

export KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONFIG}"

# Shared directory for agent-exported processing policies (grant-time CAN_REMOVE verification).
export DIFC_POLICY_REGISTRY_DIR="${DIFC_POLICY_REGISTRY_DIR:-/tmp/kafka-streams-examples/policy}"

# Project version (not parent POM version); used for standalone JAR path
PROJECT_VERSION="$(awk '/<artifactId>kafka-streams-examples<\/artifactId>/{found=1} found && /<version>/{gsub(/.*<version>|<\/version>.*/,""); print; exit}' "${EXAMPLES_HOME}/pom.xml")"
STANDALONE_JAR="${EXAMPLES_HOME}/target/kafka-streams-examples-${PROJECT_VERSION}-standalone.jar"

if [[ ! -x "${KAFKA_HOME}/bin/kafka-storage.sh" ]]; then
  echo "ERROR: difc-for-kafka not found at ${KAFKA_HOME}" >&2
  echo "Set KAFKA_HOME to your Kafka checkout (e.g. export KAFKA_HOME=/path/to/difc-for-kafka)" >&2
  exit 1
fi

require_standalone_jar() {
  if [[ ! -f "${STANDALONE_JAR}" ]]; then
    echo "ERROR: Missing ${STANDALONE_JAR}" >&2
    echo "Expected project version ${PROJECT_VERSION} from pom.xml (not the parent version)." >&2
    echo "Run: cd ${EXAMPLES_HOME} && mvn -DskipTests=true package" >&2
    exit 1
  fi
}
