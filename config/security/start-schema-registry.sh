#!/usr/bin/env bash
# Start Confluent Schema Registry against the SASL microservices broker.

set -euo pipefail
source "$(dirname "$0")/_env.sh"

CONFLUENT_HOME="${CONFLUENT_HOME:-$(cd "${EXAMPLES_HOME}/../confluent-8.1.1" 2>/dev/null && pwd || true)}"
SR_CONFIG="${SCRIPT_DIR}/schema-registry.properties"

if [[ -z "${CONFLUENT_HOME}" || ! -x "${CONFLUENT_HOME}/bin/schema-registry-start" ]]; then
  echo "ERROR: Set CONFLUENT_HOME to your Confluent install (contains bin/schema-registry-start)" >&2
  exit 1
fi

echo "CONFLUENT_HOME=${CONFLUENT_HOME}"
echo "Using ${SR_CONFIG}"
exec "${CONFLUENT_HOME}/bin/schema-registry-start" "${SR_CONFIG}"
