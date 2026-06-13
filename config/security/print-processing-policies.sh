#!/usr/bin/env bash
# Decode signed processing-policy.json files and print RA expressions + policy graphs.
set -euo pipefail
source "$(dirname "$0")/_env.sh"
require_standalone_jar

POLICY_DIR="${DIFC_POLICY_REGISTRY_DIR:-/tmp/kafka-streams-examples/policy}"
LOG_OUT="${1:-}"

if [[ -n "${LOG_OUT}" ]]; then
  exec > >(tee "${LOG_OUT}")
fi

echo "=== Processing policy graphs and relational algebra ==="
echo "Policy registry: ${POLICY_DIR}"
java -cp "${STANDALONE_JAR}" io.confluent.examples.streams.microservices.PrintProcessingPolicies \
  -d "${POLICY_DIR}"

DIAGRAM_DIR="${LOG_OUT:-/tmp/ms-workflow-logs/policy-diagrams}"
if [[ -n "${LOG_OUT}" ]]; then
  DIAGRAM_DIR="$(dirname "${LOG_OUT}")/policy-diagrams"
fi
python3 "$(dirname "$0")/generate-policy-diagrams.py" "${POLICY_DIR}" "${DIAGRAM_DIR}"
echo "Diagrams written under ${DIAGRAM_DIR}"
