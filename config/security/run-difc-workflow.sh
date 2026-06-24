#!/usr/bin/env bash
# DIFC end-to-end workflow for kafka-streams-examples microservices demo.
set -euo pipefail
source "$(dirname "$0")/_env.sh"
source "$(dirname "$0")/_policy_agent.sh"

LOG_ROOT="${LOG_ROOT:-/tmp/kafka-streams-examples-difc-logs}"
export LOG_ROOT
export STATE_DIR
echo "Sanitization modes: agent=${DIFC_AGENT_SANITIZATION_MODE:-both}, grantor=${DIFC_GRANT_SANITIZATION_MODE:-lineage}"
mkdir -p "${LOG_ROOT}"
mkdir -p "${STATE_DIR}/policy"
rm -rf "${STATE_DIR}/policy"/*
: > "${LOG_ROOT}/pids.txt"

echo "=== Build policy-agent + standalone JAR ==="
if [[ ! -f "$(policy_agent_jar)" ]]; then
  (cd "${KAFKA_HOME}" && ./gradlew :security:policy-agent:jar -x spotbugsMain -q)
fi
require_standalone_jar
if [[ ! -f "${STANDALONE_JAR}" ]]; then
  (cd "${EXAMPLES_HOME}" && mvn -q -DskipTests package)
fi

echo "=== Run full microservices workflow (cluster + DIFC grants + orders) ==="
"$(dirname "$0")/run-workflow-from-scratch.sh" > >(tee "${LOG_ROOT}/difc-workflow.log") 2>&1

echo
echo "=== DIFC grant summary (orders grantor) ==="
grep -E '\[DIFC\] (grantPrivilege|grantDenied|grantPolicyVerified|grantExternalVerified|grantExternalCheckSkipped|createTag)' \
  "${LOG_ROOT}/orders-service.log" 2>/dev/null | tail -30 || echo "(no grant lines on orders-service)"

echo
echo "=== Expression lineage verification (grantor) ==="
grep -E '\[DIFC\] grantLineageVerify' "${LOG_ROOT}/orders-service.log" 2>/dev/null | tail -40 \
  || echo "(no grantLineageVerify lines — rebuild standalone JAR if missing)"

echo
echo "=== DIFC requester summary (validators + aggregator) ==="
grep -E '\[DIFC\] (requestGrantCap|addTag|attestedPolicyReady|registerClient|DIFC grants ready)' \
  "${LOG_ROOT}/fraud-service.log" \
  "${LOG_ROOT}/inventory-service.log" \
  "${LOG_ROOT}/order-details-service.log" \
  "${LOG_ROOT}/email-service.log" \
  "${LOG_ROOT}/validations-aggregator.log" 2>/dev/null | tail -40 || true

echo
echo "=== External connection attestation (policy files) ==="
for f in "${STATE_DIR}/policy"/*/*.json; do
  [[ -f "${f}" ]] || continue
  principal="$(basename "$(dirname "${f}")")"
  ext="$(grep -o '"externalConnections"[^]]*]' "${f}" 2>/dev/null | head -1 || true)"
  echo "${principal}: ${ext:-no externalConnections field}"
done

echo
echo "=== Pipeline data flow ==="
grep -E 'Aggregation decision|VALIDATED|FAILED|Order key=' \
  "${LOG_ROOT}/"*.log 2>/dev/null | tail -25 || true

echo
echo "=== Processing policy graphs and RA diagrams ==="
POLICY_DIAGRAM_DIR="${LOG_ROOT}/policy-diagrams"
"$(dirname "$0")/print-processing-policies.sh" "${LOG_ROOT}/processing-policies.log" >/dev/null
mkdir -p "${EXAMPLES_HOME}/docs/difc-ra-diagrams"
cp -a "${POLICY_DIAGRAM_DIR}"/*.{png,dot} "${EXAMPLES_HOME}/docs/difc-ra-diagrams/" 2>/dev/null || true
echo "RA diagrams: ${POLICY_DIAGRAM_DIR} (also copied to ${EXAMPLES_HOME}/docs/difc-ra-diagrams/)"

echo
echo "Logs: ${LOG_ROOT}"
echo "Policies: ${STATE_DIR}/policy"
echo "Done."
