#!/usr/bin/env bash
# Summarize a kafka-streams-examples DIFC workflow run for comparison tables.
set -euo pipefail
LOG_ROOT="${1:?usage: analyze-difc-run.sh LOG_ROOT MODE_LABEL}"
MODE="${2:-unknown}"
LABEL="${3:-$LOG_ROOT}"

count_logs() {
  local pattern="$1"
  shift
  grep -hE "${pattern}" "$@" 2>/dev/null | wc -l | tr -d '[:space:]'
}

unique_logs() {
  local pattern="$1"
  shift
  grep -hE "${pattern}" "$@" 2>/dev/null | sort -u | wc -l | tr -d '[:space:]'
}

last_match() {
  local pattern="$1"
  shift
  grep -hE "${pattern}" "$@" 2>/dev/null | tail -1 || echo "(none)"
}

svc_logs=(
  "${LOG_ROOT}/orders-service.log"
  "${LOG_ROOT}/fraud-service.log"
  "${LOG_ROOT}/inventory-service.log"
  "${LOG_ROOT}/order-details-service.log"
  "${LOG_ROOT}/email-service.log"
  "${LOG_ROOT}/validations-aggregator.log"
)

REQ_BYTES=$(count_logs 'requestGrantCap.*attestedPolicyBytes=[1-9]' "${svc_logs[@]}")
REQ_TOTAL=$(count_logs 'requestGrantCap' "${svc_logs[@]}")
GRANTS=$(unique_logs 'grantPrivilege|addClientPrivs.*errorCode=0' "${LOG_ROOT}/orders-service.log")
VERIFIED=$(count_logs 'grantPolicyVerified|Attested-policy \(RA/taint\) check passed' "${LOG_ROOT}/orders-service.log")
DENIED=$(count_logs 'grantDenied|Attested-policy \(RA/taint\) check failed' "${LOG_ROOT}/orders-service.log")
LINEAGE=$(count_logs 'grantLineageVerify' "${LOG_ROOT}/orders-service.log")
EXTERNAL=$(count_logs 'grantExternalVerified' "${LOG_ROOT}/orders-service.log")
VALIDATED=$(count_logs 'Aggregation decision.*state=VALIDATED' "${LOG_ROOT}/validations-aggregator.log")
ORDERS_POSTED=$(grep -c 'HTTP 201' "${LOG_ROOT}/post-orders.log" 2>/dev/null || echo 0)
BROKER_COUNT="${KSE_BROKER_COUNT:-3}"
BROKER_TAGS=$(last_match 'Final message tags' "${LOG_ROOT}/broker-1.log" "${LOG_ROOT}/broker-2.log")
WORKFLOW_OK="no"
if grep -q 'Workflow run complete' "${LOG_ROOT}/difc-workflow.log" 2>/dev/null \
  || grep -q 'Workflow run complete' "${LOG_ROOT}/../difc-workflow.log" 2>/dev/null; then
  WORKFLOW_OK="yes"
fi
if [[ -f "${LOG_ROOT}/difc-workflow.log" ]] && grep -q 'Done\.' "${LOG_ROOT}/difc-workflow.log"; then
  WORKFLOW_OK="yes"
fi

printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
  "${MODE}" \
  "${LABEL}" \
  "${WORKFLOW_OK}" \
  "${BROKER_COUNT}" \
  "${REQ_BYTES}/${REQ_TOTAL}" \
  "${GRANTS}" \
  "${VERIFIED}" \
  "${DENIED}" \
  "${LINEAGE}" \
  "${EXTERNAL}" \
  "${VALIDATED}" \
  "${ORDERS_POSTED}" \
  "${BROKER_TAGS}"
