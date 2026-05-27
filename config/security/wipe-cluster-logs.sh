#!/usr/bin/env bash
# Remove all KRaft log directories for this demo cluster (destructive).

set -euo pipefail
source "$(dirname "$0")/_env.sh"

DIRS=(
  /tmp/kafka-logs-ms-sasl-controller
  /tmp/kafka-logs-ms-sasl-broker-1
  /tmp/kafka-logs-ms-sasl-broker-2
  /tmp/kafka-logs-microservices-sasl
)

for d in "${DIRS[@]}"; do
  if [[ -d "${d}" ]]; then
    echo "Removing ${d}"
    rm -rf "${d}"
  fi
done
rm -f "${SCRIPT_DIR}/.cluster-id"
echo "Done. Run ./format-kraft-storage.sh before starting again."
