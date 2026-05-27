#!/usr/bin/env bash
# Interactive CLI: POST one order to OrdersService (same REST flow as PostOrdersAndPayments).
set -euo pipefail
source "$(dirname "$0")/_env.sh"
require_standalone_jar

ORDER_URL="${ORDER_SERVICE_URL:-http://localhost:5432}"
TIMEOUT_MS="${ORDER_HTTP_TIMEOUT_MS:-60000}"

exec java -cp "${STANDALONE_JAR}" io.confluent.examples.streams.microservices.PostOrderInteractive \
  -o "${ORDER_URL}" \
  -t "${TIMEOUT_MS}" \
  "$@"
