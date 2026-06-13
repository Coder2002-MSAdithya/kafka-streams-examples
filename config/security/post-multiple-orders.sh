#!/usr/bin/env bash
# POST several orders to OrdersService REST API, one after another.
# Order ids and unit prices are assigned by OrdersService.
# Usage: ./post-multiple-orders.sh [count]
set -euo pipefail
source "$(dirname "$0")/_env.sh"

ORDER_URL="${ORDER_SERVICE_URL:-http://localhost:5432}"
COUNT="${1:-13}"
TIMEOUT_MS="${ORDER_HTTP_TIMEOUT_MS:-60000}"

PRODUCTS=(JUMPERS UNDERPANTS STOCKINGS SOCKS SCARVES)

echo "Posting ${COUNT} order(s) to ${ORDER_URL}/v1/orders (server-assigned ids and prices)"

post_order() {
  local n="$1"
  local customer="$2"
  local product="$3"
  local quantity="$4"
  local payload
  payload=$(cat <<EOF
{"customerId":${customer},"product":"${product}","quantity":${quantity}}
EOF
)
  echo "--- POST order #${n} product=${product} customer=${customer} quantity=${quantity} ---"
  local headers_file="/tmp/post-order-${n}-headers.txt"
  local body_file="/tmp/post-order-${n}.json"
  local http_code
  http_code=$(curl -s -D "${headers_file}" -o "${body_file}" -w "%{http_code}" \
    -X POST "${ORDER_URL}/v1/orders?timeout=${TIMEOUT_MS}" \
    -H "Content-Type: application/json" \
    -d "${payload}" \
    --connect-timeout 10 \
    --max-time 120)
  local location
  location=$(grep -i '^Location:' "${headers_file}" 2>/dev/null | tail -1 | tr -d '\r' | awk '{print $2}' || true)
  echo "HTTP ${http_code} location=${location:-'(none)'} body=$(cat "${body_file}" 2>/dev/null || echo '(empty)')"
  if [[ "${http_code}" != "200" && "${http_code}" != "201" && "${http_code}" != "202" ]]; then
    echo "ERROR: unexpected HTTP status for order #${n}" >&2
    return 1
  fi
  sleep 2
}

for ((i = 1; i <= COUNT; i++)); do
  customer=$((i % 6))
  product="${PRODUCTS[$((i % ${#PRODUCTS[@]}))]}"
  quantity=$((1 + i % 3))
  post_order "${i}" "${customer}" "${product}" "${quantity}"
done

echo "Done posting ${COUNT} order(s)."
