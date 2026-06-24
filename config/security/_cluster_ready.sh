#!/usr/bin/env bash
# Shared helpers: wait until a 3-broker KRaft cluster can serve consumer groups.

wait_for_registered_brokers() {
  local expected="${1:-${KSE_BROKER_COUNT:-3}}"
  echo "Waiting for ${expected} brokers to accept client connections..."
  for ((n = 1; n <= 90; n++)); do
    local count=0
    for port in 9092 9094 9096; do
      if "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" \
          --bootstrap-server "localhost:${port}" \
          --command-config "${ADMIN_CONFIG}" >/dev/null 2>&1; then
        count=$((count + 1))
      fi
    done
    if [[ "${count}" -ge "${expected}" ]]; then
      echo "${count}/${expected} brokers reachable"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: only reached ${count:-0}/${expected} brokers after 180s" >&2
  return 1
}

wait_for_group_coordinator() {
  echo "Waiting for consumer group coordinator (__consumer_offsets, RF=3 on ${KSE_BROKER_COUNT:-3} brokers)..."
  for ((n = 1; n <= 120; n++)); do
    # Prime metadata for the internal offsets topic.
    "${KAFKA_HOME}/bin/kafka-consumer-groups.sh" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      --command-config "${ADMIN_CONFIG}" \
      --list >/dev/null 2>&1 || true

    local describe leaders_none
    describe=$("${KAFKA_HOME}/bin/kafka-topics.sh" \
      --bootstrap-server "${BOOTSTRAP_SERVERS}" \
      --command-config "${ADMIN_CONFIG}" \
      --describe --topic __consumer_offsets 2>/dev/null) || true

    if [[ -n "${describe}" ]]; then
      leaders_none=$(echo "${describe}" | grep -cE 'Leader: (none|-1)' || true)
      if [[ "${leaders_none}" -eq 0 ]] \
        && "${KAFKA_HOME}/bin/kafka-consumer-groups.sh" \
          --bootstrap-server "${BOOTSTRAP_SERVERS}" \
          --command-config "${ADMIN_CONFIG}" \
          --list >/dev/null 2>&1; then
        echo "Group coordinator ready (attempt ${n})"
        return 0
      fi
    fi
    sleep 2
  done
  echo "ERROR: group coordinator not ready after 240s" >&2
  "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --command-config "${ADMIN_CONFIG}" \
    --describe --topic __consumer_offsets 2>&1 | tail -20 >&2 || true
  return 1
}

wait_for_cluster_ready() {
  wait_for_registered_brokers "${1:-${KSE_BROKER_COUNT:-3}}"
  wait_for_group_coordinator
}
