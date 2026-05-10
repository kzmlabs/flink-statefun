#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Quickstart compose smoke test:
#   - Produces alice:{"name":"Alice"} to greeter.commands
#   - Polls greeter.results for "Hello, Alice!" with a 60s ceiling
#   - Exits 0 on match, 1 on timeout
#
# Assumes the quickstart stack is already up (docker compose up -d --wait)
# and that kafka is reachable as the "quickstart-kafka" container.

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-quickstart-kafka}"
PRODUCE_TOPIC="${PRODUCE_TOPIC:-greeter.commands}"
CONSUME_TOPIC="${CONSUME_TOPIC:-greeter.results}"
EXPECTED='Hello, Alice!'
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"

echo "==> Producing alice:{\"name\":\"Alice\"} to ${PRODUCE_TOPIC}"
echo 'alice:{"name":"Alice"}' | docker exec -i "${KAFKA_CONTAINER}" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic "${PRODUCE_TOPIC}" \
  --property "parse.key=true" --property "key.separator=:"

echo "==> Polling ${CONSUME_TOPIC} for '${EXPECTED}' (up to ${TIMEOUT_SECONDS}s)"
deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
while [ "$(date +%s)" -lt "${deadline}" ]; do
  output=$(docker exec "${KAFKA_CONTAINER}" \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic "${CONSUME_TOPIC}" \
    --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null || true)
  if printf '%s' "${output}" | grep -F -q "${EXPECTED}"; then
    echo "==> Received: ${output}"
    echo "PASS: round-trip completed."
    exit 0
  fi
  sleep 2
done

echo "FAIL: did not receive '${EXPECTED}' within ${TIMEOUT_SECONDS}s." >&2
exit 1
