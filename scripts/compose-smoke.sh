#!/bin/sh
# Brings up the local stack and checks it matches the declared topology:
#   - Kafka and Elasticsearch healthy, topic init exited 0
#   - every topic in infra/kafka/topics.conf exists with its partition count, and its dynamic
#     configs are exactly the declared set (nothing missing, nothing extra)
#   - the host listener works: the advertised 127.0.0.1:9092 answers, and the port is mapped
#   - rerunning the topic init converges (idempotent)
# Usage: scripts/compose-smoke.sh [--down]   (--down removes the stack and its volumes afterwards)
set -eu

cd "$(git rev-parse --show-toplevel)"
fail() {
  echo "compose-smoke: FAIL: $*" >&2
  exit 1
}

if [ "${1:-}" = "--down" ]; then
  trap 'docker compose down -v >/dev/null 2>&1 || true' EXIT
fi

docker compose up -d --wait --wait-timeout 300 kafka elasticsearch
docker compose up kafka-init --exit-code-from kafka-init >/dev/null ||
  fail "kafka-init exited non-zero (docker compose logs kafka-init)"

# stdin from /dev/null: inside the `while read` loop below, exec would otherwise swallow the
# remaining topic lines and the loop would silently check only the first topic.
kafka() {
  docker compose exec -T kafka /opt/kafka/bin/"$@" </dev/null
}

# Sorted key=value lines, one per config, for an exact comparison.
sorted_lines() {
  tr -s ' \t' '\n' | sed '/^$/d' | sort
}

topics=$(grep -v '^[[:space:]]*\(#\|$\)' infra/kafka/topics.conf)
expected=$(printf '%s\n' "$topics" | wc -l | tr -d ' ')
checked=0
while read -r name partitions configs; do
  described=$(kafka kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic "$name") ||
    fail "topic $name missing"
  actual=$(printf '%s\n' "$described" | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -n 1)
  [ "$actual" = "$partitions" ] || fail "$name has '$actual' partitions, expected $partitions"

  dynamic=$(kafka kafka-configs.sh --bootstrap-server kafka:29092 --describe \
    --entity-type topics --entity-name "$name" |
    sed -n 's/^[[:space:]]\{1,\}\([^ ]*=[^ ]*\) .*/\1/p' | sorted_lines)
  declared=$(printf '%s' "$configs" | sorted_lines)
  [ "$dynamic" = "$declared" ] ||
    fail "$name configs differ from topics.conf
  declared: $(echo $declared)
  actual:   $(echo $dynamic)"
  echo "compose-smoke: ok   $name ($partitions partitions; $(echo $declared))"
  checked=$((checked + 1))
done <<TOPICS
$topics
TOPICS
[ "$checked" -eq "$expected" ] || fail "checked $checked of $expected topics"

# Host listener: the advertised address answers (127.0.0.1:9092 is the broker itself inside its
# container), and the host side of the port mapping accepts connections.
kafka kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:9092 >/dev/null ||
  fail "host listener 127.0.0.1:9092 does not answer"
nc -z -w 5 127.0.0.1 9092 || fail "port 9092 is not reachable from the host"
echo "compose-smoke: ok   host listener 127.0.0.1:9092"

status=$(curl -fsS 'http://127.0.0.1:9200/_cluster/health' | sed -n 's/.*"status":"\([a-z]*\)".*/\1/p')
case $status in
  green | yellow) echo "compose-smoke: ok   elasticsearch ($status)" ;;
  *) fail "elasticsearch health is '$status'" ;;
esac

docker compose run --rm kafka-init >/dev/null || fail "rerunning kafka-init did not converge"
echo "compose-smoke: ok   topic init is idempotent"
echo "compose-smoke: passed"
