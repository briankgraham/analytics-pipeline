#!/bin/sh
# Creates the topics in topics.conf and converges their dynamic configs to exactly the declared
# set (adds or updates declared ones, deletes undeclared overrides), so it is safe to rerun.
# Partition counts are never changed in place: that remaps keys to partitions and invalidates the
# aggregator's per-partition state. A mismatch fails and asks for `docker compose down -v`.
# Runs inside the apache/kafka image (busybox sh, no jq, hence the plain line format).
set -eu

conf="$(dirname "$0")/topics.conf"
bootstrap="${KAFKA_BOOTSTRAP:-kafka:29092}"
bin=/opt/kafka/bin

fail() {
  echo "create-topics: $*" >&2
  exit 1
}

# The topic's dynamic (topic-level) configs as key=value lines.
dynamic_configs() {
  "$bin/kafka-configs.sh" --bootstrap-server "$bootstrap" --describe --entity-type topics \
    --entity-name "$1" </dev/null | sed -n 's/^[[:space:]]\{1,\}\([^ ]*=[^ ]*\) .*/\1/p'
}

# Kafka CLI calls take stdin from /dev/null so they cannot consume the topic list being read.
grep -v '^[[:space:]]*\(#\|$\)' "$conf" | while read -r name partitions configs; do
  create_args=""
  alter_args=""
  declared_keys=" "
  for kv in $configs; do
    key=${kv%%=*}
    value=${kv#*=}
    create_args="$create_args --config $kv"
    case $value in
      *,*) value="[$value]" ;; # kafka-configs needs brackets around list values
    esac
    alter_args="${alter_args:+$alter_args,}$key=$value"
    declared_keys="$declared_keys$key "
  done

  # shellcheck disable=SC2086 # create_args is a list of flags
  "$bin/kafka-topics.sh" --bootstrap-server "$bootstrap" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 $create_args </dev/null

  current=$("$bin/kafka-topics.sh" --bootstrap-server "$bootstrap" --describe --topic "$name" </dev/null |
    sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -n 1)
  case $current in
    '' | *[!0-9]*) fail "could not read the partition count of $name" ;;
  esac
  [ "$current" -eq "$partitions" ] ||
    fail "$name has $current partitions but declares $partitions; changing it remaps keys, so recreate the stack: docker compose down -v"

  "$bin/kafka-configs.sh" --bootstrap-server "$bootstrap" --alter --entity-type topics \
    --entity-name "$name" --add-config "$alter_args" </dev/null

  undeclared=""
  for kv in $(dynamic_configs "$name"); do
    key=${kv%%=*}
    case $declared_keys in
      *" $key "*) ;;
      *) undeclared="${undeclared:+$undeclared,}$key" ;;
    esac
  done
  if [ -n "$undeclared" ]; then
    echo "create-topics: $name: deleting undeclared configs $undeclared"
    "$bin/kafka-configs.sh" --bootstrap-server "$bootstrap" --alter --entity-type topics \
      --entity-name "$name" --delete-config "$undeclared" </dev/null
  fi
done
echo "create-topics: done"
