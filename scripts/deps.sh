#!/bin/sh
# Run dependency-cruiser over whichever workspace roots exist (an empty workspace has none yet).
set -eu
roots=""
for dir in packages services apps tools bench; do
  [ -d "$dir" ] && roots="$roots $dir"
done
if [ -z "$roots" ]; then
  echo "deps: no workspace roots yet"
  exit 0
fi
# shellcheck disable=SC2086 # word splitting of $roots is intended
exec depcruise $roots --config .dependency-cruiser.cjs
