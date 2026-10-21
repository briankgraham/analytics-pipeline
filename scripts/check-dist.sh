#!/bin/sh
# Imports every built workspace package with plain Node, the way services run in production.
# Vitest resolves src/ through @obs/source with Vite's CommonJS interop, which hides ESM import
# errors (for example a named import that a CommonJS dependency does not expose); this does not.
set -eu
found=0
for manifest in packages/*/package.json services/*/package.json; do
  [ -f "$manifest" ] || continue
  dir=$(dirname "$manifest")
  grep -q '"default": "./dist/index.js"' "$manifest" || continue
  found=$((found + 1))
  name=$(node -p "require('./$manifest').name")
  (cd "$dir" && node --input-type=module -e "await import('$name')") || {
    echo "check:dist: $name does not import under plain Node" >&2
    exit 1
  }
done
echo "check:dist: $found package(s) import under plain Node"
