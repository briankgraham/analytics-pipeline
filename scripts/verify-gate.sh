#!/bin/sh
# The one commit gate (CLAUDE.md rule 1), shared by the husky pre-commit hook and the Claude Code
# PreToolUse hook. Run from anywhere inside the repo.
#   1. never commit on main (changes reach main by PR only)
#   2. the working tree must equal the index, so `pnpm verify` checks exactly what is committed
#   3. skip if this exact tree already passed (both hooks fire on one commit)
#   4. otherwise run `pnpm verify` and record the tree on success
set -eu

fail() {
  echo "verify-gate: $*" >&2
  exit 1
}

cd "$(git rev-parse --show-toplevel)"

branch=$(git symbolic-ref --short -q HEAD || true)
[ "$branch" = "main" ] && fail "refusing to commit on main; use a branch and open a PR"

git diff --quiet || fail "unstaged changes; stage everything (git add) before committing so verify checks what is committed"
untracked=$(git ls-files --others --exclude-standard)
[ -z "$untracked" ] || fail "untracked files; stage or ignore them first:
$untracked"

tree=$(git write-tree)
stamp=$(git rev-parse --git-path verify-stamp)
if [ -f "$stamp" ] && [ "$(cat "$stamp")" = "$tree" ]; then
  echo "verify-gate: tree $tree already verified"
  exit 0
fi

# The machine default Node may differ from the pinned one; prefer .nvmrc via nvm when available.
if [ -f .nvmrc ] && [ "$(node -v 2>/dev/null || true)" != "v$(cat .nvmrc)" ]; then
  nvm_sh="${NVM_DIR:-$HOME/.nvm}/nvm.sh"
  # shellcheck disable=SC1090 # nvm's location is user-specific
  [ -s "$nvm_sh" ] && . "$nvm_sh" && nvm use >/dev/null
  [ "$(node -v 2>/dev/null || true)" = "v$(cat .nvmrc)" ] || fail "Node $(cat .nvmrc) required (see .nvmrc)"
fi

rm -f "$stamp"
pnpm verify
echo "$tree" > "$stamp"
