#!/bin/sh
# Claude Code PreToolUse (Bash) hook: gate commits (CLAUDE.md rules 1, 3, 8; ADR-0002).
# Cheap pre-filter so ordinary Bash calls never start Node; the TS side decides the rest.
input=$(cat)
case "$input" in
  *commit*) ;;
  *) exit 0 ;;
esac
printf '%s' "$input" | node "$(dirname "$0")/../../tools/hooks/src/cli/claude-commit-gate.main.ts"
status=$?
# Only exit 2 blocks in Claude Code; fail closed on anything but an explicit allow (node missing,
# a crash, a signal).
[ "$status" -eq 0 ] && exit 0
exit 2
