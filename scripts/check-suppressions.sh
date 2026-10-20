#!/bin/sh
# Rule 4: coverage and type-check suppressions need user approval. ESLint already ignores inline
# config (noInlineConfig), so this catches what ESLint cannot see. Approved exceptions are listed
# here with a one-line reason; there are none yet.
set -eu
pattern='(v8|c8|istanbul)[[:space:]]+ignore|Stryker[[:space:]]+disable|@ts-(ignore|expect-error|nocheck)'
# --untracked: new files must not slip through before they are staged.
set +e
git grep --untracked -nIE "$pattern" -- . ':!*.md' ':!scripts/check-suppressions.sh'
rc=$?
set -e
case $rc in
  1) echo "check:suppressions: none found" ;;
  0)
    echo "check:suppressions: suppression comments need user approval (CLAUDE.md rule 4)" >&2
    exit 1
    ;;
  *)
    echo "check:suppressions: git grep failed (exit $rc); refusing to report success" >&2
    exit 1
    ;;
esac
