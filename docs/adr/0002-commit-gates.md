# ADR-0002: Commit gates (verify gate, husky, Claude Code hook, CI)

- Status: accepted
- Date: 2026-10-20
- Decision log: 2026-10-20, "Commit gates"

## Context
CLAUDE.md rule 1 (never commit unless `pnpm verify` passes), rule 3 (fresh-context review, summarized in a `Review:` line), and the "never commit to main" rule must hold for every author, human or Claude, and must not depend on memory. Plain git hooks are easy to skip: `--no-verify`, `HUSKY=0`, a `core.hooksPath` override, or a fresh worktree where `pnpm install` never ran the husky `prepare` script.

## Decision
- **One gate script**, `scripts/verify-gate.sh`, used by every local entry point. It:
  - refuses commits on `main`;
  - refuses unless the working tree equals the index (no unstaged or untracked files), so `pnpm verify` checks exactly what is committed;
  - runs `pnpm verify` and records the verified tree (`git write-tree`) in a per-worktree stamp (`$(git rev-parse --git-path verify-stamp)`). The same tree is never verified twice, even though two hooks fire on one commit.
- **husky:**
  - `pre-commit` runs the gate.
  - `commit-msg` runs `checkCommitMessage`: a Conventional Commits header (≤ 100 chars), a blank line before the body, and a `Review:` line. Only a real merge (git's `MERGE_HEAD` exists) is exempt. A merge-looking header is not, and reverts use the `revert:` type.
- **Claude Code PreToolUse hook** (`.claude/settings.json` → `.claude/hooks/commit-gate.sh`), an **allowlist**:
  - Any Bash command that mentions both `git` and `commit`, after quotes and backslashes are stripped, must be exactly `git commit -F <plain path>`, optionally with `--amend` or `-q` (no `--no-edit`, so the recorded message is always the file the hook checked). It must be its own command.
  - Everything else is denied: bypass flags in any spelling (including abbreviated `--no-veri`), env prefixes, `-c`/`-C`/`--git-dir`, aliases, `-a`/`--all`/pathspecs, chained commands, and `-m`. That is the only way the hook can know the commit lands in the directory it verified, with exactly the tree it verified.
  - For an allowed command, the hook checks the message file with the same `checkCommitMessage`, then runs the gate. For a commit made this way it enforces everything husky does, so a worktree without husky shims still gates it.
  - It fails closed: unreadable input blocks, and the wrapper maps any non-zero status (missing `node`, a crash) to exit 2. `onFailure: block` is also set; a crash during development confirmed that it blocks.
  - Known gaps, with CI (verify plus every PR commit message) as the backstop:
    - Commands that create commits without the word `commit` are not gated by this hook: `git revert`, `cherry-pick`, `merge`, `rebase`, `am`, and aliases from the user's own git config (for example `ci = commit -n`).
    - Where husky is installed, `revert`, `cherry-pick`, and `merge` still run its commit-msg hook; `rebase` and `am` replay existing messages.
    - Committing on `main` this way is caught only by branch protection.
- **The commit-msg CLI logic is tested:** argument parsing and merge detection live in `commit-msg-cli.ts`. The `.main.ts` entry files only wire Node APIs.
- **Hook code runs from source:** `tools/hooks` runs under Node's type stripping (`erasableSyntaxOnly`, explicit `.ts` imports with `rewriteRelativeImportExtensions`), so the gates need no build step.
- **CI** (`.github/workflows/ci.yml`) runs `pnpm verify` and checks every non-merge commit message in a PR. That catches hook bypasses from any machine.
- **`main` is protected:** PRs only, the `verify` and `commit-messages` checks required, no force pushes. This is applied with `gh api` once the first CI run has registered the check names. Until then it is pending, and the local main-branch refusal is the only guard.

## Alternatives considered
- **lint-staged with stash/unstash, instead of requiring a clean tree:** it verifies only part of the tree, and stash juggling has lost work before. Requiring a full `git add` first is simpler and exact.
- **commitlint:** a larger dependency surface for two rules, and a custom `Review:` rule would need a plugin anyway.
- **Only CI:** feedback comes too late, and the "never on main" rule needs a local guard.
- **The Claude hook only blocks bypass flags and relies on husky:** fails open in a worktree where husky was never installed.

## Consequences
- Commit in two steps: stage everything, then commit with `-F <file>` in a separate command. Any chained form is refused. Text that mentions git and commit (such as heredocs) also trips the hook, so write such text with an editor tool, not through Bash.
- pnpm 12 skips the root `prepare` script on an already-up-to-date install. Run `pnpm install --force` (or `pnpm prepare`) once in an existing checkout to install the husky shims; a fresh clone gets them automatically.
- Stryker and Playwright CI jobs arrive with the first domain package (Phase 3) and the UI (Phase 2).
