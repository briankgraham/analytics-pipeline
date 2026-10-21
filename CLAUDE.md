# Analytics pipeline

A small version of Datadog or Sentry, built to learn how streaming systems work. Apps send logs to an
API, Kafka holds them, a worker saves them to Elasticsearch, and you can search them. What we're
building and in what order is in `docs/design.md`.

## Rules for every session

1. **Never commit to `main`.** Work on a branch, open a PR, and the user decides when to merge.
2. **One PR per session, with one commit.** Plan it, build it, review it, commit, open the PR, then stop.
3. **Keep each PR small:** about 500 lines at most.
4. **Write tests alongside the code** for any logic that matters. Plain wiring needs no test.
5. **Get a fresh review before each commit.** A reviewer that didn't write the code (a subagent or
   `/code-review`) checks the diff. Fix what it finds. If a finding seems wrong, tell the user why
   instead of skipping it.
6. **Walk the user through every file** in the PR, in plain words, before opening it.
7. **Use plain names** for files, folders, and functions. A newcomer should be able to guess what's
   inside from the name.
8. **Build only what the current PR uses.** Folders and helpers appear when something needs them.
9. **Commit titles follow Conventional Commits** (`feat: ...`, `fix: ...`, `docs: ...`). CI checks this.
10. **`pnpm verify` must pass** before committing: formatting, type checking, and tests.
11. **Keep `README.md` accurate** whenever how-to-run steps change.

These rules are kept light on purpose. Stricter checks (coverage targets, linting, git hooks) get
added back one at a time, and only when the user agrees.

## Tools

TypeScript (strict) on Node 24 (`.nvmrc`), pnpm through `corepack`, Prettier, Vitest. CI is
`.github/workflows/ci.yml`. `main` only accepts PRs whose checks named `verify` and `commit-messages`
pass, so keep those job names.

## Folder plan

Folders are added by the PR that first needs them.

```
api/              HTTP server: receives logs, answers searches
worker/           reads logs from Kafka and saves them to Elasticsearch
scripts/          small helper scripts, such as sending a test log
shared/log.ts     the shape of a log, used by both the API and the worker
docker-compose.yml  runs Kafka and Elasticsearch locally
docs/design.md    what we're building, short
```

## Earlier work

A first, much larger attempt is saved at git tag `archive/phase-1-v1`. Treat it as a parts bin to
copy from when a piece is needed, not as a plan to follow.
