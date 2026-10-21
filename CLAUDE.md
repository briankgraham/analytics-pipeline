# Observability platform (real-time analytics pipeline)

See `../CLAUDE.md` for how we work across projects. Full design: `docs/design.md`; decisions and rationale: the Decision log below and `docs/adr/`. If they disagree, this file wins.

## Non-negotiable rules (every session, no exceptions)
1. Never commit unless `pnpm verify` passes (typecheck, lint, format, dependency rules, tests, 100% coverage on lines/branches/functions/statements, build, plain-Node import of each built package). Enforced by `scripts/verify-gate.sh` (husky pre-commit and a Claude Code PreToolUse hook), a `commit-msg` check, and CI (ADR-0002). Never commit on `main`; changes reach it by PR. Stage everything first, then commit with `-F <file>` in a separate command; the Claude hook gates any Bash command that mentions a commit, so write such text with an editor tool, not a heredoc.
2. Tests are written with the code, never after. Domain logic is test-first (red, green, refactor).
3. Before every commit or PR, review the diff against `CODE_REVIEW.md` using a **fresh-context reviewer** (subagent or `/code-review`), not the author's own context. Fix **all** blocker, major, and minor findings; fix nits if cheap. Stopping rule (details in `CODE_REVIEW.md`): pass 1 reviews the full diff, passes 2 and 3 review only the fix delta, and there is no pass 4; after pass 3, remaining blockers/majors are fixed with a targeted test or escalated to the user, and minors/nits go to `docs/follow-ups.md`. If I believe a finding is wrong, do **not** skip it silently: tell the user with reasoning and let them decide. Put the review summary (found / fixed / flagged) in the commit body as a `Review:` line.
4. No new coverage exclusions, `eslint-disable`, `@ts-ignore`, or `any` without explicit user approval and a one-line reason comment. The complete list of allowed coverage exclusions is in `docs/design.md` (Testing standard); adding to it needs approval.
5. Domain classes never import Kafka or Elasticsearch clients. Services never import other services; shared code lives in `packages/*`.
6. Any decision affecting more than one service gets an ADR in `docs/adr/` and a dated line in the Decision log below.
7. Keep `README.md` accurate whenever behavior or run instructions change.
8. Small, meaningful commits (Conventional Commits) inside this repo only.
9. One commit per session: plan, implement, verify, review (stopping rule above), commit, update Status and the handoff below, then end. The next commit starts in a fresh session. Plan each commit small enough for one full review pass; split it during planning when it is not.

## Product
A mini Datadog/Sentry: apps send logs/events to an ingest API; Kafka buffers them; stream processors compute windowed aggregates (counts, error rates, p50/p95/p99) and alerts; Elasticsearch serves search and aggregates; a React dashboard shows it live.

Interview talking points we build for real: event time vs processing time, watermarks and late-event correction, idempotent sinks / what "exactly-once" really means, partitioning and hot keys, backpressure, DLQ, mergeable percentile sketches, ES mapping design.

## Stack
TypeScript (strict) on Node LTS, Koa (HTTP), React + Vite (UI), pnpm workspaces monorepo, Docker Compose, Kafka (KRaft), Elasticsearch, Vitest, fast-check, Testcontainers, Playwright, Stryker (domain packages), pino, zod.

## Layout (target)
```
services/{ingest-api,indexer,aggregator,alert-evaluator,query-api,log-generator}/
apps/web/
packages/{schemas,kafka,config,logger}/
docs/adr/
```

## Code shape
Template-method base classes for lifecycle and plumbing (`BaseConsumer`, `BatchingProducer`, one inheritance level only). Pure, Kafka-free domain classes (`WindowAssigner`, `WatermarkTracker`, `WindowStore`, `LatencySketch`). Interfaces at every I/O boundary (`EventSink`, `AggregateStore`, `CheckpointStore`) with a real impl and an in-memory fake. Manual constructor DI, wired in each service's `main.ts`. Koa: `createApp(deps)` factory, thin routers, logic in service classes, zod at every boundary.

## Key design points (details in `docs/design.md`)
- Partition key `{tenantId}:{service}` (locality; no salting on the shared topic); aggregator state and checkpoints are partitioned like the input. Event docs use `_id = tenantId:eventId`.
- Aggregator order: emit, flush and await acks, checkpoint, commit. Checkpoint = one fenced ES doc per input partition (offset + watermark + window state + `rejectedCount` + seen keys). Restart restores, re-emits open windows, and seeks.
- Per-partition watermarks only: `min(max event ts, max receivedAt) - 30 s`, monotonic, persisted. Caught-up quiet partitions advance on the injected clock (the one documented determinism limit); stalled or lagging partitions never do. DDSketch for percentiles.
- Aggregates go to compacted Kafka topic `aggregates`; indexer sinks to ES, alert-evaluator consumes it, and query-api serves the last 15 min (1m resolution) from a bounded in-process map plus SSE (no dual write). One aggregate record per (tenant, service, env, window) with counts by level; closed windows are immutable and alerts evaluate closed windows only. ES writes use external versioning with `revision = (count + rejectedCount)*2 + closed`; a 409 means the stored doc is at least as complete.
- Raw events: `events-YYYY.MM.DD` routed by event-time day, `op_type: create`, 409 = success. `attrs` is `flattened`.
- Ingest `202` means durably in Kafka. Event `id` (ULID) is the idempotency key. Reject `ts` > 30 s in the future (never clamp).

## Toolchain notes
- Node 24.22.0 LTS (`.nvmrc`, `engines`, Docker `node:24.22.0-*`), pnpm 12.14.0 via corepack, TypeScript 6.0.3 (capped by typescript-eslint; not TS 7), ESM only, `@obs/source` export condition for dev resolution (ADR-0001). Shell commands need `source ~/.nvm/nvm.sh && nvm use` first; the machine default is Node 25. pnpm settings go in `pnpm-workspace.yaml`, not `.npmrc`.
- Kafka client: `@confluentinc/kafka-javascript` 1.11.0 behind our own port in `packages/kafka` (benchmarked and decided, ADR-0003): KafkaJS-compatible producer, native consumer (the only API with group-less `assign`, the one capability kafkajs lacks, and a pull-style `consume(n)`); producers set `partitioner: 'murmur2_random'` explicitly; ingest's producer uses `linger.ms=0`. pnpm `allowBuilds` lets it fetch its native binary (supply-chain notes in the ADR).

## Related projects
- `change-data-capture-pipeline`: shares Kafka infra (compose, topic conventions, `packages/kafka`).
- `real-time-collaborative-editor-or-live-leaderboard`: same SSE/WebSocket push pattern.
- `rate-limiter-or-api-gateway-service`: ingest uses a simple in-memory token bucket; the distributed version lives there.
- `llm-request-gateway`: can ship its logs here later.

## Status
Design settled and committed 2026-10-20 (`babb40f`, after 12 fresh-context review passes). **Phase 1 in progress on branch `phase-1/skeleton`** (never commit to `main`; PR only): done: (a) toolchain and `pnpm verify`, (b) enforcement, (c) `@obs/schemas`, `@obs/config`, `@obs/logger`, (d) compose (`pnpm compose:smoke`), Kafka client benchmark (ADR-0003), `@obs/kafka` ports + confluent adapter + in-memory fake pinned by one contract suite (real broker via Testcontainers); next: the deferred `commit()` confirmation fix (top of `docs/follow-ups.md`), then `BatchingProducer`, then `BaseConsumer`, then ADR backfill. Background, rejected alternatives, deploy/domain notes, and process lessons are in `docs/notes.md`.

### Starting a fresh session (handoff)
1. Read this file, `docs/design.md`, and `CODE_REVIEW.md`. Open in plan mode: the user wants thorough planning, not short plans.
2. Phase 1 proposal, in order, one commit each (each reviewed fresh-context before commit):
   - a. Toolchain: pin Node LTS (`.nvmrc`, `engines`, Docker base), `corepack enable` for pnpm, pnpm workspace skeleton, strict `tsconfig`, ESLint (typescript-eslint strict-type-checked), Prettier, Vitest with the 100% coverage gate, dependency-cruiser, and the single `pnpm verify` entry point (typecheck, lint, format, dependency rules, tests, coverage, build). This commit is not docs-only: it must itself pass `pnpm verify`, including the empty case (Vitest `passWithNoTests` or equivalent).
   - b. Enforcement: husky pre-commit, `commit-msg` check for the `Review:` line, Claude Code PreToolUse hook blocking `git commit` unless `pnpm verify` passes (use the `update-config` skill), GitHub Actions CI, PR template.
   - c. `packages/schemas`, `config`, `logger`, then `packages/kafka` after the client benchmark (`@confluentinc/kafka-javascript` vs `kafkajs`: maintenance status, throughput, API fit; record the result in an ADR).
   - d. `docker-compose.yml` with Kafka (KRaft) and Elasticsearch, healthchecks, topic creation (see the Topic settings paragraph in `docs/design.md`: `events.raw` 12 partitions, 1 h retention locally; `aggregates` 12 partitions, compact+delete, 6 h; `events.rejected` 12 partitions with explicit partition = input partition; `events.late`, `events.dlq`, `alerts` 1 partition each; side topics and `alerts` 7 days).
   - e. Backfill ADRs for the cross-service decisions in the log below (rule 6).
3. Domain code is test-first (red, green, refactor), starting with the pure classes (`WindowAssigner`, `WatermarkTracker`, `WindowStore`, `LatencySketch` using DDSketch) in Phase 3, not before the Phase 1 gates exist.
4. When scripting multi-file doc edits, assert each replacement matches once, save every file, and grep afterwards (see `docs/notes.md`).

Phases: 1 skeleton (workspace, tooling, CI, compose, shared packages, ADRs, Kafka client benchmark); 2 ingest, indexer, generator, search; 3 aggregator + SSE dashboard; 4 alerts, DLQ, rate limiting; 5 chaos scripts, measured numbers, README; 6 out of scope for now (revisit after Phase 5).

## Decision log
- 2026-10-20: Product = observability platform (idea #1 of the analytics shortlist). Showcases windows, watermarks, and ES search with a natural load generator.
- 2026-10-20: TypeScript strict across all services and the UI. Shared zod schemas give compile-time contracts, which makes reviews meaningful.
- 2026-10-20: Koa for HTTP, React + Vite for UI, Docker Compose for everything, pnpm monorepo in this single repo.
- 2026-10-20: Hand-rolled Node stream processors, not Flink/Kafka Streams. One language, and windows/watermarks are ours to explain.
- 2026-10-20: Elasticsearch holds raw events and aggregates. Aggregates are idempotent via deterministic ID plus external versioning.
- 2026-10-20: Aggregator publishes to compacted Kafka topic `aggregates` (no dual write); indexer sinks to ES; alert-evaluator and SSE consume the topic.
- 2026-10-20: Aggregator dedups with per-window sets of seen keys (first 128 bits of SHA-256 of `tenantId:eventId`), checkpointed. Hashes replace exact IDs to bound the checkpoint (see caps below); collision odds are negligible and a cryptographic hash prevents crafted cross-tenant collisions. Bloom filters would silently drop real events from counts.
- 2026-10-20: Kafka client = `@confluentinc/kafka-javascript` behind our interface (kafkajs is largely unmaintained). Verify and benchmark in Phase 1.
- 2026-10-20: Dashboard v1 auth = query-scoped API key in sessionStorage; OIDC/login documented as the production path.
- 2026-10-20: Quality bar = 100% coverage (narrow approved exclusions), Stryker on domain packages, fresh-context review against `CODE_REVIEW.md`, fix all findings or flag disagreements, commit hook for tests/coverage.
- 2026-10-20: One aggregate record per (tenant, service, env, window) holding counts by level, so error-rate rules see a complete window; alerts evaluate closed windows only (stable breach start); closed windows are immutable, backfill is out of scope for v1.
- 2026-10-20: Aggregator and alert-evaluator checkpoints are fenced: the new owner writes a claim (generation + 1, conditional on the loaded seq_no; `op_type: create` for the first claim) before consuming; the loser drops state.
- 2026-10-20: Quiet-partition watermark advance applies only to caught-up, unpaused partitions with nothing fetched-but-unprocessed, and never right after a restore. Stalled or lagging partitions are never idle.
- 2026-10-20: `query-api` serves the last 15 min from a bounded, tenant-scoped in-process map of the `aggregates` topic (1m resolution, revision-guarded snapshot + delta over fetch-based SSE); older history comes from ES. The `aggregates` topic is a delivery channel, not a rebuild source.
- 2026-10-20: Live-map safety: `tenantId`/`env`/rule ids restricted to `^[a-z0-9-]{1,64}$` (composite IDs unambiguous); no per-tenant series limit at ingest in v1 (needs shared state; planned for v2); query-api map caps series (200 per tenant, 15,625 per instance) with at most 32 windows each so entries are hard-bounded, evicts by window start before admitting, sheds only never-seen series, and flags `partial` per tenant for 30 min after a shed (on `/v1/series`, the stream's snapshot-meta event, and every heartbeat); live endpoints (not `/readyz`) are gated on catch-up and lag; SSE streams re-check the key and are connection- and buffer-bounded.
- 2026-10-20: Alerts reach the UI by polling ES (`/v1/alerts`), not SSE; the evaluator ignores windows at or before its last-evaluated start, and a missing window breaks a streak.
- 2026-10-20: Aggregator caps: 5,000 open windows per partition, 100,000 seen keys per window, 500,000 per partition; overflow goes to `events.rejected` (never a pause, which would stop the watermark and deadlock the partition). Check order per event: watermark, too-late, seen-set, caps. Worst-case checkpoint budget ~33 MB uncompressed, under the ES request limit.
- 2026-10-20: Side topics and `alerts` (7-day local retention): `events.late` and `events.rejected` keyed `tenantId:eventId`, `events.dlq` keyed `sourceTopic:partition:offset`, `alerts` keyed by alert id; window records carry `rejectedCount` so undercounts are visible.
- 2026-10-20: `revision = (count + rejectedCount)*2 + closed` so rejection-only updates are not dropped as stale; `events.dlq` is keyed by source topic:partition:offset (undecodable input has no event id); evaluator per-series state expires after 1 h of event time.
- 2026-10-20: Toolchain pinned: Node 24.22.0 LTS (26 not LTS yet), pnpm 12.14.0 via corepack, TypeScript 6.0.3 for tsc and ESLint (typescript-eslint caps at <6.1), ESM, inline lint suppressions disabled, dependency rules fixture-tested (ADR-0001).
- 2026-10-20: Commit gates: one `verify-gate.sh` (no commits on main, clean tree, `pnpm verify`, tree-hash stamp) behind husky and a Claude Code hook that also denies hook bypasses; `commit-msg` requires Conventional Commits and a `Review:` line; CI re-checks verify and every PR commit message; `main` protected (ADR-0002).
- 2026-10-20: Kafka client = `@confluentinc/kafka-javascript` (benchmark: ~2.8x kafkajs produce throughput with idempotent acks=all; level median latency at 5k msg/s with linger.ms=0, kafkajs a few ms better at p99; similar consume; maintained; only it offers group-less assign); explicit murmur2 partitioner for Java-compatible key hashing (ADR-0003).
- 2026-10-21: Review stopping rule and one commit per session: pass 1 reviews the full diff, passes 2-3 only the fix delta, no pass 4; leftovers are fixed with a targeted test or escalated (blocker/major) or logged to `docs/follow-ups.md` (minor/nit). Five full re-review passes on the 4,000-line `@obs/kafka` diff kept surfacing new edge cases; smaller commits and delta reviews keep review cost bounded.
