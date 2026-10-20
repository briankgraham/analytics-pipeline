# Observability platform (real-time analytics pipeline)

See `../CLAUDE.md` for how we work across projects. Full design: `docs/design.md`; decisions and rationale: the Decision log below and `docs/adr/`. If they disagree, this file wins.

## Non-negotiable rules (every session, no exceptions)
1. Never commit unless `pnpm verify` passes (typecheck, lint, format, dependency rules, tests, 100% coverage on lines/branches/functions/statements, build). Enforcement (CI, husky, `commit-msg` check, Claude Code hook) is built in Phase 1; until the commit that adds it, commits are docs-only and exempt, and each one still gets a fresh-context review.
2. Tests are written with the code, never after. Domain logic is test-first (red, green, refactor).
3. Before every commit or PR, review the diff against `CODE_REVIEW.md` using a **fresh-context reviewer** (subagent or `/code-review`), not the author's own context. Fix **all** blocker, major, and minor findings, then rerun the review until it reports no blocker, major, or minor findings. Fix nits if cheap. If I believe a finding is wrong, do **not** skip it silently: tell the user with reasoning and let them decide. Put the review summary (found / fixed / flagged) in the commit body as a `Review:` line.
4. No new coverage exclusions, `eslint-disable`, `@ts-ignore`, or `any` without explicit user approval and a one-line reason comment. The complete list of allowed coverage exclusions is in `docs/design.md` (Testing standard); adding to it needs approval.
5. Domain classes never import Kafka or Elasticsearch clients. Services never import other services; shared code lives in `packages/*`.
6. Any decision affecting more than one service gets an ADR in `docs/adr/` and a dated line in the Decision log below.
7. Keep `README.md` accurate whenever behavior or run instructions change.
8. Small, meaningful commits (Conventional Commits) inside this repo only.

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
- Local machine has Node v25 (non-LTS) and no pnpm at project start. Pin Node LTS (`.nvmrc`, `engines`, Docker `node:<lts>`), and use `corepack enable` for pnpm.
- Kafka client: `@confluentinc/kafka-javascript` behind our own interface. Phase 1 verifies maintenance status and benchmarks against kafkajs before locking in.

## Related projects
- `change-data-capture-pipeline`: shares Kafka infra (compose, topic conventions, `packages/kafka`).
- `real-time-collaborative-editor-or-live-leaderboard`: same SSE/WebSocket push pattern.
- `rate-limiter-or-api-gateway-service`: ingest uses a simple in-memory token bucket; the distributed version lives there.
- `llm-request-gateway`: can ship its logs here later.

## Status
Design approved 2026-10-20. Phase 1 (skeleton) next. No code yet.

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
- 2026-10-20: Side topics (7-day retention): `events.late` and `events.rejected` keyed `tenantId:eventId`, `events.dlq` keyed `sourceTopic:partition:offset`; window records carry `rejectedCount` so undercounts are visible.
- 2026-10-20: `revision = (count + rejectedCount)*2 + closed` so rejection-only updates are not dropped as stale; `events.dlq` is keyed by source topic:partition:offset (undecodable input has no event id); evaluator per-series state expires after 1 h of event time.
