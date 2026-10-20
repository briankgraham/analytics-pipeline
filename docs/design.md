# Observability platform (mini Datadog/Sentry): design

## Context
Project #2 in `TODO.md` (`real-time-analytics-pipeline/`), reshaped into a product: apps ship logs/events to an ingest API, Kafka buffers them, stream processors compute windowed aggregates and alerts, Elasticsearch serves search and aggregates, and a React dashboard shows it live. Goal per root `CLAUDE.md`: depth and defensible tradeoffs, not features. Talking points to build for real: event time vs processing time, watermarks and late-event correction, idempotent sinks / "exactly-once", partitioning and hot keys, backpressure, DLQ.

Design settled 2026-10-20. The Decision log in the project `CLAUDE.md` records when and why each choice was made; this file holds the resulting design.

## Core decisions
- TypeScript (strict) on Node LTS for every service: shared event schemas across services and UI, and compile-time contracts make reviews meaningful.
- Koa for HTTP services (ingest-api, query-api); React + Vite for the UI; Docker Compose for everything.
- Stream processing is hand-rolled Node consumers (on the Confluent client, see Tooling flag), not Flink/Kafka Streams: one language, and windows/watermarks are ours to explain.
- Elasticsearch holds both raw events and windowed aggregates (separate indices). Aggregates are upserted by deterministic doc ID, which makes the sink idempotent.
- Single git repo, pnpm workspaces monorepo.

## Architecture
```
 log-generator ──HTTP──> ingest-api (Koa) ──> Kafka: events.raw (key=tenant:service)
                                                   │
                      ┌────────────────────────────┴───────────────┐
                      v                                            v
                 indexer ──> ES: events-*                     aggregator ──> Kafka: aggregates (compact,delete)
                 (bulk, create, _id=tenantId:eventId)                 (windows, watermarks,     │      │      │
                      ^                                        checkpoints in ES)       │      │      │
                      └──── indexer also sinks aggregates/alerts to ES <────────────────┘      │      │
                                                              alert-evaluator <────────────────┘      │
                                                              (rules, dedup) ──> Kafka: alerts        │
                                                                                                      v
 web (React) <──REST + SSE── query-api (Koa) <── ES (search, history) + bounded in-process map of recent windows (from `aggregates`)
```

| Service | Responsibility | Notes |
|---|---|---|
| `ingest-api` | `POST /v1/events` (batch), validate, API-key auth, per-key rate limit, produce to Kafka, `202` | Stateless; backpressure = 429/503 when producer queue is full; key lookups survive an ES outage (see API keys) |
| `indexer` | Consume `events.raw` → ES bulk index, `_id` = `tenantId:eventId`; also sinks `aggregates` and `alerts` topics to their ES indices | Idempotent on replay; poison/invalid → `events.dlq` after bounded retries |
| `aggregator` | Tumbling 1-min windows per (tenant, service, env); one record holds counts by level + a latency sketch; per-partition watermark = min(max event time, max receivedAt) − allowed lateness (30s) | Late-but-allowed events emit a corrected window record; too-late go to `events.late` + a counter; cap overflow goes to `events.rejected` |
| `alert-evaluator` | Rules (v1: error rate over one (tenant, service, env) series) over aggregates; dedup/cooldown; emits `alerts` | Rule logic is pure functions. Each window record carries counts for every level, so error rate comes from one complete record (no partial-window problem). Evaluates **closed windows only**, in window order per series, and ignores any closed window at or before its per-series last-evaluated start (aggregator restore re-emits cannot reorder it); a window with no record counts as a non-breach and breaks a streak, so `breachStartWindow` is stable. Gap detection: a closed record whose start is more than one window after the series' last-evaluated start means the windows between were missing and count as non-breach; a silent series emits nothing, so its streak breaks when its next window arrives. Aggregator restore re-emits finalized windows in start order. The evaluator uses `errorRate = (error + fatal) / count`, where count 0 means no breach. Alert id: `{ruleId}:{tenantId}:{service}:{env}:{breachStartWindow}`. A series lives in one `aggregates` partition (explicit partition = input partition); the evaluator checkpoints per-partition offsets, rule state, and each series' last-evaluated start (fenced like the aggregator); a series' entry expires after 1 h of event time without a window for it (event time, so the injected-`Clock` rule holds; the aggregator's re-emit horizon is at most one checkpoint interval plus the 30 s lateness, far under 1 h), and closed windows more than 1 h behind the partition's newest closed window are ignored, so a re-emit of an expired series is never evaluated as new and an unknown series' first window counts as no gap (tested) and emits in the same order: emit, flush and await acks, checkpoint, commit. A restart may re-emit an alert, always with the same id; the indexer's `create` dedupes it, and the UI reads alerts from ES by polling. Out of scope for v1: alert resolution/updates, and tenant-wide or multi-service rules (services hash to different partitions) |
| `query-api` | Search (full-text + filters), aggregate series, alert list, SSE stream of live window updates | SSE over WebSocket: one-way, simple, proxy-friendly |
| `web` | Live dashboard, log search, alerts view | Vite + TanStack Query; charts via uPlot or Recharts (load `dataviz` skill when building) |
| `log-generator` | Synthetic services with controllable rate, lateness, duplicates, skew, bursts | Doubles as load and chaos tool |

Side topics (`events.late` and `events.rejected` keyed `tenantId:eventId`; `events.dlq` keyed `sourceTopic:partition:offset`, since undecodable bytes carry no event id): `events.dlq` (poison/invalid input), `events.late` (too late for a closed window), `events.rejected` (cap overflow, see Deep dive 2). Retention 7 days.

Topic settings (local): `events.raw` 12 partitions, 1 h retention; `aggregates` 12 partitions, compact+delete, 6 h; `events.rejected` 12 partitions with explicit partition = input partition; `events.late`, `events.dlq`, and `alerts` 1 partition each (not partition-aligned; raise if throughput needs it); `alerts` is keyed by alert id with a delete cleanup policy; side topics and `alerts` have 7-day retention (a local default, not a hard requirement).

Shared packages: `packages/schemas` (zod event/alert schemas + inferred types), `packages/kafka` (client factory, producer/consumer wrappers, graceful shutdown), `packages/config` (env parsing), `packages/logger` (pino).

### Key design choices to defend
- **Partition key = `{tenantId}:{service}`.** Keeps every window's events on one consumer (locality; the aggregator itself is order-insensitive by design). Cost: hot-key skew when one service is loud. The indexer shares this topic, so salting the key is not possible without breaking window locality. v1 measures skew; if it bites, the fix is a second, salted topic for the indexer. Name this tradeoff in interviews.
- **Percentiles:** p95 is not averageable across windows or partitions, so each window stores a mergeable sketch, not a number. **DDSketch is mandated**: its bucket counts commute, so the result is independent of arrival order and batching. t-digest is not order-independent and would break replay determinism.
- **"Exactly-once" for the aggregator:** at-least-once consumption plus in-memory window state would double-count on restart. Plan: periodic checkpoint of window state + partition offsets in one ES doc; on restart restore and seek; offsets committed only after checkpoint; output records idempotent via deterministic window IDs and the revision rule below. This is the main hard part and the best interview story. Open item to validate in first build: checkpoint interval vs. recovery time.
- **Event time:** clients send `ts`; ingest stamps `receivedAt`; windows use `ts`. Absurd timestamps are rejected at ingest, never clamped (clamping to `receivedAt` would let a retried event land in a different day index and break dedup).
- **Retention/rebuild:** ES indices by day with ILM-style deletion. Rebuilding aggregates is out of scope for v1 (closed windows are immutable); the only resync source is `events.raw`, and only within its retention (1 h locally). An outage longer than that leaves a gap that a gap/lag metric reports.
- **Schema evolution:** `schemaVersion` field on events; zod parsers tolerate unknown fields.

### Capacity numbers (to be measured in Phase 5)
- Target 5k events/s sustained, 10k/s burst; ~500 B/event → ~2.5 MB/s, ~216 GB/day raw; local demo retention 1 h in Kafka, 1–7 d in ES.
- 12 partitions on `events.raw`; ~3 consumers per group at ~2–5k msg/s per consumer with batching. Measure and record real numbers in README.
- query-api live map: ~300 B per (tenant, service, env, window) entry; the map caps **series**, not entries: 200 series per tenant and 15,625 per instance, and each series holds at most 32 windows (the 30 min eviction by window start guarantees it), so entries are hard-bounded at 6,400 per tenant and 500k per instance (~150 MB worst case). A series here is a (tenant, service, env). Series cardinality is client-controlled (`service`, `env`), and v1 does **not** enforce a per-tenant limit at ingest: a correct limit needs shared state across stateless ingest instances (v2: a tenant-scoped series registry with expiry). v1 bounds the damage where state lives: the query-api map (shedding, see below) and a per-partition open-window cap in the aggregator (5,000; overflow goes to `events.rejected`, see Deep dive 2).

## Repo layout
```
real-time-analytics-pipeline/
  services/{ingest-api,indexer,aggregator,alert-evaluator,query-api,log-generator}/
  apps/web/
  packages/{schemas,kafka,config,logger}/
  docs/adr/            # one short ADR per major decision
  docker-compose.yml   # kafka (KRaft), elasticsearch, all services, web
  CLAUDE.md  README.md
```

## Code quality standard (what reviews check against)
Tooling, enforced in CI and pre-commit (from Phase 1):
- `tsc --noEmit` with `strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`; ESLint with `typescript-eslint` strict-type-checked; Prettier; `dependency-cruiser` to forbid service-to-service imports (services share only `packages/*`).
- Tests: Vitest unit tests for pure logic (windowing, watermarks, rules, sketches); **property-based tests (fast-check)** that shuffled/duplicated event streams produce identical aggregates; integration tests with Testcontainers (real Kafka + ES); Playwright e2e for the dashboard. Coverage gate is 100% (see Testing standard).
- Conventions: Conventional Commits, small PRs, ADR for any decision affecting >1 service.
- Koa conventions: ordered middleware (error handler → request ID → logger → auth → rate limit → body parser → router); routes are thin, logic lives in service modules; zod validation at every boundary; typed context; structured pino logs with request/event IDs; `/healthz` and `/readyz`; graceful shutdown (stop intake, flush producer, commit offsets).
- React conventions: function components, TanStack Query for server state, no business logic in components, accessible markup, Testing Library for component tests.
- Config via env, validated at boot; no secrets in repo; Dockerfiles multi-stage, non-root, pinned base images.

PR review checklist (goes in `.github/pull_request_template.md`): failure modes considered (crash, dup delivery, late data, overload, bad input)? idempotent? backpressure/timeouts/retries bounded? new config documented? tests cover the failure path? observability (logs/metrics) added? docs/ADR/CLAUDE.md updated?

## Code review process (`CODE_REVIEW.md` + always-fix rule)
- `CODE_REVIEW.md` at repo root is the single source of truth for reviews. Contents: severity scale (blocker / major / minor / nit), required output format (finding, `file:line`, why it matters, concrete fix), and checklists by area: correctness, failure modes (crash, dup delivery, late data, overload, bad input), idempotency, backpressure and bounded retries, concurrency and shutdown, boundary validation, security (secrets, injection, authz), observability, test quality (tests the failure path, not just the happy path), TypeScript hygiene (no `any`, no unchecked casts), React (state, a11y), Docker/config, docs/ADR updated.
- The project `CLAUDE.md` carries a hard rule: before every commit or PR, review the diff against `CODE_REVIEW.md` in a fresh-context reviewer (subagent or `/code-review`) so the author is not grading their own work.
- **Always fix the recommendations**: all blocker/major/minor findings are fixed, then the review reruns until it reports no blocker, major, or minor findings. Nits are fixed if cheap. A finding the author believes is wrong is never skipped silently: it is reported to the user with reasoning and the user decides. The review summary (found / fixed / flagged) goes in the commit body as a `Review:` line.
- Optional: a `/review` project command (`.claude/commands/review.md`) that loads `CODE_REVIEW.md`, so the process is one keystroke.

## Testing standard: 100% coverage, enforced
- **Gate:** Vitest + v8 coverage with thresholds `100` for lines, branches, functions, and statements on every workspace package, set in a shared `vitest.config` base. CI fails below 100 (CI and hooks are planned for Phase 1). A pre-commit hook runs the same check on changed packages, so it cannot be skipped locally.
- **Exclusions are explicit and rare.** The complete allowed list: composition roots (`main.ts`), tool config files (`*.config.ts`), generated code, type-only files, the Vite entry, and CLI bootstrap scripts (`keys:create`). Each is a config entry or `v8 ignore` with a one-line reason; `CODE_REVIEW.md` makes unexplained exclusions a blocker, and adding to the list requires user approval. Coverage thresholds and `include`/`exclude` patterns are themselves review-checked.
- **One entry point:** `pnpm verify` runs typecheck, lint, Prettier check, dependency-cruiser, unit and integration tests with the 100% coverage gate, and build. Stryker (mutation score ≥ 85%, domain packages) and Playwright run in CI.
- **Test pyramid:** (1) unit tests for all domain classes and base classes, using in-memory fakes; (2) property-based tests (fast-check) for windowing, watermarks, sketch merging, and rule evaluation (shuffled/duplicated/late streams give identical results); (3) integration tests with Testcontainers against real Kafka + ES for each consumer and the ES-backed stores; (4) Koa route tests with supertest; (5) React component tests (Testing Library) plus Playwright e2e for the main flows; (6) chaos/failure-mode tests (kill aggregator mid-window, ES down, poison message, duplicate delivery).
- **Coverage is necessary, not sufficient:** 100% line coverage can still assert nothing. So `CODE_REVIEW.md` requires each test to assert behavior (and failure paths), and we run **Stryker mutation testing** on the pure domain packages (windowing, sketch, rules, checkpointing) with a high mutation-score target (≥85% to start, tightened later). This is also a strong interview talking point.
- **TDD for domain logic:** write the failing test first for windowing, watermark, and checkpoint code; tests and code land in the same commit.
- **Determinism:** no real time or randomness in unit tests (injected `Clock` and seeded RNG), no sleeps; integration tests wait on conditions with timeouts.

## Rules that every session must follow
They live in a "Non-negotiable rules" section at the top of the project `CLAUDE.md` (loaded every session), with tooling enforcement added in Phase 1 so no rule depends on memory alone:
1. Never commit with failing `pnpm verify` (typecheck, lint, format, dependency rules, tests, 100% coverage, build). CI and pre-commit enforce it from Phase 1 on; commits before the Phase 1 tooling commit are docs-only and exempt.
2. Tests are written with the code, never after; domain logic is test-first.
3. Before every commit or PR, run a fresh-context review against `CODE_REVIEW.md`; fix all blocker/major/minor findings; rerun until clean; flag disagreements to the user.
4. No new coverage exclusions, lint suppressions (`eslint-disable`, `@ts-ignore`, `any`) without user approval and a reason comment.
5. Domain classes never import Kafka or ES; services never import other services; shared code lives in `packages/*`.
6. Any decision affecting more than one service gets an ADR and a dated line in the CLAUDE.md decision log.
7. README stays accurate whenever behavior or run instructions change.
8. Small, meaningful commits (Conventional Commits), made inside this repo only.
Enforcement layers: `CLAUDE.md` (rules for the author), `CODE_REVIEW.md` (rules for reviewers), CI plus husky pre-commit and a `commit-msg` check requiring a `Review:` line (rules for everyone), and a Claude Code PreToolUse hook that blocks `git commit` unless `pnpm verify` passes.

## What the code looks like
Principle: **template-method base classes for lifecycle and plumbing; composition and pure classes for domain logic; interfaces at every I/O boundary.** Inheritance is one level deep and only for lifecycle. No DI framework; manual constructor injection wired in each service's `main.ts` (composition root).

```ts
// packages/kafka: shared plumbing, written once
abstract class BaseConsumer<T> {
  constructor(protected readonly deps: ConsumerDeps) {}   // kafka, logger, metrics, dlq, config
  async start(): Promise<void>        // subscribe, run loop, heartbeat, health flag
  async stop(): Promise<void>         // pause, drain in-flight batch, commit, disconnect
  protected abstract decode(raw: KafkaMessage): T;                  // zod parse; throws PoisonMessageError
  protected abstract handleBatch(batch: Decoded<T>[]): Promise<void>; // domain work
  // template method (private): per-batch retry with capped backoff, poison -> DLQ,
  // offset resolution, metrics, structured logging, graceful shutdown
}

// services/indexer
class IndexerConsumer extends BaseConsumer<RawEvent> {
  constructor(deps: ConsumerDeps, private readonly sink: EventSink) { super(deps); }
  protected decode = parseRawEvent;
  protected handleBatch(batch) { return this.sink.bulkIndex(batch.map(m => m.value)); }
}

// services/aggregator: Kafka-free domain classes, unit/property-tested in isolation
class WindowAssigner { assign(ts: number): WindowKey[] }
class WatermarkTracker { observe(partition: number, ts: number): void; current(): number }
class WindowStore { add(event): WindowUpdate; close(upTo: number): ClosedWindow[]; snapshot(): State; restore(s: State): void }
class LatencySketch { add(ms): void; merge(o: LatencySketch): void; quantile(q): number }
class AggregatorConsumer extends BaseConsumer<RawEvent> {
  constructor(deps, windows: WindowStore, watermark: WatermarkTracker,
              aggregates: AggregateStore, checkpoints: CheckpointStore) { ... }
}

// I/O boundaries are interfaces with a real impl and an in-memory fake
interface EventSink { bulkIndex(events: RawEvent[]): Promise<void> }          // ES impl + fake
interface AggregateStore { upsert(w: WindowDoc[]): Promise<void> }            // idempotent by ID
interface CheckpointStore { load(): Promise<Checkpoint|null>; save(c: Checkpoint): Promise<void> }

// producers: BatchingProducer wraps the Kafka client with bounded queue, retry, backpressure signal
// HTTP: createApp(deps): Koa  (middleware stack + routers); routers call service classes
class EventIngestService { ingest(events: RawEvent[], apiKey: ApiKeyInfo): Promise<IngestResult> }
```
Why: the consumer loop, retries, DLQ, shutdown, and metrics are the same everywhere, so they are written and tested once in `BaseConsumer`. Domain logic never imports Kafka or ES, so it is fast to test (fake sinks, property-based tests) and easy to explain. Tradeoff to name in interviews: inheritance couples subclasses to the base's lifecycle, so we keep it to one level and push variation into injected collaborators.

## Deep dive 1: event schema and Elasticsearch mappings
**Event (client-sent, zod in `packages/schemas`):** `schemaVersion` (1), `id` (ULID, client-generated: the idempotency/dedup key), `ts` (epoch ms, event time), `service` (`^[a-z0-9-]{1,64}$`), `env` (`^[a-z0-9-]{1,64}$`), `host?`, `level` (debug|info|warn|error|fatal), `message` (max 8 KB; longer is rejected per event with `message_too_large`), `traceId?`, `spanId?`, `durationMs?` (request-latency events feed the percentile sketch), `attrs?` (flat map, max 32 keys, string/number/boolean values, 256-char value cap). Batch limit: 500 events or 1 MB.
**Server-added (never trusted from payload):** `tenantId` (from the API key), `receivedAt`. `tenantId`, `env`, and rule ids all use `^[a-z0-9-]{1,64}$`, so `:`-joined composite IDs (ES `_id`, Kafka key, alert id) are unambiguous; a schema test rejects `:`.
**Timestamp guards (ingest, per-event reject with a code):** reject `ts` more than 30 s (the allowed lateness) ahead of `receivedAt`, or older than the ES retention window as of arrival (configured; default 7 days). One bad future timestamp would otherwise advance a partition's watermark and make every other key on that partition "too late". As defense in depth the aggregator also caps the watermark by the partition's max `receivedAt` (see Watermark). The "older than retention" check depends on arrival time, so it is applied once at ingest and never re-evaluated on replay.

**Indices:**
- `events-YYYY.MM.DD`, **routed by event-time day, not arrival day.** Dedup by `_id` only works within one index, so a replayed or retried event must deterministically land in the same index. **`_id = {tenantId}:{eventId}`**: the event id is client-supplied, so it must be tenant-scoped or one tenant could suppress another's events (or two honest tenants could collide). Index with `op_type: create`; a 409 means "already stored" and is treated as success. Tests include two tenants sending the same event id.
- Mapping: `dynamic: strict` at top level; `service/env/host/level/traceId/tenantId` = `keyword`; `ts/receivedAt` = `date` (epoch_millis); `durationMs` = `float`; `message` = `text`; **`attrs` = `flattened`** (prevents mapping explosion from arbitrary client keys; interview talking point). Local settings: 1 shard, 0 replicas. Production sizing note: at ~216 GB/day, target 20-50 GB shards, so ~6 shards/day with 1 replica, or size-based rollover.
- `aggregates-v1` (ES index, small; distinct from the `aggregates` Kafka topic): `_id = {tenantId}:{service}:{env}:{windowStartMs}`; one doc per window holding `windowStart/End`, `count`, `countsByLevel` (debug/info/warn/error/fatal), `durationCount`, `p50/p95/p99`, `rejectedCount`, `sketch` (stored, `enabled: false`), `closed`, `revision`.
- Writes use ES **external versioning** with `version = revision = (foldedEventCount + rejectedCount) * 2 + (closed ? 1 : 0)`. ES requires a strictly greater version, so finalizing a window (same count, `closed: true`) still wins, and a stale or replayed write loses. A 409 on this path means the stored doc is at least as complete, so it is treated as success. The dashboard never sees counts go backwards. A window is never mutated after `closed`.
- `alerts-v1` (`_id` is the deterministic alert id, see alert-evaluator), `api-keys` (SHA-256 hash, tenant, scopes, revokedAt), `checkpoints` indices. The `checkpoints` mapping stores the state body (windows, sketches, seen keys) as a non-indexed object (`enabled: false`) and maps only `service` (keyword), `partition`, `generation`, `nextOffset`, and `watermark`, with `_id = {service}:{partition}` (for example `aggregator:7` and `alert-evaluator:7`, which share partition numbers but never a doc; the claim and `op_type: create` apply per `_id`), so window-keyed fields never hit ES field limits. The `alerts` Kafka topic is keyed by alert id with 7-day retention.

**DLQ envelope** (`events.dlq`): original bytes (base64), `stage`, `error`, `attempts`, source topic/partition/offset, `failedAt`.

## Deep dive 2: aggregator internals
- **State is partitioned like the input.** Key = `{tenantId}:{service}` so one Kafka partition owns whole windows. Checkpoint = one ES doc per input partition: `{partition, nextOffset, watermark, windows (counts, `rejectedCount`, sketches, seen keys)}`. A single doc write is atomic, so state and offset can never disagree.
- **Loop:** consume batch → fold into `WindowStore` → emit window updates (throttled ~1/s per window; a window's final `closed: true` record is never throttled away) → **flush the producer and await acks for everything emitted so far** → write the checkpoint → commit Kafka offsets (informational; the checkpoint offset is the source of truth, applied with an explicit `seek` on start). Ordering rule: output durable, then checkpoint, then commit. Never checkpoint past output that is not yet acked.
- **Checkpoint fencing:** each checkpoint write uses ES `if_seq_no`/`if_primary_term` and carries a monotonically increasing `generation`. **On assignment the new owner first writes a claim** (generation + 1, conditional on the `seq_no`/`primary_term` it just loaded; for the first-ever claim with no checkpoint doc, `op_type: create` with generation 1, where a 409 means lose) and only then starts consuming; a stalled former owner's later write conflicts. Whoever loses a conflict stops that partition, drops its in-memory state, and re-loads from the checkpoint when (if) reassigned. Tested explicitly, including a stale owner that wakes up after the new owner claimed but before it wrote.
- **Recovery:** on assignment (start or rebalance) load that partition's checkpoint, restore state, seek to `nextOffset`. Events replayed after a crash are recomputed deterministically; output writes are idempotent via the revision rule above. **On restore, every restored open window is re-emitted** (and any window already past the restored watermark is finalized and emitted), so an output lost between emit and crash is regenerated. Changing partition count remaps keys and invalidates state: treat as repartition = new topic + replay (talking point).
- **Watermark:** per partition only, `W_p = min(maxEventTs_p, maxReceivedAt_p) - allowedLateness (30s)`, monotonic and persisted in the checkpoint. A window lives in exactly one partition (see key), so no cross-partition minimum is needed, and a rebalance can never move a watermark backwards. The `receivedAt` cap bounds the damage of a bad producer clock. **Quiet partitions:** a partition counts as idle only when its consumer position equals the partition's high-water mark, it is not paused, it has no fetched-but-unprocessed messages, and it has fetched no messages of any kind (duplicate, too-late, rejected, or poison) for 30 s (injected `Clock`). Only then does `W_p` become `max(W_p, clock.now() − 30 s)`, so open windows still close. A partition that is merely stalled (backlog after a restart, `pause()` for backpressure, ES outage) is never idle, and after a restore the rule stays off until the partition has caught up once. Determinism limit, stated plainly: this idle rule is the one place where a replay can classify an event differently from the original run (an event arriving after an idle close is too-late); all other behavior replays identically.
- **Window lifecycle:** window `[s, e)` is finalized when `W >= e`, emitted with `closed: true`, and evicted. An event whose window is already finalized is *too late*: sent to `events.late`, counted in a metric, still searchable in the raw index; closed aggregates are never mutated, so a backfill would rebuild into a new index version (`aggregates-v2`) and stored alerts are not re-derived (out of scope for v1). An event after `e` but before finalization is accepted and bumps the revision (late-but-allowed correction).
- **Duplicates:** client HTTP retries put the same event `id` in Kafka twice, and idempotent producers do not prevent that. Each open window keeps a set of seen event keys (the first 128 bits of SHA-256 of `tenantId:eventId`, stored as unpadded base64 (22 characters) in the JSON checkpoint; a cryptographic hash so a client cannot craft a collision that suppresses another tenant's event, and accidental collision odds are negligible), included in the checkpoint (hashes do not compress, so all budgets here are uncompressed). Realistic size: 5k/s over 12 partitions x 90 s is about 36k keys per partition, far under the caps. Caps: 100,000 keys per window and 500,000 per partition (about 12.5 MB as base64 in the JSON checkpoint, worst case). **Check order for every event: (1) feed the watermark tracker (any fetched message, including duplicates, too-late, rejected, and poison, counts as activity for the idle rule), (2) too-late check (window already finalized: `events.late`), (3) seen-set lookup (a duplicate is dropped silently as a duplicate, never rejected), (4) caps.** On a cap overflow the event goes to `events.rejected` (reason `dedup_cap`) and is not counted in aggregates; the window's `rejectedCount` counts them and is finalized on `closed` (a `dedup_cap` rejection when no window exists yet behaves like `window_cap`: metric and topic only), and it is part of the revision so live updates are never dropped as stale, so an undercount is visible rather than silent. Overflow is never handled by pausing: a pause stops the watermark and would deadlock the partition.
- **Percentiles:** one DDSketch per window; p95 over a longer range = merge sketches, never average percentiles. Determinism claims in tests (shuffled input gives identical output) rely on this choice.
- **Open-window cap and rejected events:** each partition holds at most 5,000 open windows (tunable; chosen to keep the worst-case checkpoint bounded, see budget below). Series cardinality is client-controlled, so at the cap the aggregator keeps consuming and closing windows, and events that would open a new window go to `events.rejected` (reason `window_cap`). **Watermark first:** every event, rejected or not, updates `WatermarkTracker` before any cap check and, like any fetched message, counts as activity for the idle rule, so a full cap drains as windows age out (test: cap full of silent series, only new-series traffic arrives, windows close and the partition recovers; a diff that observes the watermark after the cap check fails review). Rejected events are not counted in aggregates: `dedup_cap` rejections increment the window's `rejectedCount`; `window_cap` rejections have no window and are visible only through the metric and the topic. **Topic `events.rejected`:** key `tenantId:eventId`, same partition number as the input partition, value = original event + `reason` + source offset, 7-day retention, no consumer in v1 (the aggregator itself emits the rejection counter, which is approximate and may over-count after a crash replay; exact accounting uses the topic's distinct keys); the events remain searchable because the indexer reads `events.raw` independently. It is distinct from the `rejected` array in the ingest API response. Records are produced and acked before the checkpoint under the same emit, flush, checkpoint, commit order, so advancing `nextOffset` past them never loses them. **Checkpoint size budget:** worst case is about 33 MB uncompressed (5,000 windows x ~4 KB of DDSketch + counts = 20 MB, plus 500,000 seen keys as unpadded base64 in the JSON doc at ~25 B each = ~12.5 MB), under the ES 100 MB request limit with margin; a test writes and restores a cap-sized checkpoint. Rule: no pause may depend on window closing to clear; the only pause is output backpressure, which resumes when downstream recovers.
- **Backpressure:** bounded in-flight batches; if the ES/Kafka output slows, the consumer pauses partitions (`pause/resume`), so lag grows visibly instead of memory.
- **Output path:** the aggregator produces window updates to Kafka topic `aggregates` (record key = window ID, for compaction only; explicit partition = the input partition number, so `aggregates` has the same 12 partitions as `events.raw` and a test asserts produced partition == input partition; `cleanup.policy=compact,delete` with 6 h retention, longer than any consumer's catch-up window, so window IDs do not accumulate forever). The indexer sinks that topic to ES; `alert-evaluator` and `query-api` (for SSE) consume it. The aggregator's only external write besides Kafka output is its checkpoint, and every output is idempotent, so crash recovery needs no distributed transaction. Honest tradeoffs: one more hop of latency (~100s of ms); compaction keeps the *last-written* record, not the highest revision, so a zombie or replay write could leave a stale record. Every consumer (indexer via ES versioning, alert-evaluator and SSE via an explicit `revision` guard) ignores records with a lower revision than they have seen. A fenced zombie can still emit during the one checkpoint interval before it notices, so **the `aggregates` topic is a delivery channel, not a rebuild source**: rebuilding derived state means replaying `events.raw`. Records go to the same partition number as their input partition. `events.late` records carry `tenantId:eventId`, and consumers count distinct IDs, so crash-replay duplicates are harmless.
- **Failure matrix to test:** crash before/after checkpoint; crash after emit but before ack (window must be re-emitted); crash after final `closed` record is buffered but not flushed; stale-owner checkpoint write after rebalance (fenced); rebalance mid-window; duplicate events (including same id from two tenants); event exactly at window boundary; event exactly at the watermark; future timestamp (rejected at ingest, and capped by `receivedAt` in the aggregator); idle partition; open-window cap and dedup cap overflow (events go to `events.rejected`, the partition keeps closing windows and recovers, nothing is skipped silently); cap full of silent series with only new-series traffic arriving (watermark still advances, windows close); cap-sized checkpoint writes and restores; alert-evaluator restart (no duplicate alert); restart with a large backlog, backpressure `pause()`, and ES outage (none may trigger the idle rule or produce spurious `events.late`); stale owner waking between the new owner's claim and first write.

## Deep dive 3: API and auth design
**Ingest (`ingest-api`, Koa):** `POST /v1/events` with `{ "events": [...] }`, `Authorization: Bearer <key>`.
- Responses: `202 {accepted, rejected:[{index, code}]}` (partial acceptance is allowed and reported per event), `400` malformed body, `401/403`, `413` too large, `429` + `Retry-After`, `503` when Kafka is unavailable or the producer queue is full.
- **`202` means durably in Kafka** (idempotent producer, `acks=all`), awaited before responding, never fire-and-forget. Event `id` is the idempotency key, so clients can safely retry whole batches.
- Errors use RFC 9457 `application/problem+json` with `requestId`.
- Rate limit: per-key token bucket, in-memory per instance for now (default 1,000 events/s, burst 5,000). The correct distributed version belongs to the `rate-limiter-or-api-gateway-service` project; note the reuse there rather than building it twice.

**API keys:** `obs_<env>_<32 random bytes base64url>`, stored as SHA-256 (keys are high-entropy, so a slow hash adds nothing), with `tenantId`, `scopes` (`ingest`, `query`), `revokedAt`. Looked up through an in-process cache with a 30 s freshness window (revocation delay is the stated tradeoff). **If ES is unavailable, expired entries are served stale for up to 1 h** (fail-open for known-valid keys, fail-closed for unknown ones), so an ES outage does not stop ingest and the "ES down, Kafka buffers" demo holds. Unknown keys during an outage get `503`, not `401`. Precondition, stated plainly: this works for instances with a warm cache; a cold instance started during an ES outage can only serve keys loaded from the bootstrap key file (config seed: hashes plus tenant and scopes, never raw keys; a revoked key stays valid on a cold instance until the file is redeployed), so the demo starts instances before killing ES. The cache is bounded (LRU, 10k entries) and has a 30 s negative-cache TTL so random unknown keys cannot cause an ES lookup per request. Scope is checked per route (`ingest` vs `query`). Keys are created by a CLI script (`pnpm keys:create`), no management UI in v1. Keys are never logged (the logger redacts `authorization`).

**Multi-tenancy:** `tenantId` comes only from the key. All reads go through one `TenantScopedSearch` class that always injects the tenant filter, with a test proving no query path can omit it. The in-process recent-window map is a second read path, so it is wrapped by a `TenantScopedWindowCache` with the same always-inject-tenant guarantee and the same omission test. Noisy-neighbor control = per-tenant rate limit at ingest. Kafka key includes the tenant so tenants spread across partitions.

**Query API (`query-api`, Koa):**
- `GET /v1/search?q&service&level&env&from&to&cursor&limit`: `simple_query_string` (not `query_string`, to block expensive or syntax-abusing queries), `search_after` cursor on `[ts, id]`, max limit 200, max range 7 d, request timeout, `track_total_hits` capped at 10,000.
- `GET /v1/series?metric=count|errorRate|p95&service&from&to&step`, `GET /v1/services`, `GET /v1/alerts` (polled by the UI from ES every 10 s; alerts are not on the SSE stream). `errorRate = (error + fatal) / count` per bucket (0 when count is 0). The live segment (last 15 min) is always returned as 1m buckets flagged `resolution: "1m"`, and the client re-buckets counts and error rate; p95 inside the last 15 min is shown at 1m only, a documented limit (coarse p95 needs sketches, which the live map deliberately omits). History older than that honors `step`.
- `GET /v1/stream` (SSE wire format; window updates only): browser `EventSource` cannot send an `Authorization` header, so the UI consumes the stream with `fetch` + `ReadableStream` and sends `Authorization: Bearer`; the key never appears in a URL or access log. `query-api` runs one in-process consumer per instance using `assign()` with **no consumer group**, starting from the offsets at (now − 30 min) via `offsetsForTimes`. It keeps a bounded in-process map (`TenantScopedWindowCache`) of the latest record per window (counts, `countsByLevel`, p50/p95/p99, `revision`; no sketches), **evicting entries whose window start is older than now − 30 min** (records for old windows, e.g. after a backlog replay, are never admitted). Caps: 200 series per tenant and 15,625 per instance, at most 32 windows per series; eviction runs before admission and records with a window start in the future beyond the 30 s guard are not admitted. At a series cap, a never-seen series is shed (metric incremented) while established series always admit their new windows ("never-seen" means no entry in the map: a series' slot is freed when its last window is evicted, after which it is subject to the shed rule again), so entries can never exceed 6,400 per tenant or 500k per instance. **`partial` semantics:** a per-tenant flag, set when that tenant has had any series shed in the last 30 min and cleared 30 min after the last shed (lifetime tested). `/v1/series` returns `partial` and the shed-series count; the stream sends `partial` in its initial snapshot-meta event and on every heartbeat, so SSE clients see changes too. Older windows of a shed series still come from ES; other tenants are unaffected. It fans out to connected clients filtered by tenant, heartbeat comment every 15 s.
  **Liveness:** `/readyz` reflects only hard dependencies for the endpoints that need them (search and alerts need ES; ingest needs Kafka); it does not depend on the live map. The live endpoints (`/v1/stream` and the live segment of `/v1/series`) return `503` + `Retry-After` until the consumer position reaches the high-water mark captured at startup, and again whenever consumer lag exceeds 30 s; lag is exported as a metric, so a stale map is never served as fresh.
  **Live range = the last 15 min at 1m resolution.** Windows with start ≥ now − 15 min come from the map; older windows come from ES, except that for windows 15–30 min old the higher `revision` of map vs ES wins (this covers ES lag or an outage), so a chart never regresses. **Gap-free snapshot + delta:** the client opens the stream first and buffers deltas, fetches the 1m snapshot, then applies only buffered/live deltas whose `revision` is greater than the snapshot's value for that window, ignoring deltas for windows outside its snapshot range; lower ones are dropped (this also hides compacted-topic replays). **Connection hygiene:** the key is re-checked against the key cache every 60 s (every 4th heartbeat) and the stream is closed when it is revoked; per-key connection cap (10); bounded per-client send buffer (1,000 messages), and a client whose buffer fills is disconnected rather than allowed to grow memory.
- OpenAPI generated from the zod schemas (`/openapi.json`); contract tests assert responses match.

**Dashboard auth (v1):** the UI takes a query-scoped API key, held in memory/`sessionStorage`. Simple and honest for a demo; named limitation: a key in browser JS is exposed to XSS, and a real product would use user login (OIDC) with sessions.

**Tooling flag:** `kafkajs` has been largely unmaintained for years. Recommendation: build `packages/kafka` against `@confluentinc/kafka-javascript` (librdkafka-based, with a KafkaJS-compatible API) behind our own interfaces so the choice is swappable. Verify current maintenance status and benchmark both in Phase 1 before locking in.

## Quality and delivery decisions
- Coverage: 100% on lines/branches/functions/statements; the only allowed exclusions are the list in the Testing standard (each justified, additions need user approval); composition roots are covered by compose smoke/e2e tests.
- Stryker mutation testing on pure domain packages, run in CI on PRs touching them.
- Claude Code PreToolUse hook blocks `git commit` unless `pnpm verify` passes; the review step stays a CLAUDE.md rule, backed by a `commit-msg` check for the `Review:` line.
- Reviews use a fresh-context reviewer; disagreements are flagged to the user, never silently skipped.

## Delivery phases
1. Skeleton: workspace, `pnpm verify`, CI, hooks, compose with Kafka + ES, shared packages, Kafka client benchmark, and ADRs backfilled for every cross-service decision in the Decision log (rule 6).
2. Ingest → Kafka → indexer → ES, plus log-generator and basic search API/UI.
3. Aggregator with windows, watermarks, late handling, checkpointing; aggregates in dashboard via SSE.
4. Alert-evaluator, DLQ handling, rate limiting/backpressure.
5. Chaos scripts + measured numbers; README with architecture, tradeoffs, results.
6. **Out of scope for now** (cross-project rule: prefer `docker compose` plus load scripts over cloud infra). Revisit only after Phase 5. If time runs short, cut in this order: Phase 4 rate limiting polish, alert cooldown tuning, Playwright breadth; never cut the aggregator tests or failure-mode demos.

## Failure-mode demos (each a script + test where possible)
Aggregator killed mid-window → restart yields identical counts; duplicate delivery from generator → no double count; late event inside/outside allowed lateness; poison message → DLQ, consumer keeps moving; ES down → ingest keeps accepting (stale-OK key cache), Kafka buffers, indexer resumes, no loss; ingest overload → 429s, not OOM; hot service key → skew visible and mitigated; ES down longer than `events.raw` retention → documented limit: the indexer resumes from the oldest retained offset and the gap is reported by a metric.

## Verification
- Phase 1: `docker compose up` brings Kafka + ES healthy; `pnpm verify` passes locally and in CI.
- End to end: run log-generator at 5k events/s, confirm event counts in ES equal produced counts (with duplicates injected), dashboard updates live, then run each failure-mode script and assert the expected outcome.
- UI checked with the Claude in Chrome extension plus Playwright.
