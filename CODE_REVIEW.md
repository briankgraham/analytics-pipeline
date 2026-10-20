# Code review standard

Every diff is reviewed against this file before it is committed or merged. Reviewers work from a fresh context (no knowledge of the author's reasoning) and judge only what the diff, the tests, and the docs show.

## How to run a review
1. Read the diff, then the surrounding code of every changed function. A change that looks fine in isolation can break an invariant elsewhere.
2. Run `pnpm verify` (typecheck, lint, format, dependency rules, tests with the 100% coverage gate, build; Stryker and Playwright run in CI). A failing check is a **blocker**; do not review further until it passes. Before Phase 1 tooling exists, a docs-only diff skips this step and is reviewed for consistency with `docs/design.md` and `CLAUDE.md` instead (see Docs-only diffs). Confirm with `git status`/`git diff --stat` that the diff touches only `*.md` files before skipping; any code, config, or tooling file means the step is not skippable.
3. Walk the checklists below. Skip a section only if the diff genuinely cannot touch it.
4. Report findings in the format below, most severe first. If there are no findings, say what you checked so the result is auditable. Never pad the list with praise or restated code.

## Severity
| Level | Meaning | Required action |
|---|---|---|
| **blocker** | Data loss or corruption, wrong results, security hole, broken build, coverage below 100%, unexplained exclusion or suppression | Must fix before commit |
| **major** | Likely bug under realistic conditions (crash, duplicate, overload, race), missing failure-path test, violated architecture rule | Must fix before commit |
| **minor** | Real but low-impact: unclear naming, missing log context, weak assertion, small duplication | Fix in this change |
| **nit** | Style or taste with no behavioral effect | Fix if cheap |

The author fixes every blocker, major, and minor finding, then the review reruns until it reports no blocker, major, or minor findings. If the author believes a finding is wrong, they do not skip it silently: they report it to the user with reasoning, and the user decides.

## Finding format
```
[severity] path/to/file.ts:LINE  short title
Why it matters: concrete failure (input/state -> wrong output/crash), not a vague worry.
Fix: the specific change to make.
```

## Checklists

### Correctness
- Does the code do what the commit message and tests claim? Check edge values: empty, one, max, boundary timestamps, negative, NaN, unicode.
- Off-by-one at window boundaries: windows are `[start, end)`; an event at exactly `end` belongs to the next window.
- Time: no `Date.now()` or `Math.random()` in domain code; use the injected `Clock` and seeded RNG. Event time and processing time are never mixed up.
- Integer/float handling for counts, versions, and offsets (offsets are bigint or string, never lossy numbers).

### Failure modes (name what happens in each)
- **Crash** mid-batch, mid-checkpoint, mid-flush: is state recoverable and is replay safe?
- **Duplicate delivery**: same event twice, same batch twice. Is the effect idempotent?
- **Late and out-of-order data**: within allowed lateness, beyond it, future timestamps.
- **Overload**: bounded queues, bounded retries, bounded memory. Is there a `429`/`503`/backpressure-pause path instead of unbounded growth? (A pause must never be the only way to clear a condition that depends on progress, such as window closing.)
- **Dependency down** (Kafka, ES): timeouts set, retries capped with backoff and jitter, no retry storms, no infinite loops, clear degraded behavior.
- **Bad input**: validated at the boundary with zod; poison messages go to the DLQ and never block the partition.
- **Partition/rebalance**: state released and restored correctly; no processing after assignment is lost; checkpoint writes are fenced (`if_seq_no`/`if_primary_term` plus generation) so a stale owner cannot overwrite a new owner; checkpoint `_id` is `{service}:{partition}`, and a bare partition `_id` fails review because aggregator and alert-evaluator would overwrite each other.
- **Watermarks**: monotonic, per partition, persisted in the checkpoint, capped by `receivedAt` (the idle rule is the one exception: it uses the injected clock); future timestamps are rejected at ingest, never clamped; the idle/quiet-partition rule is clock-injected, applies only when the consumer is caught up, not paused, and has no fetched-but-unprocessed messages (a merely stalled partition is never idle), stays off after a restore until caught up once, and is called out as the documented determinism limit.
- **Restore**: open windows are re-emitted after restore; the final `closed` record is never throttled away or left unflushed before a checkpoint.
- **Caps and pauses**: the only pause is output backpressure; no pause may depend on window closing to clear (it stops the watermark and deadlocks the partition). The watermark tracker observes every event, including ones about to be rejected, before any cap check. Check order is watermark (any fetched message counts as activity for the idle rule: duplicates, too-late, rejected, poison), too-late, seen-set (duplicates are dropped silently), then caps. Open-window and dedup-cap overflow goes to `events.rejected`, acked before the checkpoint, never skipped silently (`dedup_cap` increments the window's `rejectedCount`; `window_cap` has no window and is metric plus topic only). Tests: the partition recovers with a cap full of silent series and only new-series traffic; a cap-sized checkpoint writes and restores within the size budget and round-trips `rejectedCount`.
- **Aggregates topic and alerts**: records use explicit partition = input partition (a test asserts it); the evaluator evaluates closed windows only, persists each series' last-evaluated start in its checkpoint, ignores any closed window at or before it, counts a gap of more than one window as non-breach, and relies on the aggregator re-emitting finalized windows in start order; per-series evaluator entries expire after 1 h of event time, closed windows more than 1 h behind the partition's newest closed window are ignored, and both are tested; alerts reach the UI by polling ES, never over SSE.
- **Live path (query-api)**: the window map evicts by window start (30 min) before admitting, caps series (200 per tenant, 15,625 per instance) with at most 32 windows each, so entries are hard-bounded (a test with adversarial input shows entries never exceed 6,400 per tenant or 500k per instance); it sheds only never-seen series (no entry in the map; a slot is freed when the series' last window is evicted, so no ever-seen set may exist) and sets `partial` per tenant for 30 min after any shed (lifetime tested; carried on `/v1/series`, the stream's snapshot-meta event, and every heartbeat; a diff missing any carrier fails); live endpoints (not `/readyz`) return 503 + `Retry-After` until caught up and while lag exceeds 30 s; `TenantScopedWindowCache` has the same omission test as `TenantScopedSearch`; snapshot + delta uses the revision guard and ignores deltas outside the snapshot range.
- **SSE connections**: key re-checked every 60 s and the stream closed on revoke, per-key connection cap, bounded per-client buffer with disconnect on overflow.

### Idempotency and delivery guarantees
- Writes use deterministic, **tenant-scoped** IDs (`tenantId:eventId`) and the revision rule from the design (`(count + rejectedCount)*2 + closed`, strictly increasing). A 409 is success only when the stored doc is at least as complete as the one being written (`op_type: create` exists-already, or external version conflict); any other 409 is an error. Final `closed` records must still win.
- Offsets are committed only after the work they cover is durable. Aggregator order is emit, flush and await acks, checkpoint, commit; reordering it is a **blocker**.
- No unrecoverable dual write: if a step writes to two systems, a crash between them must be repaired by idempotent replay, and the diff must show how.

### Concurrency, lifecycle, shutdown
- No unhandled promise rejections; every async call is awaited or deliberately detached with an error handler.
- `start()`/`stop()` are idempotent. Shutdown stops intake, drains in-flight work, flushes producers, commits, then disconnects. Timers and listeners are cleared.
- No shared mutable state across requests or batches without a clear owner.

### Architecture rules
- Domain classes import no Kafka or ES clients. Services import no other services; shared code is in `packages/*`.
- Inheritance only from lifecycle base classes, one level deep; variation lives in injected collaborators behind interfaces.
- Business logic is not in routers, React components, or `main.ts`.
- New cross-service decision? There must be an ADR and a Decision log line.

### Security
- No secrets, keys, or tokens in code, tests, logs, or Docker images. Config is validated env.
- `tenantId` comes only from the authenticated key; every read path goes through the tenant-scoped query layer; event and dedup IDs are tenant-scoped. Route scopes (`ingest` vs `query`) are checked per route.
- Keys and `Authorization` headers are never logged (redaction tested); SSE auth uses the header, never a URL parameter.
- Identifier fields (`tenantId`, `env`, `service`, rule ids) match `^[a-z0-9-]{1,64}$`, and a schema test rejects `:` so composite IDs stay unambiguous.
- Search input uses `simple_query_string`; limits (page size, time range, body size, batch size) are enforced.
- No user input in shell commands, file paths, index names, or raw queries. Errors do not leak internals (RFC 9457 problem responses with a request ID).
- Dependencies: new ones are justified, maintained, and pinned.

### Observability
- Structured pino logs with request/event/trace IDs; no `console.log`. No PII or full payloads at info level.
- Metrics or counters for the things that page you: consumer lag, DLQ count, late-event count, dropped events, retry count.
- `/healthz` (process up) and `/readyz` (dependencies ready) stay truthful.

### Tests
- Coverage is 100% on lines, branches, functions, and statements. Every exclusion has a reason comment and prior user approval; an unexplained one is a **blocker**. Any edit to coverage thresholds, `include`/`exclude` patterns, Stryker thresholds, or test `retry` settings is a **blocker** unless the user approved it.
- No `.skip`, `.only`, `.todo`, or `xit` tests left in the diff.
- Each test asserts behavior, including failure paths. Look for tests that execute code without asserting on its result, over-mocking that hides the real contract, and snapshots standing in for assertions.
- Domain logic has a test written first and, where it is order- or duplicate-sensitive, a property-based test (fast-check).
- Tests are deterministic: no real time, no sleeps, no randomness without a seed, no ordering dependence between tests.
- Integration tests use real Kafka/ES (Testcontainers), wait on conditions with timeouts, and clean up after themselves.
- Mutation testing (Stryker, score ≥ 85%) on domain packages: surviving mutants in changed code are findings.

### TypeScript hygiene
- No `any`, `as` casts that lie (including `as unknown as`), non-null `!`, `@ts-ignore`, `@ts-expect-error`, `@ts-nocheck`, `eslint-disable`, `v8`/`c8`/`istanbul ignore`, or `// Stryker disable` without user approval and a reason.
- `strict`, `noUncheckedIndexedAccess`, and `exactOptionalPropertyTypes` stay on. Types are derived from zod schemas, not duplicated.
- Errors are typed (`PoisonMessageError`, etc.), not thrown as strings; error causes are preserved.

### React and UI
- Server state through TanStack Query; no business logic in components; effects clean up (SSE connection closed on unmount, reconnect handled).
- Accessible: labels, roles, keyboard use, contrast, no information by color alone. Loading, empty, error, and stale-data states exist.
- Large lists are virtualized or paginated; charts degrade gracefully on gaps and late corrections.

### Docker and config
- Multi-stage builds, pinned base image tags, non-root user, no dev dependencies in the final image, `.dockerignore` present.
- Every new env var is validated at boot and documented. Compose healthchecks and `depends_on` conditions are correct.

### Docs-only diffs (ADRs, design, README, CLAUDE.md)
- Do the files agree with each other (stack, partition key, ID scheme, watermark, output path, rules, thresholds)? Search for the same term in all four docs.
- Are design claims sound? For any correctness claim (exactly-once, ordering, idempotency, versioning), construct the failure scenario and check the doc handles it.
- Is anything stale (planning-session language, removed features, wrong script names)? Are enforcement claims true yet, or marked as planned?
- A cross-service decision has an ADR and a dated Decision log line.

### Docs and process
- `README.md` is accurate for any changed behavior or run instructions. The project `CLAUDE.md` Status and Decision log are current.
- Commit is small, uses Conventional Commits, and its body summarizes the review (found / fixed / flagged).
