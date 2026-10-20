# Observability platform (real-time analytics pipeline)

A mini Datadog/Sentry built to explore stream processing end to end. Apps send logs and events to an ingest API; Kafka buffers them; stream processors compute windowed aggregates (counts, error rates, latency percentiles) and alerts; Elasticsearch serves full-text search and aggregates; a React dashboard shows it live.

**Status:** Phase 1 (skeleton) in progress: toolchain and `pnpm verify` are in place. See [`docs/design.md`](docs/design.md) for the architecture, tradeoffs, and capacity numbers, and [`CODE_REVIEW.md`](CODE_REVIEW.md) for the review standard.

## What it demonstrates
- Event time vs processing time, per-partition watermarks, late-event correction
- Idempotent sinks and what "exactly-once" really means (checkpointed state, deterministic IDs, versioned writes)
- Partitioning, hot keys, rebalancing, backpressure, and DLQs
- Elasticsearch mapping design and tenant-isolated search

## Stack
TypeScript, Node, Koa, React + Vite, Kafka, Elasticsearch, Docker Compose.

## How to run
Prerequisites: [nvm](https://github.com/nvm-sh/nvm) (or any Node manager that reads `.nvmrc`) and Docker.

```sh
nvm install && nvm use   # Node 24.22.0 from .nvmrc
corepack enable          # pnpm 12.14.0 from package.json "packageManager"
pnpm install
pnpm verify              # format, lint, typecheck, dependency rules, suppressions, tests (100% coverage), build
```

`pnpm verify` is the single quality gate; individual steps are `pnpm format:check`, `pnpm lint`, `pnpm typecheck`, `pnpm deps`, `pnpm check:suppressions`, `pnpm test`, and `pnpm build`. `docker compose up` (Kafka + Elasticsearch) arrives later in Phase 1.
