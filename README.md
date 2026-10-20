# Observability platform (real-time analytics pipeline)

A mini Datadog/Sentry built to explore stream processing end to end. Apps send logs and events to an ingest API; Kafka buffers them; stream processors compute windowed aggregates (counts, error rates, latency percentiles) and alerts; Elasticsearch serves full-text search and aggregates; a React dashboard shows it live.

**Status:** design approved, implementation not started. See [`docs/design.md`](docs/design.md) for the architecture, tradeoffs, and capacity numbers, and [`CODE_REVIEW.md`](CODE_REVIEW.md) for the review standard.

## What it demonstrates
- Event time vs processing time, per-partition watermarks, late-event correction
- Idempotent sinks and what "exactly-once" really means (checkpointed state, deterministic IDs, versioned writes)
- Partitioning, hot keys, rebalancing, backpressure, and DLQs
- Elasticsearch mapping design and tenant-isolated search

## Stack
TypeScript, Node, Koa, React + Vite, Kafka, Elasticsearch, Docker Compose.

## How to run
Not available yet. Phase 1 adds `docker compose up`.
