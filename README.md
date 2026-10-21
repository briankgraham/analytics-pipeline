# Observability platform (real-time analytics pipeline)

A mini Datadog/Sentry built to explore stream processing end to end. Apps send logs and events to an ingest API; Kafka buffers them; stream processors compute windowed aggregates (counts, error rates, latency percentiles) and alerts; Elasticsearch serves full-text search and aggregates; a React dashboard shows it live.

**Status:** Phase 1 (skeleton) in progress: toolchain, `pnpm verify`, commit gates, shared packages (schemas, config, logger), and the local Kafka + Elasticsearch stack are in place. See [`docs/design.md`](docs/design.md) for the architecture, tradeoffs, and capacity numbers, and [`CODE_REVIEW.md`](CODE_REVIEW.md) for the review standard.

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

In an existing checkout, run `pnpm install --force` once so the husky git hooks are installed (pnpm skips `prepare` on an up-to-date install).

Property tests (fast-check) run from a fixed seed; `FC_SEED=<n> pnpm test` explores another one.

`pnpm verify` is the single quality gate; individual steps are `pnpm format:check`, `pnpm lint`, `pnpm typecheck`, `pnpm deps`, `pnpm check:suppressions`, `pnpm test`, and `pnpm build`.

### Local stack (Kafka + Elasticsearch)
```sh
docker compose up -d --wait kafka elasticsearch   # healthy brokers and search
docker compose up kafka-init                      # create/converge topics from infra/kafka/topics.conf
pnpm compose:smoke                                # or do both and verify topics, configs, and health
docker compose down -v                            # stop and delete all local data
```

| Service | Host address | In-network address |
|---|---|---|
| Kafka (KRaft, single node) | `127.0.0.1:9092` | `kafka:29092` |
| Elasticsearch (single node) | `http://127.0.0.1:9200` | `http://elasticsearch:9200` |

Topics, partition counts, and configs (retention, segment size, compaction) are declared once in `infra/kafka/topics.conf`; the init converges each topic to exactly that config set, a test keeps it in sync with the code and the design, and topic auto-creation is disabled. Partition counts are never changed in place (that remaps keys): to change one, `docker compose down -v`. Ports bind to 127.0.0.1 only. Local Elasticsearch runs **without security** (no TLS, no auth) and Kafka uses plaintext listeners: fine for a laptop, never for a shared host. The stack needs about 3 GB of Docker memory (Elasticsearch heap 1 GB, container limit 2 GB).

### Commit gates
Every commit passes `scripts/verify-gate.sh`, which refuses commits on `main` and requires the working tree to match the index. It then runs `pnpm verify`, at most once per tree. A `commit-msg` hook requires a Conventional Commits header and a `Review:` line. Commit by staging everything and then running `git commit -F <message-file>`. CI runs the same checks on every pull request. See [ADR-0002](docs/adr/0002-commit-gates.md).
