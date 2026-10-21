# ADR-0003: Kafka client: @confluentinc/kafka-javascript behind our port

- Status: accepted
- Date: 2026-10-20
- Decision log: 2026-10-20, "Kafka client"

## Context
Every service talks to Kafka, so the client is a cross-service decision (rule 6). The design (Tooling flag) recommended `@confluentinc/kafka-javascript` over `kafkajs`, pending a Phase 1 check of maintenance, throughput, and API fit. The design depends on these capabilities:
- **`assign()` with no consumer group:** query-api's live map.
- **Offsets by timestamp:** query-api starts at now − 30 min.
- **High-water marks:** the aggregator's idle rule (position == high-water mark).
- **`pause`/`resume`:** output backpressure.
- **`seek`:** restore from the checkpoint offset.
- **Rebalance callbacks:** fenced checkpoint claims.
- **Idempotent producer with `acks=all`:** ingest's `202` means durable.
- **Explicit partitions:** aggregates go to the input partition number.

## Measurements (2026-10-20)
Harness: `bench/kafka-client`, run against the compose broker (Kafka 4.3.2, single KRaft node, 12-partition topic per run).
- **Variants:** three clients, run interleaved (A, B, C, A, B, C, ...), each after a discarded warmup.
- **Throughput:** 1,000,000 messages of 500 B, keyed `tenant:service` like `events.raw`. A pool of 200 pre-built batches is sent in rotation, so nothing is built inside the timer and the live heap stays small. (Earlier versions held every message in memory, which slowed kafkajs's JS encoder through garbage collection: by about 25% in throughput, and by about 6 ms at p99 in the latency phase. The fresh-context review caught both.) Sent in batches of 500 (ingest's max batch) with 16 batches in flight, `acks=all`, idempotent.
- **Latency at design load:** open loop, 10 batches of 500 per second (5k msg/s, the design's sustained target) for 60 s, giving 600 samples per run, sent from a 20-batch pool in rotation. Reports p50/p99 of each batch's ack time, which is what ingest's `202` waits for.
- **Consume:** everything produced, read back with a consumer group. Throughput is timed from the first to the last batch; group-join time is reported separately.
- **Runs:** 3 per variant.

Machine: Apple M1 Max (10 cores, 64 GB), Docker Desktop, Node 24.22.0. Reproduce with `docker compose up -d --wait kafka` and then `pnpm --filter @obs/bench-kafka-client bench`.

| Variant (runs 1 / 2 / 3) | Produce msg/s | Ack p50 / p99 at 5k msg/s | Consume msg/s | Group join |
|---|---|---|---|---|
| confluent 1.11.0, librdkafka defaults | 246k / 254k / 253k | 15.4/31.5, 16.1/34.9, 16.6/32.7 ms | 251k / 261k / 281k | ~360 ms |
| confluent, `linger.ms=0` | 245k / 241k / 247k | 12.0/29.1, 12.1/32.0, 11.5/29.6 ms | 263k / 252k / 276k | 345–390 ms |
| kafkajs 2.2.4 | 87k / 91k / 89k | 13.1/25.6, 11.6/25.7, 12.8/27.7 ms | 244k / 281k / 279k | ~90 ms |

Reading the numbers:
- **Throughput:** confluent produces about 2.8× faster under load (idempotent, `acks=all`). kafkajs's idempotent producer is only safe with one request in flight, which caps it. Turning idempotence off would break ingest's durability contract, so that cap counts against it.
- **Latency at design load:**
  - With librdkafka's default `linger.ms` (5 ms), confluent's median is 2–4.5 ms higher than kafkajs's (about 3.5 ms on average).
  - With `linger.ms=0`, its median is level with kafkajs (within about 1 ms either way), at no measurable throughput cost.
  - kafkajs has the better tail: its p99 is 2–6 ms lower in all three runs (about 26–28 ms vs 29–32 ms). For ingest that difference sits inside a request whose budget is set by HTTP and the network, not by these milliseconds. So the tail is noted but it is not decisive.
- **Consume:** similar (within run-to-run noise). Both are far above the design's 2–5k msg/s per consumer, so this does not decide anything. confluent's group join is slower (~350 ms vs ~90 ms), a one-time cost per rebalance.

## Maintenance and fit
These rows were checked against the installed type definitions and sources.

| | confluent | kafkajs |
|---|---|---|
| Last release | 1.11.0 on 2026-10-07 (frequent releases) | 2.2.4 on 2023-02-27 |
| Repository | pushed 2026-10-18, 115 open issues | pushed 2024-08-02, 419 open issues |
| Node 24 | prebuilt binary (darwin-arm64 verified; CI loads it on linux) | runs, but emits `TimeoutNegativeWarning` on Node 24 |
| **Group-less `assign()`** | **yes**: native `KafkaConsumer.assign` (the KafkaJS-compatible consumer has only `assignment()`) | **no**: `assign` is a group partition-assigner only |
| Watermarks, offsets by timestamp | yes: admin `fetchTopicOffsets` and `fetchTopicOffsetsByTimestamp`; native per-partition `queryWatermarkOffsets` and `offsetsForTimes` | yes: admin `fetchTopicOffsets` and `fetchTopicOffsetsByTimestamp`, batch `highWatermark` |
| Rebalance callbacks | yes: `rebalance_cb` (native and KafkaJS-compatible) | group join and rebalance events |
| pause / resume / seek, idempotent producer, explicit partition | yes | yes |

The deciding gap is group-less `assign()`, which query-api's design requires. With kafkajs it would need a throwaway consumer group per instance, which adds group coordination, rebalances, and committed-offset noise to what is meant to be a plain reader.

## Decision
- **Client:** use `@confluentinc/kafka-javascript` 1.11.0, pinned. pnpm `allowBuilds` lets it fetch its prebuilt native binary.
- **Isolation:** services never import it. `packages/kafka` defines our own ports (producer, consumer, admin) with offsets as strings, plus an in-memory fake. The adapter uses the KafkaJS-compatible API where it suffices, and the native API for group-less `assign` (query-api). Per-partition synchronous watermark queries for the idle rule also go native where that is simpler.
- **Partitioner:** producers set `partitioner: 'murmur2_random'` explicitly, the Java-compatible key hash, which matches kafkajs's DefaultPartitioner. The KafkaJS-compatible layer applies murmur2_random by default only when it is given `kafkaJS` options. The native producer defaults to `consistent_random` (CRC32), which would put a key on a different partition. Key → partition stability is what window locality rests on, so `packages/kafka` gets a contract test pinning key → partition for known keys.
- **Producer profiles:** latency-sensitive producers (ingest) set `linger.ms=0`. Throughput producers (aggregator output, DLQ) keep the default.

## Alternatives considered
- **kafkajs:**
  - About 2.8× slower to produce with idempotent `acks=all`.
  - Unmaintained since 2023, and warns on Node 24.
  - No group-less `assign`.
  - Its faster group join and slightly better p99 at design load do not outweigh these.
- **Platformatic Kafka and other pure-JS clients:** not evaluated in depth. The port in `packages/kafka` keeps a later switch contained to one adapter.

## Consequences
- **Supply chain of the native addon.** The allowed install script runs `node-pre-gyp install --fallback-to-build`:
  - It downloads a binary from the project's GitHub releases. The pnpm lockfile integrity covers the npm tarball, not that binary, and the download is not checksum-verified.
  - If the download fails, it silently compiles the bundled librdkafka from source, which gives a different binary.
  - `allowBuilds` matches by package name, so every future version inherits permission to run its install script.
  - Mitigations: the version is pinned exactly, upgrades are reviewed as dependency changes, and CI loads the addon on Linux after install. A checksum pin for the binary is the next step if this becomes a production deployment.
- **Images:** Docker images need a platform with prebuilt binaries (linux glibc x64/arm64) or a build toolchain for the fallback.
- **Two API styles:** the KafkaJS-compatible API and the native API are mixed inside one adapter, never in services. Contract tests run the port against both the real client (Testcontainers) and the fake, so their semantics stay pinned.
- **Revisit when:** the native binary becomes a deployment problem, or a maintained pure-JS client gains group-less assign.
