// Kafka client benchmark (ADR-0003). Needs the compose broker: docker compose up -d --wait kafka
//   node bench/kafka-client/src/run.main.ts [--client all|confluent|kafkajs] [--messages N]
//                                           [--runs N] [--brokers host:port,...]
// Each client first does a discarded warmup; then runs interleave clients (A, B, A, B, ...) so
// neither always pays for a cold broker. Per run:
//   1. Throughput: N messages of 500 B (keyed like events.raw) in batches of 500 (ingest's max
//      batch), 16 batches in flight, acks=all, idempotent. A pool of 200 pre-built batches is
//      sent in rotation: nothing is built inside the timer, and the live heap stays small (holding
//      all N messages taxes the JS-encoding client's GC and skews the comparison).
//      With a fixed in-flight count, ack latency here is just throughput restated, so it is not
//      reported for this phase.
//   2. Latency at design load: open-loop 10 batches/s of 500 (5k msg/s, the design's sustained
//      target) for 60 s (600 samples); p50/p99 of each batch's ack time, what ingest's 202 waits
//      for. Skipped in the warmup.
//   3. Consume: everything produced, back with a consumer group; throughput timed from the first
//      to the last batch, the group-join time reported separately.
// Prints one JSON line per run and a summary table.
import { cpus, totalmem } from 'node:os';
import { performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { confluentClient, kafkajsClient, type BenchClient, type BenchMessage } from './adapters.ts';

const MESSAGE_BYTES = 500;
const BATCH = 500;
const IN_FLIGHT = 16;
const PARTITIONS = 12;
const LOAD_BATCHES_PER_SEC = 10;
const LOAD_SECONDS = 60;
const POOL_BATCHES = 200;
const LOAD_POOL_BATCHES = 20;
const WARMUP_MESSAGES = 50_000;
const CONSUME_TIMEOUT_MS = 300_000;

const { values } = parseArgs({
  options: {
    client: { type: 'string', default: 'all' },
    messages: { type: 'string', default: '1000000' },
    runs: { type: 'string', default: '3' },
    brokers: { type: 'string', default: '127.0.0.1:9092' },
  },
});
const total = Number(values.messages);
const runs = Number(values.runs);
const brokers = values.brokers.split(',');
const clients: BenchClient[] = [];
// confluent with librdkafka defaults, confluent tuned for latency (linger.ms 0), and kafkajs.
if (values.client !== 'kafkajs')
  clients.push(confluentClient(brokers), confluentClient(brokers, 0));
if (values.client !== 'confluent') clients.push(kafkajsClient(brokers));

function batches(count: number): BenchMessage[][] {
  return Array.from({ length: Math.ceil(count / BATCH) }, (_, b) =>
    Array.from({ length: Math.min(BATCH, count - b * BATCH) }, (_, j) => {
      const i = b * BATCH + j;
      const value = Buffer.alloc(MESSAGE_BYTES, 'x');
      value.write(`{"i":${String(i)}}`);
      return { key: `tenant-${String(i % 50)}:service-${String(i % 7)}`, value };
    }),
  );
}

/** Nearest-rank percentile of an ascending array. */
function percentile(sorted: readonly number[], q: number): number {
  return sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)] ?? Number.NaN;
}

async function throughput(client: BenchClient, topic: string, count: number) {
  const pool = batches(Math.min(count, POOL_BATCHES * BATCH));
  const sends = Math.ceil(count / BATCH);
  const producer = await client.producer(topic);
  let next = 0;
  const started = performance.now();
  await Promise.all(
    Array.from({ length: IN_FLIGHT }, async () => {
      for (let b = next++; b < sends; b = next++) {
        const batch = pool[b % pool.length];
        if (batch !== undefined) await producer.send(batch);
      }
    }),
  );
  const ms = performance.now() - started;
  await producer.close();
  return ms;
}

/** Open-loop: batch k is sent at k / rate seconds whether or not earlier ones were acked. */
async function latencyAtLoad(client: BenchClient, topic: string): Promise<number[]> {
  const producer = await client.producer(topic);
  // A small pool in rotation, like throughput(): holding all 600 batches skews the GC tail.
  const pool = batches(LOAD_POOL_BATCHES * BATCH);
  const sends = Array.from({ length: LOAD_BATCHES_PER_SEC * LOAD_SECONDS }, (_, k) => k);
  const started = performance.now();
  const acks = await Promise.all(
    sends.map(async (k) => {
      const batch = pool[k % pool.length] ?? [];
      await sleep(Math.max(0, started + (k * 1000) / LOAD_BATCHES_PER_SEC - performance.now()));
      const sent = performance.now();
      await producer.send(batch);
      return performance.now() - sent;
    }),
  );
  await producer.close();
  return acks.sort((a, b) => a - b);
}

async function consume(client: BenchClient, topic: string, expected: number) {
  const started = performance.now();
  const timeout = sleep(CONSUME_TIMEOUT_MS).then(() => {
    throw new Error(`consume timed out after ${String(CONSUME_TIMEOUT_MS)} ms`);
  });
  const times = await Promise.race([client.consumeAll(topic, `${topic}.group`, expected), timeout]);
  return { joinMs: times.firstBatchAt - started, ms: times.lastBatchAt - times.firstBatchAt };
}

async function run(client: BenchClient, label: string, count: number, withLoad: boolean) {
  const topic = `bench.${client.name}.${label}.${String(Date.now())}`;
  await client.createTopic(topic, PARTITIONS);
  const produceMs = await throughput(client, topic, count);
  const acks = withLoad ? await latencyAtLoad(client, topic) : [];
  const consumedCount = count + acks.length * BATCH;
  const consumed = await consume(client, topic, consumedCount);
  await client.deleteTopic(topic);
  return {
    client: client.name,
    run: label,
    produceMsgPerSec: Math.round(count / (produceMs / 1000)),
    produceMBPerSec: +((count * MESSAGE_BYTES) / 2 ** 20 / (produceMs / 1000)).toFixed(1),
    loadAckP50Ms: +percentile(acks, 0.5).toFixed(1),
    loadAckP99Ms: +percentile(acks, 0.99).toFixed(1),
    loadSamples: acks.length,
    consumeMsgPerSec: Math.round(consumedCount / (consumed.ms / 1000)),
    groupJoinMs: Math.round(consumed.joinMs),
  };
}

const write = (line: string): boolean => process.stdout.write(`${line}\n`);
write(
  JSON.stringify({
    machine: {
      cpus: cpus().length,
      cpu: cpus()[0]?.model,
      memGb: Math.round(totalmem() / 2 ** 30),
    },
    node: process.version,
    total,
    runs,
    messageBytes: MESSAGE_BYTES,
    batch: BATCH,
    inFlight: IN_FLIGHT,
    load: `${String(LOAD_BATCHES_PER_SEC * BATCH)} msg/s for ${String(LOAD_SECONDS)} s`,
  }),
);

for (const client of clients) await run(client, 'warmup', WARMUP_MESSAGES, false);

const summary: string[] = [];
for (let r = 1; r <= runs; r++) {
  for (const client of clients) {
    const result = await run(client, String(r), total, true);
    write(JSON.stringify(result));
    summary.push(
      `${result.client.padEnd(10)} run ${result.run}  produce ${String(result.produceMsgPerSec).padStart(8)} msg/s ` +
        `(${String(result.produceMBPerSec)} MB/s)  ack@5k/s p50 ${String(result.loadAckP50Ms)} ms ` +
        `p99 ${String(result.loadAckP99Ms)} ms  consume ${String(result.consumeMsgPerSec).padStart(8)} msg/s ` +
        `(join ${String(result.groupJoinMs)} ms)`,
    );
  }
}
write(summary.join('\n'));
