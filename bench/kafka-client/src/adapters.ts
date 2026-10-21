import { KafkaJS as Confluent } from '@confluentinc/kafka-javascript';
import { performance } from 'node:perf_hooks';
import { Kafka as KafkaJs, logLevel } from 'kafkajs';

export interface BenchMessage {
  key: string;
  value: Buffer;
}

export interface ProducerHandle {
  /** Resolves when the broker has acknowledged every message (acks=all). */
  send(messages: readonly BenchMessage[]): Promise<void>;
  close(): Promise<void>;
}

/** When the first and the last expected message arrived (performance.now() times). */
export interface ConsumeTimes {
  firstBatchAt: number;
  lastBatchAt: number;
}

/** The surface the benchmark needs, implemented once per client library. */
export interface BenchClient {
  readonly name: string;
  createTopic(topic: string, partitions: number): Promise<void>;
  deleteTopic(topic: string): Promise<void>;
  producer(topic: string): Promise<ProducerHandle>;
  /** Consumes the topic from the beginning until `expected` messages arrived. */
  consumeAll(topic: string, groupId: string, expected: number): Promise<ConsumeTimes>;
}

/** Shared by both adapters: count messages from eachBatch until `expected`. */
function countUntil(expected: number): { add(n: number): void; done: Promise<ConsumeTimes> } {
  let seen = 0;
  let first = Number.NaN;
  let resolve: (times: ConsumeTimes) => void = () => undefined;
  const done = new Promise<ConsumeTimes>((r) => {
    resolve = r;
  });
  return {
    add(n) {
      const now = performance.now();
      if (seen === 0) first = now;
      seen += n;
      if (seen >= expected) resolve({ firstBatchAt: first, lastBatchAt: now });
    },
    done,
  };
}

/**
 * `lingerMs` is librdkafka's `linger.ms` (default 5): how long the producer waits to fill a batch.
 * Lower favours per-request latency (ingest's 202), higher favours throughput.
 */
export function confluentClient(brokers: string[], lingerMs?: number): BenchClient {
  const kafka = new Confluent.Kafka({
    kafkaJS: { brokers, clientId: 'bench-confluent', logLevel: Confluent.logLevel.ERROR },
  });
  return {
    name: lingerMs === undefined ? 'confluent' : `confluent-linger${String(lingerMs)}`,
    async createTopic(topic, partitions) {
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
      });
      await admin.disconnect();
    },
    async deleteTopic(topic) {
      const admin = kafka.admin();
      await admin.connect();
      await admin.deleteTopics({ topics: [topic] });
      await admin.disconnect();
    },
    async producer(topic) {
      const producer = kafka.producer({
        // Explicit, not relied on: the KafkaJS-compatible layer defaults to murmur2_random only
        // when given kafkaJS options; the native producer defaults to CRC32 (consistent_random).
        partitioner: 'murmur2_random',
        ...(lingerMs === undefined ? {} : { 'linger.ms': lingerMs }),
        kafkaJS: { idempotent: true, acks: -1 },
      });
      await producer.connect();
      return {
        async send(messages) {
          await producer.send({ topic, messages: [...messages] });
        },
        close: () => producer.disconnect(),
      };
    },
    async consumeAll(topic, groupId, expected) {
      const consumer = kafka.consumer({ kafkaJS: { groupId, fromBeginning: true } });
      await consumer.connect();
      await consumer.subscribe({ topic });
      const counter = countUntil(expected);
      await consumer.run({
        eachBatch: ({ batch }) => {
          counter.add(batch.messages.length);
          return Promise.resolve();
        },
      });
      const times = await counter.done;
      await consumer.disconnect();
      return times;
    },
  };
}

export function kafkajsClient(brokers: string[]): BenchClient {
  const kafka = new KafkaJs({ brokers, clientId: 'bench-kafkajs', logLevel: logLevel.ERROR });
  return {
    name: 'kafkajs',
    async createTopic(topic, partitions) {
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
      });
      await admin.disconnect();
    },
    async deleteTopic(topic) {
      const admin = kafka.admin();
      await admin.connect();
      await admin.deleteTopics({ topics: [topic] });
      await admin.disconnect();
    },
    async producer(topic) {
      // kafkajs's idempotent producer is only safe (sequence ordering) with one request in flight.
      const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
      await producer.connect();
      return {
        async send(messages) {
          await producer.send({ topic, acks: -1, messages: [...messages] });
        },
        close: () => producer.disconnect(),
      };
    },
    async consumeAll(topic, groupId, expected) {
      const consumer = kafka.consumer({ groupId });
      await consumer.connect();
      await consumer.subscribe({ topic, fromBeginning: true });
      const counter = countUntil(expected);
      await consumer.run({
        eachBatch: ({ batch }) => {
          counter.add(batch.messages.length);
          return Promise.resolve();
        },
      });
      const times = await counter.done;
      await consumer.disconnect();
      return times;
    },
  };
}
