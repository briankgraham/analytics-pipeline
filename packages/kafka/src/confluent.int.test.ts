import { afterAll, beforeAll, expect } from 'vitest';
import { ConfluentKafka } from './confluent.js';
import { kafkaContract } from './testing/contract.js';
import { startKafka, type StartedKafka } from './testing/kafka-container.js';

let kafka: StartedKafka;
let client: ConfluentKafka;
/** Client warnings and errors logged during the run (they must reach our logger, not the console). */
const logged: { level: string; message: string }[] = [];
const log = {
  warn: (_fields: Record<string, unknown>, message: string) =>
    logged.push({ level: 'warn', message }),
  error: (_fields: Record<string, unknown>, message: string) =>
    logged.push({ level: 'error', message }),
};

beforeAll(async () => {
  kafka = await startKafka();
  client = new ConfluentKafka({ brokers: kafka.brokers, clientId: 'contract-test', log });
});

afterAll(async () => {
  await kafka.stop();
  // A healthy run raises no client errors.
  expect(logged.filter((entry) => entry.level === 'error')).toEqual([]);
});

kafkaContract('confluent adapter on a real broker', () => ({
  createTopic: (topic, partitions) => kafka.createTopic(topic, partitions),
  producer: () => client.producer({ lingerMs: 0 }),
  consumer: (groupId) => client.consumer(groupId),
  pollTimeoutMs: 1_000,
}));
