// A real single-node Kafka for integration tests (Testcontainers), configured like
// docker-compose.yml. @testcontainers/kafka targets Confluent Platform images, so this uses
// GenericContainer with the same apache/kafka image the compose stack pins.
import { createServer, type AddressInfo } from 'node:net';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';

export const KAFKA_IMAGE =
  'apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7';

/** The TCP port of a listening server's address (a pipe or unbound server has none). */
export function portOf(address: AddressInfo | string | null): number {
  if (typeof address !== 'object' || address === null) {
    throw new Error(`expected a TCP address, got ${JSON.stringify(address)}`);
  }
  return address.port;
}

/** A free local port, so the advertised listener can be known before the container starts. */
export function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const port = portOf(server.address());
      server.close(() => {
        resolve(port);
      });
    });
  });
}

export interface StartedKafka {
  readonly brokers: string[];
  createTopic(topic: string, partitions: number): Promise<void>;
  stop(): Promise<void>;
}

/** Runs `attempt` up to `attempts` times, returning the first success or the last failure. */
export async function retry<T>(attempts: number, attempt: () => Promise<T>): Promise<T> {
  let failure: unknown = new Error('retry needs at least one attempt');
  for (let i = 0; i < attempts; i++) {
    try {
      return await attempt();
    } catch (error) {
      failure = error;
    }
  }
  throw failure;
}

/**
 * Starts the broker. The free port is released before Docker binds it, so another process can take
 * it in between ("port is already allocated"); a fresh port per attempt makes that a retry.
 */
export function startKafka(): Promise<StartedKafka> {
  return retry(3, startOnce);
}

async function startOnce(): Promise<StartedKafka> {
  const port = await freePort();
  const container: StartedTestContainer = await new GenericContainer(KAFKA_IMAGE)
    .withExposedPorts({ container: 9092, host: port })
    .withEnvironment({
      KAFKA_NODE_ID: '1',
      KAFKA_PROCESS_ROLES: 'broker,controller',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:29093',
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_LISTENERS: 'INTERNAL://:29092,CONTROLLER://:29093,EXTERNAL://:9092',
      KAFKA_ADVERTISED_LISTENERS: `INTERNAL://localhost:29092,EXTERNAL://127.0.0.1:${String(port)}`,
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
        'CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT',
      KAFKA_INTER_BROKER_LISTENER_NAME: 'INTERNAL',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
    })
    .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
    .withStartupTimeout(120_000)
    .start();

  const brokers = [`127.0.0.1:${String(port)}`];
  const admin = new KafkaJS.Kafka({
    kafkaJS: { brokers, clientId: 'test-admin', logLevel: KafkaJS.logLevel.ERROR },
  }).admin();
  await admin.connect();

  return {
    brokers,
    async createTopic(topic, partitions) {
      await admin.createTopics({
        topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
      });
    },
    async stop() {
      await admin.disconnect();
      await container.stop();
    },
  };
}
