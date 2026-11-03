import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KafkaClient } from '../../kafka/client.ts';
import type { WorkerSettings } from '../../worker/settings.ts';
import { Worker } from '../../worker/worker.ts';

// Stands in for both libraries, and records the order things happen in.
const { order, admin, consumer, producer, elastic } = vi.hoisted(() => {
  const order: string[] = [];
  const note = (step: string) => async () => void order.push(step);
  return {
    order,
    admin: {
      connect: async () => {},
      disconnect: async () => {},
      fetchTopicMetadata: vi.fn(note('failed topic checked')),
    },
    consumer: {
      events: { CRASH: 'crash', GROUP_JOIN: 'group-join' },
      on: vi.fn(),
      connect: async () => {},
      subscribe: async () => {},
      run: vi.fn(note('reading started')),
      disconnect: vi.fn(note('reading stopped')),
    },
    producer: {
      connect: vi.fn(note('kafka opened')),
      disconnect: vi.fn(note('kafka closed')),
    },
    elastic: {
      close: vi.fn(note('elasticsearch closed')),
      create: vi.fn(note('index set up')),
    },
  };
});

vi.mock('kafkajs', () => ({
  Partitioners: {},
  Kafka: class {
    admin() {
      return admin;
    }
    consumer() {
      return consumer;
    }
    producer() {
      return producer;
    }
  },
}));

vi.mock('@elastic/elasticsearch', () => ({
  errors: {
    ResponseError: class extends Error {},
    ConnectionError: class extends Error {},
    TimeoutError: class extends Error {},
    NoLivingConnectionsError: class extends Error {},
    ElasticsearchClientError: class extends Error {},
  },
  Client: class {
    indices = { create: elastic.create };
    close = elastic.close;
  },
}));

const settings: WorkerSettings = {
  kafkaBrokers: ['b:9092'],
  topic: 'logs',
  topicPartitions: 3,
  failedTopic: 'failed',
  group: 'worker',
  waitBeforeTryingAgainMs: 1000,
  moveToFailedAfterMs: 1000,
  alertAfterMs: 1000,
  elasticsearchUrl: 'http://127.0.0.1:9200',
  index: 'logs',
  replicas: 0,
  bulkSize: 2,
  requestTimeoutMs: 1000,
  retries: { maxRetries: 0, delayMs: 1, exponential: false, jitter: false },
};

const stopProgram = vi.fn();
let worker: Worker | undefined;

function getWorker() {
  worker = Worker.getInstance(settings, stopProgram);
  return worker;
}

beforeEach(() => {
  vi.clearAllMocks();
  order.length = 0;
  vi.spyOn(console, 'log').mockImplementation(() => {});
});

afterEach(async () => {
  await worker?.close();
  worker = undefined;
  vi.restoreAllMocks();
});

describe('Worker', () => {
  it('touches nothing until init', () => {
    getWorker();
    expect(order).toEqual([]);
  });

  it('has only one instance, and refuses different settings', () => {
    expect(getWorker()).toBe(getWorker());
    expect(() =>
      Worker.getInstance({ ...settings, waitBeforeTryingAgainMs: 2 }, stopProgram),
    ).toThrow('different settings');
    expect(() => Worker.getInstance(settings, () => {})).toThrow('different settings');
  });

  it('starts everything in order, with reading last', async () => {
    await getWorker().init();
    expect(order).toEqual([
      'index set up',
      'kafka opened',
      'failed topic checked',
      'reading started',
    ]);
  });

  it('starts once however many times init is called, even at the same time', async () => {
    const started = getWorker();
    await Promise.all([started.init(), started.init()]);
    await started.init();
    expect(consumer.run).toHaveBeenCalledOnce();
    expect(producer.connect).toHaveBeenCalledOnce();
  });

  it('closes in the opposite order', async () => {
    const started = getWorker();
    await started.init();
    order.length = 0;

    await started.close();
    await started.close();
    expect(order).toEqual(['reading stopped', 'kafka closed', 'elasticsearch closed']);
  });

  it('closes only what it started', async () => {
    elastic.create.mockRejectedValueOnce(new Error('not allowed'));
    const started = getWorker();
    await expect(started.init()).rejects.toThrow('not allowed');
    expect(consumer.run).not.toHaveBeenCalled();

    const closeKafka = vi.spyOn(KafkaClient.prototype, 'close');
    await started.close();
    expect(elastic.close).toHaveBeenCalledOnce();
    expect(closeKafka).not.toHaveBeenCalled();
  });

  it('closes a client that only partly started', async () => {
    producer.connect.mockRejectedValueOnce(new Error('Kafka is down'));
    const started = getWorker();
    await expect(started.init()).rejects.toThrow('Kafka is down');

    await started.close();
    expect(producer.disconnect).toHaveBeenCalledOnce();
    expect(elastic.close).toHaveBeenCalledOnce();
  });

  it('still closes the others when one fails to close, then reports it', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const started = getWorker();
    await started.init();
    producer.disconnect.mockRejectedValueOnce(new Error('stuck'));

    await expect(started.close()).rejects.toThrow('not everything closed cleanly');
    expect(elastic.close).toHaveBeenCalledOnce();
    worker = undefined;
  });

  it('does not start reading if it closed during start-up', async () => {
    let finishSetUp: () => void = () => {};
    elastic.create.mockImplementationOnce(
      () => new Promise<undefined>((resolve) => (finishSetUp = () => resolve(undefined))),
    );
    const started = getWorker();
    const starting = started.init();
    await vi.waitFor(() => expect(elastic.create).toHaveBeenCalled());
    const closing = started.close();
    finishSetUp();

    await expect(starting).rejects.toThrow('is closed');
    await closing;
    expect(producer.connect).not.toHaveBeenCalled();
    expect(consumer.run).not.toHaveBeenCalled();
  });

  it('asks the program to stop when reading from Kafka stops for good', async () => {
    await getWorker().init();
    const [, onCrash] = consumer.on.mock.calls.find(([event]) => event === 'crash') ?? [];
    const error = new Error('gave up');
    onCrash({ payload: { restart: false, error } });
    expect(stopProgram).toHaveBeenCalledWith(error);
  });

  it('refuses to start after close, and gives a new instance', async () => {
    const started = getWorker();
    await started.close();
    await expect(started.init()).rejects.toThrow('is closed');
    expect(getWorker()).not.toBe(started);
  });
});
