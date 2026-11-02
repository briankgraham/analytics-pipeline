import { beforeEach, describe, expect, it, vi } from 'vitest';
import { readLogsFromKafka, type HandleMessage } from '../../kafka/read-logs.ts';

// Stands in for kafkajs, keeping hold of the function the code gives it for each message.
const consumer = vi.hoisted(() => ({
  events: { CRASH: 'crash' },
  on: vi.fn(),
  connect: vi.fn(),
  subscribe: vi.fn(),
  run: vi.fn(),
  disconnect: vi.fn(),
}));

vi.mock('kafkajs', () => ({
  Kafka: class {
    consumer() {
      return consumer;
    }
  },
}));

// Starts reading with the given handler, then delivers one message the way kafkajs would.
async function deliver(handleMessage: HandleMessage, heartbeat: () => Promise<void>) {
  await readLogsFromKafka({
    brokers: ['127.0.0.1:9092'],
    topic: 'logs',
    group: 'worker',
    handleMessage,
    onGiveUp: () => {},
  });
  const { eachMessage } = consumer.run.mock.calls[0]?.[0];
  return eachMessage({
    partition: 2,
    message: { value: Buffer.from('hello'), offset: '7' },
    heartbeat,
  });
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('readLogsFromKafka', () => {
  it('hands over the text and where it sits, with no extra heartbeat when it works', async () => {
    const handleMessage = vi.fn(async () => {});
    const heartbeat = vi.fn(async () => {});
    await deliver(handleMessage, heartbeat);
    expect(handleMessage).toHaveBeenCalledWith('hello', 'logs partition 2 offset 7');
    expect(heartbeat).not.toHaveBeenCalled();
  });

  it('tells Kafka the worker is alive when handling fails, then passes the error on', async () => {
    const heartbeat = vi.fn(async () => {});
    const failing = async () => {
      throw new Error('too busy');
    };
    await expect(deliver(failing, heartbeat)).rejects.toThrow('too busy');
    expect(heartbeat).toHaveBeenCalledOnce();
  });

  it('passes on a failed heartbeat, because kafkajs uses it to rejoin the group', async () => {
    const heartbeat = async () => {
      throw new Error('the group is rebalancing');
    };
    const failing = async () => {
      throw new Error('too busy');
    };
    await expect(deliver(failing, heartbeat)).rejects.toThrow('the group is rebalancing');
  });
});
