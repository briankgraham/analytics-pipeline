import { beforeEach, describe, expect, it, vi } from 'vitest';
import { connectToFailedTopic, splitBySize } from '../../kafka/send-failed-logs.ts';

// Stands in for kafkajs, so each path can be checked without a running Kafka. Real Kafka was checked
// by hand: asking about a missing topic throws UNKNOWN_TOPIC_OR_PARTITION and doesn't create it.
const { admin, producer, ProtocolError } = vi.hoisted(() => ({
  admin: { connect: vi.fn(), disconnect: vi.fn(), fetchTopicMetadata: vi.fn() },
  producer: { connect: vi.fn(), disconnect: vi.fn(), send: vi.fn() },
  ProtocolError: class extends Error {
    type = 'UNKNOWN_TOPIC_OR_PARTITION';
  },
}));

vi.mock('kafkajs', () => ({
  Partitioners: {},
  Kafka: class {
    admin() {
      return admin;
    }
    producer() {
      return producer;
    }
  },
}));

const log = { service: 'checkout', level: 'error', message: 'card declined' } as const;

beforeEach(() => {
  vi.resetAllMocks();
});

describe('connectToFailedTopic', () => {
  it('stops with a clear message when the topic is missing', async () => {
    admin.fetchTopicMetadata.mockRejectedValue(new ProtocolError('unknown'));
    await expect(connectToFailedTopic(['b:9092'], 'failed')).rejects.toThrow('pnpm topics');
    expect(admin.disconnect).toHaveBeenCalledOnce();
    expect(producer.connect).not.toHaveBeenCalled();
  });

  it('passes on any other problem as it is', async () => {
    admin.fetchTopicMetadata.mockRejectedValue(new Error('network down'));
    await expect(connectToFailedTopic(['b:9092'], 'failed')).rejects.toThrow('network down');
    expect(admin.disconnect).toHaveBeenCalledOnce();
  });

  it('sends a big group of logs in pieces small enough for Kafka', async () => {
    const { sendFailedLogs } = await connectToFailedTopic(['b:9092'], 'failed');
    const big = { ...log, message: 'x'.repeat(10_000) };
    await sendFailedLogs(
      Array.from({ length: 200 }, (_, i) => ({ id: `logs-0-${i}`, log: big, reason: 'too busy' })),
    );

    const sends = producer.send.mock.calls.map(([sent]) => sent.messages);
    expect(sends.length).toBeGreaterThan(1);
    expect(sends.flat()).toHaveLength(200);
    for (const messages of sends) {
      const bytes = messages.reduce(
        (total: number, { value }: { value: string }) => total + value.length,
        0,
      );
      expect(bytes).toBeLessThanOrEqual(500_000);
    }
  });

  it('passes on a failed send, so the logs stay where they are', async () => {
    const { sendFailedLogs } = await connectToFailedTopic(['b:9092'], 'failed');
    producer.send.mockRejectedValue(new Error('Kafka is down'));
    await expect(sendFailedLogs([{ id: 'logs-0-1', log, reason: 'x' }])).rejects.toThrow(
      'Kafka is down',
    );
  });
});

describe('splitBySize', () => {
  it('keeps every message, in order, within the limit', () => {
    expect(splitBySize(['aaa', 'bbb', 'c', 'dd'], 4)).toEqual([['aaa'], ['bbb', 'c'], ['dd']]);
  });

  it('gives an oversized message a list of its own', () => {
    expect(splitBySize(['a', 'bbbbbb', 'c'], 3)).toEqual([['a'], ['bbbbbb'], ['c']]);
  });

  it('counts bytes, not characters', () => {
    expect(splitBySize(['éé', 'éé'], 4)).toEqual([['éé'], ['éé']]);
  });

  it('returns nothing for nothing', () => {
    expect(splitBySize([], 4)).toEqual([]);
  });
});
