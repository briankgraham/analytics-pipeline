import { describe, expect, it, vi } from 'vitest';
import type { KafkaCalls } from '../../kafka/client.ts';
import { createFailedLogSender, splitBySize } from '../../kafka/send-failed-logs.ts';

const log = { service: 'checkout', level: 'error', message: 'card declined' } as const;

function standInClient() {
  return {
    sendMessages: vi.fn<KafkaCalls['sendMessages']>(async () => {}),
    topicExists: vi.fn<KafkaCalls['topicExists']>(async () => true),
  };
}

describe('createFailedLogSender', () => {
  it('stops with a clear message when the topic is missing', async () => {
    const client = standInClient();
    client.topicExists.mockResolvedValue(false);
    await expect(createFailedLogSender(client, 'failed')).rejects.toThrow('pnpm topics');
  });

  it('passes on any other problem as it is', async () => {
    const client = standInClient();
    client.topicExists.mockRejectedValue(new Error('network down'));
    await expect(createFailedLogSender(client, 'failed')).rejects.toThrow('network down');
  });

  it('sends a big group of logs in pieces small enough for Kafka', async () => {
    const client = standInClient();
    const sendFailedLogs = await createFailedLogSender(client, 'failed');
    const big = { ...log, message: 'x'.repeat(10_000) };
    await sendFailedLogs(
      Array.from({ length: 200 }, (_, i) => ({ id: `logs-0-${i}`, log: big, reason: 'too busy' })),
    );

    const sends = client.sendMessages.mock.calls.map(([, messages]) => messages);
    expect(sends.length).toBeGreaterThan(1);
    expect(sends.flat()).toHaveLength(200);
    for (const messages of sends) {
      const bytes = messages.reduce((total, message) => total + message.length, 0);
      expect(bytes).toBeLessThanOrEqual(500_000);
    }
  });

  it('passes on a failed send, so the logs stay where they are', async () => {
    const client = standInClient();
    const sendFailedLogs = await createFailedLogSender(client, 'failed');
    client.sendMessages.mockRejectedValue(new Error('Kafka is down'));
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
