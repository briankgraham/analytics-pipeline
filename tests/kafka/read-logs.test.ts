import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { readLogsFromKafka, type HandleBatch } from '../../kafka/read-logs.ts';

// Stands in for kafkajs, keeping hold of the function the code gives it for each batch.
const consumer = vi.hoisted(() => ({
  events: { CRASH: 'crash', GROUP_JOIN: 'group-join' },
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

const resume = vi.fn();
const pause = vi.fn(() => resume);
const resolveOffset = vi.fn();
const heartbeat = vi.fn(async () => {});
const isRunning = vi.fn(() => true);
const isStale = vi.fn(() => false);
const onPartitionsAssigned = vi.fn();

// Starts reading with the given handler, then delivers one batch the way kafkajs would.
async function deliver(handleBatch: HandleBatch) {
  const reader = await readLogsFromKafka({
    brokers: ['127.0.0.1:9092'],
    topic: 'logs',
    group: 'worker',
    pauseMs: 120_000,
    handleBatch,
    onPartitionsAssigned,
    onGiveUp: () => {},
  });
  const { eachBatch } = consumer.run.mock.calls[0]?.[0];
  await eachBatch({
    batch: {
      partition: 2,
      messages: [
        { value: Buffer.from('hello'), offset: '7', timestamp: '1000' },
        { value: null, offset: '8', timestamp: '2000' },
      ],
    },
    resolveOffset,
    heartbeat,
    pause,
    isRunning,
    isStale,
  });
  return reader;
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.useFakeTimers();
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  isRunning.mockReturnValue(true);
  isStale.mockReturnValue(false);
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('readLogsFromKafka', () => {
  it('leaves marking messages done to the handler', async () => {
    await deliver(async () => 'finished');
    expect(consumer.run.mock.calls[0]?.[0].eachBatchAutoResolve).toBe(false);
    expect(resolveOffset).not.toHaveBeenCalled();
  });

  it('hands over the messages in plain form', async () => {
    const handleBatch = vi.fn<HandleBatch>(async () => 'finished');
    await deliver(handleBatch);
    const batch = handleBatch.mock.calls[0]?.[0];
    expect(batch?.partition).toBe(2);
    expect(batch?.messages).toEqual([
      { id: 'logs-2-7', offset: '7', text: 'hello', timestampMs: 1000 },
      { id: 'logs-2-8', offset: '8', text: null, timestampMs: 2000 },
    ]);
  });

  it('connects markDone and keepAlive to kafkajs', async () => {
    await deliver(async (batch) => {
      batch.markDone('7');
      await batch.keepAlive();
      return 'finished';
    });
    expect(resolveOffset).toHaveBeenCalledWith('7');
    expect(heartbeat).toHaveBeenCalledOnce();
  });

  it.each([
    [true, false, false],
    [false, false, true],
    [true, true, true],
  ])('with isRunning %s and isStale %s, shouldStop is %s', async (running, stale, stop) => {
    isRunning.mockReturnValue(running);
    isStale.mockReturnValue(stale);
    await deliver(async (batch) => {
      expect(batch.shouldStop()).toBe(stop);
      return 'finished';
    });
  });

  it('does not pause after a batch that finished', async () => {
    await deliver(async () => 'finished');
    expect(pause).not.toHaveBeenCalled();
  });

  it('pauses the partition when asked, and resumes after pauseMs', async () => {
    await deliver(async () => 'pause');
    expect(pause).toHaveBeenCalledOnce();

    vi.advanceTimersByTime(119_999);
    expect(resume).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(resume).toHaveBeenCalledOnce();
  });

  it('does not resume after the reader has closed', async () => {
    const reader = await deliver(async () => 'pause');
    await reader.close();
    vi.advanceTimersByTime(120_000);
    expect(resume).not.toHaveBeenCalled();
    expect(consumer.disconnect).toHaveBeenCalledOnce();
  });

  it('does not pause when the reader closed while the handler was working', async () => {
    await deliver(async () => {
      isRunning.mockReturnValue(false);
      return 'pause';
    });
    expect(pause).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('says which partitions this worker reads after joining the group', async () => {
    await deliver(async () => 'finished');
    const [, onJoin] = consumer.on.mock.calls.find(([event]) => event === 'group-join') ?? [];
    onJoin({ payload: { memberAssignment: { logs: [0, 2] } } });
    expect(onPartitionsAssigned).toHaveBeenLastCalledWith([0, 2]);
    onJoin({ payload: { memberAssignment: {} } });
    expect(onPartitionsAssigned).toHaveBeenLastCalledWith([]);
  });

  it('passes on an unexpected error, because kafkajs uses it to rejoin the group', async () => {
    await expect(
      deliver(async () => {
        throw new Error('the group is rebalancing');
      }),
    ).rejects.toThrow('the group is rebalancing');
  });
});
