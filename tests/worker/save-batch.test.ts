import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { LogToSave, SaveAnswer } from '../../elasticsearch/save-logs.ts';
import type { MessageBatch } from '../../kafka/read-logs.ts';
import type { FailedLog } from '../../kafka/send-failed-logs.ts';
import { createBatchSaver } from '../../worker/save-batch.ts';

const good = { service: 'checkout', level: 'error', message: 'card declined' } as const;
const dayMs = 24 * 60 * 60 * 1000;
const options = {
  bulkSize: 2,
  moveStuckLogAfterRounds: 3,
  moveToFailedAfterMs: 6 * dayMs,
  alertAfterMs: 600_000,
};

let sentGroups: string[][];
let failedLogs: FailedLog[];
let doneOffsets: string[];
// Says which of the logs in each request stay unsaved, and which are refused as bad.
let unsavedIds: string[];
let badIds: string[];
let sendFailedLogs: (logs: FailedLog[]) => Promise<void>;
let shouldStop: () => boolean;

const saveLogs = async (logs: LogToSave[]): Promise<SaveAnswer> => {
  sentGroups.push(logs.map(({ id }) => id));
  const unsaved = logs.filter(({ id }) => unsavedIds.includes(id));
  const bad = logs.filter(({ id }) => badIds.includes(id));
  return {
    savedCount: logs.length - unsaved.length - bad.length,
    badLogs: bad.map(({ id }) => ({ id, reason: 'wrong shape' })),
    unsavedLogs: unsaved.map(({ id }) => ({ id, reason: '429: too busy' })),
  };
};

function batchSaver() {
  return createBatchSaver({ saveLogs, sendFailedLogs: (logs) => sendFailedLogs(logs), ...options });
}

function saver() {
  return batchSaver().handleBatch;
}

// A message with the id m<offset>. Its text is a good log unless given.
function message(
  offset: number,
  text: string | null = JSON.stringify(good),
  timestampMs = Date.now(),
) {
  return { id: `m${offset}`, offset: String(offset), text, timestampMs };
}

function batchOf(...messages: MessageBatch['messages']): MessageBatch {
  return {
    partition: 2,
    messages,
    keepAlive: async () => {},
    markDone: (offset) => doneOffsets.push(offset),
    shouldStop: () => shouldStop(),
  };
}

beforeEach(() => {
  sentGroups = [];
  failedLogs = [];
  doneOffsets = [];
  unsavedIds = [];
  badIds = [];
  shouldStop = () => false;
  sendFailedLogs = async (logs) => {
    failedLogs.push(...logs);
  };
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('saving a batch', () => {
  it('saves in groups of bulkSize and marks everything done', async () => {
    const answer = await saver()(batchOf(message(1), message(2), message(3)));
    expect(answer).toBe('finished');
    expect(sentGroups).toEqual([['m1', 'm2'], ['m3']]);
    expect(doneOffsets.at(-1)).toBe('3');
    expect(failedLogs).toEqual([]);
  });

  it('saves only the fields of a log', async () => {
    const logs: LogToSave[] = [];
    const { handleBatch: save } = createBatchSaver({
      ...options,
      sendFailedLogs,
      saveLogs: async (sent) => {
        logs.push(...sent);
        return { savedCount: sent.length, badLogs: [], unsavedLogs: [] };
      },
    });
    await save(batchOf(message(1, JSON.stringify({ ...good, extra: 'ignored' }))));
    expect(logs).toEqual([{ id: 'm1', log: good }]);
  });

  it('stops early when asked, leaving the rest to be read again', async () => {
    shouldStop = () => sentGroups.length === 1;
    const answer = await saver()(batchOf(message(1), message(2), message(3)));
    expect(answer).toBe('finished');
    expect(sentGroups).toHaveLength(1);
    expect(doneOffsets).toEqual(['2']);
  });
});

describe('bad data', () => {
  it.each([
    ['empty', null, 'it is empty'],
    ['not JSON', '{"service":', 'it is not JSON'],
    ['not a log', JSON.stringify({ ...good, level: 'loud' }), 'level must be one of'],
    ['a JSON string', '"hello"', 'a log must be a JSON object'],
  ])('skips a message that is %s with a warning, never storing it', async (_name, text, why) => {
    const answer = await saver()(batchOf(message(1, text), message(2)));
    expect(answer).toBe('finished');
    expect(sentGroups).toEqual([['m2']]);
    expect(failedLogs).toEqual([]);
    expect(console.warn).toHaveBeenCalledOnce();
    expect(vi.mocked(console.warn).mock.calls[0]?.[0]).toContain('m1');
    expect(vi.mocked(console.warn).mock.calls[0]?.[0]).toContain(why);
  });

  it('marks a group of only bad messages done without calling Elasticsearch', async () => {
    await saver()(batchOf(message(1, 'nope')));
    expect(sentGroups).toEqual([]);
    expect(doneOffsets).toEqual(['1']);
  });

  it('warns about a log Elasticsearch refuses with a 400, and moves on without storing it', async () => {
    badIds = ['m1'];
    const answer = await saver()(batchOf(message(1), message(2)));
    expect(answer).toBe('finished');
    expect(doneOffsets).toEqual(['2']);
    expect(failedLogs).toEqual([]);
    const warning = vi.mocked(console.warn).mock.calls[0]?.[0];
    expect(warning).toContain('wrong shape');
    expect(warning).toContain('card declined');
  });

  it('prints at most 1,000 characters of a bad message', async () => {
    await saver()(batchOf(message(1, 'x'.repeat(5000))));
    const warning = String(vi.mocked(console.warn).mock.calls[0]?.[0]);
    expect(warning).toContain('x'.repeat(1000));
    expect(warning).not.toContain('x'.repeat(1001));
  });
});

describe('logs that are not saved yet', () => {
  it('marks done only up to the first unsaved log, then pauses', async () => {
    unsavedIds = ['m3'];
    const answer = await saver()(batchOf(message(1), message(2), message(3), message(4)));
    expect(answer).toBe('pause');
    expect(doneOffsets).toEqual(['2']);
  });

  it('marks nothing done when the first log is the unsaved one', async () => {
    unsavedIds = ['m1'];
    expect(await saver()(batchOf(message(1), message(2)))).toBe('pause');
    expect(doneOffsets).toEqual([]);
  });

  it('marks nothing done when bad data comes after the unsaved log', async () => {
    unsavedIds = ['m1'];
    await saver()(batchOf(message(1), message(2, 'nope')));
    expect(doneOffsets).toEqual([]);
  });

  it('counts bad data before the first unsaved log as done', async () => {
    unsavedIds = ['m2'];
    await saver()(batchOf(message(1, 'nope'), message(2)));
    expect(doneOffsets).toEqual(['1']);
  });
});

describe('moving a stuck log', () => {
  it('moves a log that fails enough rounds while others save, then finishes', async () => {
    unsavedIds = ['m1'];
    const save = saver();
    const batch = batchOf(message(1), message(2));
    expect(await save(batch)).toBe('pause');
    expect(await save(batch)).toBe('pause');
    expect(failedLogs).toEqual([]);

    expect(await save(batch)).toBe('finished');
    expect(failedLogs).toHaveLength(1);
    expect(failedLogs[0]).toMatchObject({ id: 'm1', log: good });
    expect(failedLogs[0]?.reason).toContain('429: too busy');
    expect(doneOffsets).toEqual(['2']);
  });

  it('does not count a round where nothing saved, because that means Elasticsearch is the problem', async () => {
    unsavedIds = ['m1', 'm2'];
    const save = saver();
    const batch = batchOf(message(1), message(2));
    for (let round = 0; round < 5; round++) {
      expect(await save(batch)).toBe('pause');
    }
    expect(failedLogs).toEqual([]);
  });

  it('starts counting again when a log saves, because the rounds must be in a row', async () => {
    const save = saver();
    const batch = batchOf(message(1), message(2));
    unsavedIds = ['m1'];
    await save(batch);
    await save(batch);
    // m1 saves this round, while m2 fails, so the partition stays in trouble.
    unsavedIds = ['m2'];
    await save(batch);
    unsavedIds = ['m1'];
    await save(batch);
    await save(batch);
    expect(failedLogs).toEqual([]);
  });

  it('remembers a moved log behind an unsaved one: not resent, not moved twice', async () => {
    const save = saver();
    const old = batchOf(message(1), message(2, JSON.stringify(good), Date.now() - 6 * dayMs));
    unsavedIds = ['m1', 'm2'];
    expect(await save(old)).toBe('pause');
    expect(failedLogs.map(({ id }) => id)).toEqual(['m2']);
    expect(doneOffsets).toEqual([]);

    sentGroups = [];
    expect(await save(old)).toBe('pause');
    expect(sentGroups).toEqual([['m1']]);
    expect(failedLogs).toHaveLength(1);

    unsavedIds = [];
    expect(await save(old)).toBe('finished');
    expect(doneOffsets).toEqual(['2']);
  });

  it('does not mistake a new message for a moved log just because it reuses the id', async () => {
    const save = saver();
    const movedAt = Date.now() - 6 * dayMs;
    unsavedIds = ['m1', 'm2'];
    await save(batchOf(message(1), message(2, JSON.stringify(good), movedAt)));
    expect(failedLogs.map(({ id }) => id)).toEqual(['m2']);

    sentGroups = [];
    await save(batchOf(message(1), message(2, JSON.stringify(good), movedAt + 1)));
    expect(sentGroups).toEqual([['m1', 'm2']]);
  });

  it('marks a moved log done when it comes before the unsaved one', async () => {
    unsavedIds = ['m1', 'm2'];
    const old = message(1, JSON.stringify(good), Date.now() - 6 * dayMs);
    expect(await saver()(batchOf(old, message(2)))).toBe('pause');
    expect(failedLogs.map(({ id }) => id)).toEqual(['m1']);
    expect(doneOffsets).toEqual(['1']);
  });

  it('starts counting again after a batch finishes', async () => {
    const save = saver();
    unsavedIds = ['m1'];
    await save(batchOf(message(1), message(2)));
    await save(batchOf(message(1), message(2)));
    unsavedIds = [];
    await save(batchOf(message(1), message(2)));
    unsavedIds = ['m1'];
    await save(batchOf(message(1), message(2)));
    expect(failedLogs).toEqual([]);
  });

  it('keeps the partition paused, with nothing lost, if the failed topic cannot be written', async () => {
    unsavedIds = ['m1'];
    sendFailedLogs = async () => {
      throw new Error('Kafka is down');
    };
    const save = saver();
    const batch = batchOf(message(1), message(2));
    await save(batch);
    await save(batch);
    expect(await save(batch)).toBe('pause');
    expect(doneOffsets).toEqual([]);
  });
});

describe('moving a log before Kafka deletes it', () => {
  it('moves an old log that still cannot be saved, even when nothing else saves', async () => {
    unsavedIds = ['m1'];
    const old = message(1, JSON.stringify(good), Date.now() - 6 * dayMs);
    expect(await saver()(batchOf(old))).toBe('finished');
    expect(failedLogs).toHaveLength(1);
    expect(failedLogs[0]?.reason).toContain('Kafka will delete it soon');
    expect(doneOffsets).toEqual(['1']);
  });

  it('saves an old log normally when Elasticsearch takes it', async () => {
    const old = message(1, JSON.stringify(good), Date.now() - 6 * dayMs);
    await saver()(batchOf(old));
    expect(failedLogs).toEqual([]);
  });

  it('leaves a younger log waiting', async () => {
    unsavedIds = ['m1'];
    const young = message(1, JSON.stringify(good), Date.now() - 6 * dayMs + 60_000);
    expect(await saver()(batchOf(young))).toBe('pause');
    expect(failedLogs).toEqual([]);
  });
});

describe('alerting', () => {
  const alerts = () =>
    vi.mocked(console.error).mock.calls.filter(([text]) => String(text).includes('ALERT'));

  it('alerts at each pause once a partition has been in trouble long enough', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    unsavedIds = ['m1'];
    const save = saver();
    const batch = batchOf(message(1));

    await save(batch);
    vi.advanceTimersByTime(options.alertAfterMs - 1);
    await save(batch);
    expect(alerts()).toHaveLength(0);

    vi.advanceTimersByTime(1);
    await save(batch);
    expect(alerts()).toHaveLength(1);
    expect(alerts()[0]?.[0]).toContain('partition 2');
    await save(batch);
    expect(alerts()).toHaveLength(2);
  });

  it('keeps timing through any wait, however long, while the partition stays with this worker', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    unsavedIds = ['m1'];
    const { handleBatch, keepOnlyPartitions } = batchSaver();
    await handleBatch(batchOf(message(1)));
    vi.advanceTimersByTime(options.alertAfterMs * 100);
    keepOnlyPartitions([2]);
    await handleBatch(batchOf(message(1)));
    expect(alerts()).toHaveLength(1);
  });

  it('starts timing again if the partition went to another worker and came back', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    unsavedIds = ['m1'];
    const { handleBatch, keepOnlyPartitions } = batchSaver();
    await handleBatch(batchOf(message(1)));
    vi.advanceTimersByTime(options.alertAfterMs);
    keepOnlyPartitions([0, 1]);
    await handleBatch(batchOf(message(1)));
    expect(alerts()).toHaveLength(0);
  });

  it('starts timing again after a batch finishes', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    const save = saver();
    unsavedIds = ['m1'];
    await save(batchOf(message(1)));
    vi.advanceTimersByTime(options.alertAfterMs);
    unsavedIds = [];
    await save(batchOf(message(1)));
    unsavedIds = ['m1'];
    await save(batchOf(message(1)));
    expect(alerts()).toHaveLength(0);
  });
});
