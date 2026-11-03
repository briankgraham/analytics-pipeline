import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { LogToSave, SaveAnswer } from '../../elasticsearch/save-logs.ts';
import type { MessageBatch } from '../../kafka/client.ts';
import type { FailedLog } from '../../kafka/send-failed-logs.ts';
import { createBatchSaver } from '../../worker/save-batch.ts';

const good = { service: 'checkout', level: 'error', message: 'card declined' } as const;
const dayMs = 24 * 60 * 60 * 1000;
const options = {
  bulkSize: 2,
  waitBeforeTryingAgainMs: 1,
  moveToFailedAfterMs: 6 * dayMs,
  alertAfterMs: 600_000,
};

let sentGroups: string[][];
let failedLogs: FailedLog[];
let doneOffsets: string[];
// Says which of the logs in each request stay unsaved.
let unsavedIds: string[];
// Says which of the logs in each request Elasticsearch answers 400 for.
let badRequestIds: string[];
let sendFailedLogs: (logs: FailedLog[]) => Promise<void>;
let shouldStop: () => boolean;
let keepAliveCalls: number;

const saveLogs = async (logs: LogToSave[]): Promise<SaveAnswer> => {
  sentGroups.push(logs.map(({ id }) => id));
  const unsaved = logs.filter(({ id }) => unsavedIds.includes(id));
  const bad = logs.filter(({ id }) => badRequestIds.includes(id));
  return {
    savedCount: logs.length - unsaved.length - bad.length,
    unsavedLogs: unsaved.map(({ id }) => ({ id, reason: '429: too busy' })),
    badRequestLogs: bad.map(({ id }) => ({ id, reason: '400: bad field' })),
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
    keepAlive: async () => {
      keepAliveCalls += 1;
    },
    markDone: (offset) => doneOffsets.push(offset),
    shouldStop: () => shouldStop(),
  };
}

beforeEach(() => {
  sentGroups = [];
  failedLogs = [];
  doneOffsets = [];
  unsavedIds = [];
  badRequestIds = [];
  shouldStop = () => false;
  keepAliveCalls = 0;
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
  it('moves a log that got a 400 to the failed topic at once, and carries on past it', async () => {
    badRequestIds = ['m2'];
    await saver()(batchOf(message(1), message(2), message(3)));
    expect(failedLogs).toEqual([
      { id: 'm2', log: good, reason: 'Elasticsearch refused it: 400: bad field' },
    ]);
    expect(sentGroups).toEqual([['m1', 'm2'], ['m3']]);
    expect(doneOffsets.at(-1)).toBe('3');
  });

  it('waits, leaving the log in Kafka, if the failed topic cannot be written to', async () => {
    badRequestIds = ['m1'];
    sendFailedLogs = async () => {
      throw new Error('kafka is down');
    };
    await saver()(batchOf(message(1), message(2)));
    expect(doneOffsets).toEqual([]);
    expect(failedLogs).toEqual([]);
  });

  it('moves the 400 log once the failed topic works again, instead of skipping it', async () => {
    badRequestIds = ['m1'];
    let working = false;
    sendFailedLogs = async (logs) => {
      if (!working) {
        throw new Error('kafka is down');
      }
      failedLogs.push(...logs);
    };
    const save = saver();
    const batch = batchOf(message(1), message(2));
    await save(batch);
    expect(doneOffsets).toEqual([]);
    working = true;
    await save(batch);
    expect(doneOffsets).toEqual(['2']);
    expect(failedLogs.map(({ id }) => id)).toEqual(['m1']);
  });

  it('marks a 400 log done and waits on the unsaved log after it, then does not move it twice', async () => {
    badRequestIds = ['m1'];
    unsavedIds = ['m2'];
    const save = saver();
    const batch = batchOf(message(1), message(2));
    await save(batch);
    expect(doneOffsets).toEqual(['1']);
    expect(failedLogs.map(({ id }) => id)).toEqual(['m1']);

    // Kafka hands over the same messages again, so the same batch is used. Building new messages
    // would stamp them with a new Date.now(), and a changed timestamp means a different message.
    await save(batch);
    expect(failedLogs.map(({ id }) => id)).toEqual(['m1']);
    expect(sentGroups.at(-1)).toEqual(['m2']);
  });

  it('keeps both a 400 log and an unsaved log in Kafka if the failed topic cannot be written to', async () => {
    badRequestIds = ['m1'];
    unsavedIds = ['m2'];
    sendFailedLogs = async () => {
      throw new Error('kafka is down');
    };
    await saver()(batchOf(message(1), message(2), message(3)));
    expect(doneOffsets).toEqual([]);
    expect(failedLogs).toEqual([]);
  });

  it('saves in groups of bulkSize and marks everything done', async () => {
    await saver()(batchOf(message(1), message(2), message(3)));
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
        return { savedCount: sent.length, unsavedLogs: [], badRequestLogs: [] };
      },
    });
    await save(batchOf(message(1, JSON.stringify({ ...good, extra: 'ignored' }))));
    expect(logs).toEqual([{ id: 'm1', log: good }]);
  });

  it('stops early when asked, leaving the rest to be read again', async () => {
    shouldStop = () => sentGroups.length === 1;
    await saver()(batchOf(message(1), message(2), message(3)));
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
    await saver()(batchOf(message(1, text), message(2)));
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

  it('prints at most 1,000 characters of a bad message', async () => {
    await saver()(batchOf(message(1, 'x'.repeat(5000))));
    const warning = String(vi.mocked(console.warn).mock.calls[0]?.[0]);
    expect(warning).toContain('x'.repeat(1000));
    expect(warning).not.toContain('x'.repeat(1001));
  });
});

describe('logs that are not saved yet', () => {
  it('marks done only up to the first unsaved log when nothing in its request saved', async () => {
    unsavedIds = ['m3', 'm4'];
    await saver()(batchOf(message(1), message(2), message(3), message(4)));
    expect(doneOffsets).toEqual(['2']);
    expect(failedLogs).toEqual([]);
  });

  it('marks nothing done when the first log is unsaved and nothing else saved', async () => {
    unsavedIds = ['m1', 'm2'];
    await saver()(batchOf(message(1), message(2)));
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
  it('moves at once a log that failed after the retries while others in its request saved', async () => {
    unsavedIds = ['m1'];
    await saver()(batchOf(message(1), message(2)));
    expect(failedLogs).toHaveLength(1);
    expect(failedLogs[0]).toMatchObject({ id: 'm1', log: good });
    expect(failedLogs[0]?.reason).toContain('429: too busy');
    expect(failedLogs[0]?.reason).toContain('while it saved other logs');
    expect(doneOffsets).toEqual(['2']);
  });

  it('does not hold up the logs behind a stuck one, and never waits', async () => {
    unsavedIds = ['m2'];
    await saver()(batchOf(message(1), message(2), message(3), message(4)));
    expect(sentGroups).toEqual([
      ['m1', 'm2'],
      ['m3', 'm4'],
    ]);
    expect(failedLogs.map(({ id }) => id)).toEqual(['m2']);
    expect(doneOffsets.at(-1)).toBe('4');
    // One word to Kafka before writing the stuck log to the failed topic, and one after each group.
    expect(keepAliveCalls).toBe(3);
  });

  it('does not move logs when nothing in their request saved, because Elasticsearch is the problem', async () => {
    unsavedIds = ['m1', 'm2'];
    const save = saver();
    const batch = batchOf(message(1), message(2));
    for (let round = 0; round < 5; round++) {
      await save(batch);
    }
    expect(failedLogs).toEqual([]);
    expect(doneOffsets).toEqual([]);
  });

  it('moves a log that was waiting once a newer log saves beside it', async () => {
    const save = saver();
    unsavedIds = ['m1'];
    await save(batchOf(message(1)));
    expect(failedLogs).toEqual([]);

    // Kafka hands over the unsaved log again together with a newer one.
    await save(batchOf(message(1), message(2)));
    expect(failedLogs.map(({ id }) => id)).toEqual(['m1']);
    expect(doneOffsets).toEqual(['2']);
  });

  it('remembers a moved log behind an unsaved one: not resent, not moved twice', async () => {
    const save = saver();
    const old = batchOf(message(1), message(2, JSON.stringify(good), Date.now() - 6 * dayMs));
    unsavedIds = ['m1', 'm2'];
    await save(old);
    expect(failedLogs.map(({ id }) => id)).toEqual(['m2']);
    expect(doneOffsets).toEqual([]);

    sentGroups = [];
    await save(old);
    expect(sentGroups).toEqual([['m1']]);
    expect(failedLogs).toHaveLength(1);

    unsavedIds = [];
    await save(old);
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
    await saver()(batchOf(old, message(2)));
    expect(failedLogs.map(({ id }) => id)).toEqual(['m1']);
    expect(doneOffsets).toEqual(['1']);
  });

  it('keeps a stuck log in Kafka, with nothing lost, if the failed topic cannot be written', async () => {
    unsavedIds = ['m1'];
    sendFailedLogs = async () => {
      throw new Error('Kafka is down');
    };
    await saver()(batchOf(message(1), message(2)));
    expect(doneOffsets).toEqual([]);
    expect(failedLogs).toEqual([]);
  });
});

describe('waiting', () => {
  it('waits for waitBeforeTryingAgainMs, speaking to Kafka after every second', async () => {
    vi.useFakeTimers();
    unsavedIds = ['m1', 'm2'];
    const save = createBatchSaver({
      saveLogs,
      sendFailedLogs,
      ...options,
      waitBeforeTryingAgainMs: 2_500,
    }).handleBatch;
    let finished = false;
    const running = save(batchOf(message(1), message(2))).then(() => {
      finished = true;
    });

    await vi.advanceTimersByTimeAsync(2_499);
    expect(finished).toBe(false);
    expect(keepAliveCalls).toBe(2);

    await vi.advanceTimersByTimeAsync(1);
    await running;
    expect(finished).toBe(true);
    expect(keepAliveCalls).toBe(3);
  });

  it('stops waiting within a second when the worker is shutting down', async () => {
    vi.useFakeTimers();
    unsavedIds = ['m1', 'm2'];
    const save = createBatchSaver({
      saveLogs,
      sendFailedLogs,
      ...options,
      waitBeforeTryingAgainMs: 600_000,
    }).handleBatch;
    const running = save(batchOf(message(1), message(2)));
    await vi.advanceTimersByTimeAsync(3_000);
    shouldStop = () => true;
    await vi.advanceTimersByTimeAsync(1_000);
    await running;
    expect(keepAliveCalls).toBe(4);
  });
});

describe('moving a log before Kafka deletes it', () => {
  it('moves an old log that still cannot be saved, even when nothing else saves', async () => {
    unsavedIds = ['m1'];
    const old = message(1, JSON.stringify(good), Date.now() - 6 * dayMs);
    await saver()(batchOf(old));
    expect(failedLogs).toHaveLength(1);
    expect(failedLogs[0]?.reason).toContain('Kafka will delete it soon');
    expect(doneOffsets).toEqual(['1']);
  });

  it('gives the age as the reason when a log is both old and stuck beside a log that saved', async () => {
    unsavedIds = ['m1'];
    const old = message(1, JSON.stringify(good), Date.now() - 6 * dayMs);
    await saver()(batchOf(old, message(2)));
    expect(failedLogs[0]?.reason).toContain('Kafka will delete it soon');
  });

  it('saves an old log normally when Elasticsearch takes it', async () => {
    const old = message(1, JSON.stringify(good), Date.now() - 6 * dayMs);
    await saver()(batchOf(old));
    expect(failedLogs).toEqual([]);
  });

  it('leaves a younger log waiting', async () => {
    unsavedIds = ['m1'];
    const young = message(1, JSON.stringify(good), Date.now() - 6 * dayMs + 60_000);
    await saver()(batchOf(young));
    expect(failedLogs).toEqual([]);
    expect(doneOffsets).toEqual([]);
  });
});

describe('alerting', () => {
  const alerts = () =>
    vi.mocked(console.error).mock.calls.filter(([text]) => String(text).includes('ALERT'));

  it('alerts at each wait once a partition has been in trouble long enough', async () => {
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
