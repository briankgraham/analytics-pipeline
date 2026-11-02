import { errors } from '@elastic/elasticsearch';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  DocumentAnswer,
  DocumentToSave,
  ElasticsearchCalls,
} from '../../elasticsearch/client.ts';
import { createLogSaver } from '../../elasticsearch/save-logs.ts';

const log = { service: 'checkout', level: 'error', message: 'card declined' } as const;
const logs = ['a', 'b', 'c'].map((id) => ({ id, log }));
const retries = { maxRetries: 2, delayMs: 1, exponential: true, jitter: false };
const keepAlive = async () => {};

// Elasticsearch can't be reached: an error that can pass with time.
const unreachable = () => new errors.ConnectionError('no answer');
// An error from Elasticsearch that needs a person.
const notAllowed = () => new errors.ConfigurationError('not allowed');

// Stands in for the base layer. The ids sent in each request, and what's still to happen: an Error is
// thrown for the whole request, and a list of ids means those documents are refused. Once empty, every
// document saves.
let sentIds: string[][];
let answers: (string[] | Error)[];
let saveDocuments: ReturnType<typeof vi.fn<ElasticsearchCalls['saveDocuments']>>;

beforeEach(() => {
  sentIds = [];
  answers = [];
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  saveDocuments = vi.fn(
    async (_index: string, documents: DocumentToSave[]): Promise<DocumentAnswer[]> => {
      sentIds.push(documents.map(({ id }) => id));
      const answer = answers.shift() ?? [];
      if (answer instanceof Error) {
        throw answer;
      }
      return documents.map(({ id }) =>
        answer.includes(id) ? { id, saved: false, reason: `429: no ${id}` } : { id, saved: true },
      );
    },
  );
});

afterEach(() => {
  vi.restoreAllMocks();
});

function saver() {
  const client: ElasticsearchCalls = {
    createIndex: vi.fn(),
    readIndexFields: vi.fn(),
    saveDocuments,
  };
  return createLogSaver(client, 'logs', retries);
}

describe('createLogSaver', () => {
  it('sends every log in one request to the given index, using the given ids', async () => {
    await expect(saver()(logs, keepAlive)).resolves.toEqual({ savedCount: 3, unsavedLogs: [] });
    expect(sentIds).toEqual([['a', 'b', 'c']]);
    expect(saveDocuments.mock.calls[0]?.[0]).toBe('logs');
    expect(saveDocuments.mock.calls[0]?.[1][0]).toEqual({ id: 'a', document: log });
  });

  it('retries only the logs that were not saved', async () => {
    answers = [['b']];
    await expect(saver()(logs, keepAlive)).resolves.toEqual({ savedCount: 3, unsavedLogs: [] });
    expect(sentIds).toEqual([['a', 'b', 'c'], ['b']]);
  });

  it('reports a log that keeps being refused as not saved, and says why', async () => {
    answers = [['b'], ['b'], ['b']];
    await expect(saver()(logs, keepAlive)).resolves.toEqual({
      savedCount: 2,
      unsavedLogs: [{ id: 'b', reason: '429: no b' }],
    });
    expect(sentIds).toHaveLength(3);
  });

  it('tries again after a whole request fails in a way that can pass', async () => {
    answers = [unreachable()];
    await expect(saver()(logs, keepAlive)).resolves.toEqual({ savedCount: 3, unsavedLogs: [] });
    expect(sentIds).toHaveLength(2);
  });

  it('reports every log as not saved when the whole request keeps failing', async () => {
    answers = [unreachable(), unreachable(), unreachable()];
    const { savedCount, unsavedLogs } = await saver()(logs, keepAlive);
    expect(savedCount).toBe(0);
    expect(unsavedLogs).toEqual(logs.map(({ id }) => ({ id, reason: 'no answer' })));
  });

  it('keeps the reason a log was refused even if a later try fails as a whole', async () => {
    answers = [['b'], unreachable(), unreachable()];
    await expect(saver()(logs, keepAlive)).resolves.toEqual({
      savedCount: 2,
      unsavedLogs: [{ id: 'b', reason: '429: no b' }],
    });
  });

  it('reports logs as not saved, without retrying, when the failure needs a person', async () => {
    answers = [notAllowed()];
    const { unsavedLogs } = await saver()(logs, keepAlive);
    expect(unsavedLogs).toEqual(logs.map(({ id }) => ({ id, reason: 'not allowed' })));
    expect(sentIds).toHaveLength(1);
  });

  it('passes on an error that did not come from Elasticsearch', async () => {
    answers = [new Error('a bug')];
    await expect(saver()(logs, keepAlive)).rejects.toThrow('a bug');
  });

  it('keeps Kafka informed before each wait', async () => {
    answers = [unreachable(), ['a']];
    const heartbeat = vi.fn(async () => {});
    await saver()(logs, heartbeat);
    expect(heartbeat).toHaveBeenCalledTimes(2);
  });

  it('passes on an error from keepAlive, because kafkajs uses it to rejoin the group', async () => {
    answers = [unreachable()];
    const rebalancing = async () => {
      throw new Error('the group is rebalancing');
    };
    await expect(saver()(logs, rebalancing)).rejects.toThrow('the group is rebalancing');
  });
});
