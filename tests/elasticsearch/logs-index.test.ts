import { errors } from '@elastic/elasticsearch';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ElasticsearchCalls, ExistingFields } from '../../elasticsearch/client.ts';
import { describeWrongFields, setUpLogsIndex } from '../../elasticsearch/logs-index.ts';

const retries = { maxRetries: 2, delayMs: 1, exponential: true, jitter: false };
const goodFields = {
  service: { type: 'keyword' },
  level: { type: 'keyword' },
  message: { type: 'text' },
};

// Elasticsearch can't be reached: an error that can pass with time.
const unreachable = () => new errors.ConnectionError('no answer');

// Stands in for the base layer. Each call uses up one answer; an Error is thrown, anything else is
// answered. Once a list is empty, the call succeeds.
let createAnswers: ('created' | 'already exists' | Error)[];
let fieldsAnswers: (ExistingFields[] | Error)[];
let strictAnswers: Error[];
let client: ElasticsearchCalls;

function nextAnswer<Answer>(answers: (Answer | Error)[], otherwise: Answer): Answer {
  const answer = answers.shift() ?? otherwise;
  if (answer instanceof Error) {
    throw answer;
  }
  return answer;
}

beforeEach(() => {
  createAnswers = [];
  fieldsAnswers = [];
  strictAnswers = [];
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  client = {
    createIndex: vi.fn(async () => nextAnswer(createAnswers, 'created' as const)),
    readIndexFields: vi.fn(async () => nextAnswer(fieldsAnswers, [goodFields])),
    makeMappingStrict: vi.fn(async () => {
      const error = strictAnswers.shift();
      if (error !== undefined) {
        throw error;
      }
    }),
    saveDocuments: vi.fn(),
  };
});

afterEach(() => {
  vi.restoreAllMocks();
});

const setUp = () => setUpLogsIndex(client, 'logs', 1, retries);

describe('setUpLogsIndex', () => {
  it('creates the index with our fields, and does not check an index it just created', async () => {
    await setUp();
    expect(client.createIndex).toHaveBeenCalledWith('logs', goodFields, 1);
    expect(client.readIndexFields).not.toHaveBeenCalled();
    expect(client.makeMappingStrict).not.toHaveBeenCalled();
  });

  it('makes an existing index strict once its fields are right', async () => {
    createAnswers = ['already exists'];
    await setUp();
    expect(client.makeMappingStrict).toHaveBeenCalledWith('logs');
  });

  it('does not make an existing index strict when its fields are wrong', async () => {
    createAnswers = ['already exists'];
    fieldsAnswers = [[{}]];
    await expect(setUp()).rejects.toThrow();
    expect(client.makeMappingStrict).not.toHaveBeenCalled();
  });

  it('tries again when making the index strict fails in a way that can pass', async () => {
    createAnswers = ['already exists'];
    strictAnswers = [unreachable()];
    await expect(setUp()).resolves.toBeUndefined();
    expect(client.makeMappingStrict).toHaveBeenCalledTimes(2);
  });

  it('accepts an existing index that stores our fields the right way', async () => {
    createAnswers = ['already exists'];
    await expect(setUp()).resolves.toBeUndefined();
    expect(client.readIndexFields).toHaveBeenCalledWith('logs');
  });

  it('refuses an existing index that stores a field the wrong way, naming it', async () => {
    createAnswers = ['already exists'];
    fieldsAnswers = [[{ ...goodFields, message: { type: 'date' } }]];
    await expect(setUp()).rejects.toThrow('message is date, should be text');
  });

  it('warns that deleting a wrong index also deletes the saved logs', async () => {
    createAnswers = ['already exists'];
    fieldsAnswers = [[{}]];
    await expect(setUp()).rejects.toThrow('also deletes the logs already saved');
  });

  it('checks every index behind the name', async () => {
    createAnswers = ['already exists'];
    fieldsAnswers = [[goodFields, { ...goodFields, level: { type: 'text' } }]];
    await expect(setUp()).rejects.toThrow('level is text, should be keyword');
  });

  it('tries again when creating the index fails in a way that can pass', async () => {
    createAnswers = [unreachable()];
    await expect(setUp()).resolves.toBeUndefined();
    expect(client.createIndex).toHaveBeenCalledTimes(2);
  });

  it('tries again when checking the fields fails in a way that can pass', async () => {
    createAnswers = ['already exists'];
    fieldsAnswers = [unreachable()];
    await expect(setUp()).resolves.toBeUndefined();
    expect(client.readIndexFields).toHaveBeenCalledTimes(2);
  });

  it('accepts an index that turns out to exist already when it tries again', async () => {
    createAnswers = [unreachable(), 'already exists'];
    await expect(setUp()).resolves.toBeUndefined();
    expect(client.readIndexFields).toHaveBeenCalledOnce();
  });

  it('throws once the retries run out', async () => {
    createAnswers = [unreachable(), unreachable(), unreachable()];
    await expect(setUp()).rejects.toThrow('no answer');
    expect(client.createIndex).toHaveBeenCalledTimes(3);
  });

  it('throws right away on a failure that will not pass', async () => {
    createAnswers = [new Error('something else')];
    await expect(setUp()).rejects.toThrow('something else');
    expect(client.createIndex).toHaveBeenCalledOnce();
  });
});

describe('describeWrongFields', () => {
  it('accepts our fields, and ignores fields we never send', () => {
    expect(describeWrongFields({ ...goodFields, extra: { type: 'date' } })).toEqual([]);
  });

  it('names every field that is wrong or missing', () => {
    const { level: _level, ...noLevel } = goodFields;
    expect(describeWrongFields({ ...noLevel, service: { type: 'text' } })).toEqual([
      'service is text, should be keyword',
      'level has no type (it is missing, or holds an object), should be keyword',
    ]);
  });
});
