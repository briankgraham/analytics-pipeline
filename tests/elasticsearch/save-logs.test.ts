// Runs the real Elasticsearch client against a small stand-in server, to pin down how the client
// behaves when Elasticsearch answers with each kind of failure.
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  connectToElasticsearch,
  describeWrongFields,
  type ElasticsearchConnection,
} from '../../elasticsearch/save-logs.ts';

const log = { service: 'checkout', level: 'error', message: 'card declined' } as const;
const logs = ['a', 'b', 'c'].map((id) => ({ id, log }));
const retries = { maxRetries: 2, delayMs: 1, exponential: true, jitter: false };
const keepAlive = async () => {};

const goodFields = {
  service: { type: 'keyword' },
  level: { type: 'keyword' },
  message: { type: 'text' },
};

let server: Server;
let connection: ElasticsearchConnection | undefined;
let requests: string[];
// The ids sent in each bulk request.
let bulkIds: string[][];
// The statuses still to answer, per kind of request. Once a list is empty, the answer is 200.
let createIndexAnswers: number[];
let bulkAnswers: number[];
// For each bulk request that gets a 200, the status of each log in it. Once empty, all are 201.
let itemAnswers: number[][];
// What kind of error a refused log's answer says it is.
let itemErrorType: string;
// When true, bulk requests never get an answer.
let neverAnswerBulk: boolean;
// When true, bulk requests get an answer that starts but never ends, a little at a time.
let trickleBulk: boolean;
// What an existing index says about its fields. Undefined means it has none.
let existingFields: Record<string, { type: string }> | undefined;
// The statuses still to answer when asked about an index's fields. Once empty, the answer is 200.
let mappingAnswers: number[];
// What kind of error a 400 to creating the index says it is.
let createIndexErrorType: string;

async function connect() {
  const { port } = server.address() as AddressInfo;
  connection = await connectToElasticsearch(`http://127.0.0.1:${port}`, 'logs', 0, 200, retries);
  return connection;
}

function bulkAnswer(ids: string[]) {
  const statuses = itemAnswers.shift() ?? [];
  return {
    errors: statuses.some((status) => status !== 201),
    items: ids.map((id, position) => {
      const status = statuses[position] ?? 201;
      return {
        index: {
          _id: id,
          status,
          ...(status === 201 ? {} : { error: { type: itemErrorType, reason: `no ${id}` } }),
        },
      };
    }),
  };
}

beforeEach(async () => {
  requests = [];
  bulkIds = [];
  createIndexAnswers = [];
  bulkAnswers = [];
  itemAnswers = [];
  itemErrorType = 'some_error';
  neverAnswerBulk = false;
  existingFields = goodFields;
  mappingAnswers = [];
  trickleBulk = false;
  createIndexErrorType = 'resource_already_exists_exception';
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  server = createServer((request, response) => {
    let body = '';
    request.on('data', (chunk) => (body += chunk));
    request.on('end', () => {
      requests.push(`${request.method} ${request.url}`);
      if (request.method === 'GET') {
        const status = mappingAnswers.shift() ?? 200;
        response.writeHead(status, {
          'content-type': 'application/json',
          'x-elastic-product': 'Elasticsearch',
        });
        const mappings = existingFields === undefined ? {} : { properties: existingFields };
        response.end(
          JSON.stringify(
            status === 200 ? { logs: { mappings } } : { error: { type: 'x' }, status },
          ),
        );
        return;
      }
      const isCreateIndex = request.method === 'PUT';
      if (neverAnswerBulk && !isCreateIndex) {
        return;
      }
      if (trickleBulk && !isCreateIndex) {
        response.writeHead(200, {
          'content-type': 'application/json',
          'x-elastic-product': 'Elasticsearch',
        });
        const timer = setInterval(() => response.write(' '), 20);
        request.socket.on('close', () => clearInterval(timer));
        return;
      }
      const status = (isCreateIndex ? createIndexAnswers : bulkAnswers).shift() ?? 200;
      // The client refuses to talk to a server that doesn't send this header.
      response.writeHead(status, {
        'content-type': 'application/json',
        'x-elastic-product': 'Elasticsearch',
      });

      if (status !== 200) {
        // A 400 to creating the index is what Elasticsearch answers when the index is already there.
        const type = isCreateIndex && status === 400 ? createIndexErrorType : 'some_error';
        response.end(JSON.stringify({ error: { type }, status }));
      } else if (isCreateIndex) {
        response.end('{}');
      } else {
        const ids = body
          .trim()
          .split('\n')
          .filter((_line, position) => position % 2 === 0)
          .map((line) => JSON.parse(line).index._id as string);
        bulkIds.push(ids);
        response.end(JSON.stringify(bulkAnswer(ids)));
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
});

afterEach(async () => {
  await connection?.close();
  connection = undefined;
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  vi.restoreAllMocks();
});

describe('saveLogs', () => {
  it('sends every log in one request, using the given ids', async () => {
    const { saveLogs } = await connect();
    await expect(saveLogs(logs, keepAlive)).resolves.toEqual({
      savedCount: 3,
      unsavedLogs: [],
    });
    expect(bulkIds).toEqual([['a', 'b', 'c']]);
  });

  it('retries only the logs that were not saved', async () => {
    const { saveLogs } = await connect();
    itemAnswers = [[201, 429, 201]];
    await expect(saveLogs(logs, keepAlive)).resolves.toMatchObject({
      savedCount: 3,
      unsavedLogs: [],
    });
    expect(bulkIds).toEqual([['a', 'b', 'c'], ['b']]);
  });

  it.each([
    ['document_parsing_exception', 400],
    ['index_closed_exception', 400],
    ['illegal_argument_exception', 400],
    ['index_not_found_exception', 404],
  ])('keeps a log refused with %s, because logs reaching here are good', async (type, status) => {
    const { saveLogs } = await connect();
    itemErrorType = type;
    itemAnswers = [[status, 201, 201], [status], [status]];
    await expect(saveLogs(logs, keepAlive)).resolves.toEqual({
      savedCount: 2,
      unsavedLogs: [{ id: 'a', reason: `${status}: no a` }],
    });
    expect(bulkIds).toHaveLength(3);
  });

  it('gives up on a request that never gets an answer, and retries it', async () => {
    const { saveLogs } = await connect();
    neverAnswerBulk = true;
    const { unsavedLogs } = await saveLogs(logs, keepAlive);
    expect(unsavedLogs).toHaveLength(3);
    expect(requests.filter((request) => request.startsWith('POST'))).toHaveLength(3);
  });

  it('gives up on an answer that keeps coming but never ends, and retries it', async () => {
    const { saveLogs } = await connect();
    trickleBulk = true;
    const { unsavedLogs } = await saveLogs(logs, keepAlive);
    expect(unsavedLogs).toHaveLength(3);
    expect(requests.filter((request) => request.startsWith('POST'))).toHaveLength(3);
  });

  it.each([429, 500, 503, 403])(
    'reports a log that keeps getting %i as not saved',
    async (status) => {
      const { saveLogs } = await connect();
      itemAnswers = [[201, status, 201], [status], [status]];
      await expect(saveLogs(logs, keepAlive)).resolves.toEqual({
        savedCount: 2,
        unsavedLogs: [{ id: 'b', reason: `${status}: no b` }],
      });
      expect(bulkIds).toHaveLength(3);
    },
  );

  it.each([429, 502, 503, 504])(
    'tries again, once each, after a whole-request %i',
    async (status) => {
      const { saveLogs } = await connect();
      bulkAnswers = [status];
      await expect(saveLogs(logs, keepAlive)).resolves.toMatchObject({ savedCount: 3 });
      // One failure and one success: the client's own instant retries are off.
      expect(requests.filter((request) => request.startsWith('POST'))).toHaveLength(2);
    },
  );

  it('reports every log as not saved when the whole request keeps failing', async () => {
    const { saveLogs } = await connect();
    bulkAnswers = [429, 429, 429];
    const { savedCount, unsavedLogs } = await saveLogs(logs, keepAlive);
    expect(savedCount).toBe(0);
    expect(unsavedLogs.map(({ id }) => id)).toEqual(['a', 'b', 'c']);
    expect(unsavedLogs[0]?.reason).toContain('some_error');
  });

  it.each([400, 401])('does not retry a whole-request %i, and keeps every log', async (status) => {
    const { saveLogs } = await connect();
    bulkAnswers = [status];
    const { unsavedLogs } = await saveLogs(logs, keepAlive);
    expect(unsavedLogs).toHaveLength(3);
    expect(requests.filter((request) => request.startsWith('POST'))).toHaveLength(1);
  });

  it('reports logs as not saved when Elasticsearch cannot be reached', async () => {
    const { saveLogs } = await connect();
    await new Promise((resolve) => {
      server.close(resolve);
      server.closeAllConnections();
    });
    const { unsavedLogs } = await saveLogs(logs, keepAlive);
    expect(unsavedLogs.map(({ id }) => id)).toEqual(['a', 'b', 'c']);
    // It tried again: one warning for each of the 2 retries.
    expect(console.warn).toHaveBeenCalledTimes(2);
    server = createServer();
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  });

  it('keeps Kafka informed before each wait', async () => {
    const { saveLogs } = await connect();
    bulkAnswers = [429, 429];
    const heartbeat = vi.fn(async () => {});
    await saveLogs(logs, heartbeat);
    expect(heartbeat).toHaveBeenCalledTimes(2);
  });

  it('passes on an error from keepAlive, because kafkajs uses it to rejoin the group', async () => {
    const { saveLogs } = await connect();
    bulkAnswers = [429];
    const rebalancing = async () => {
      throw new Error('the group is rebalancing');
    };
    await expect(saveLogs(logs, rebalancing)).rejects.toThrow('the group is rebalancing');
  });
});

describe('connectToElasticsearch', () => {
  it('tries again when creating the index gets a 429', async () => {
    createIndexAnswers = [429];
    await connect();
    expect(requests).toEqual(['PUT /logs', 'PUT /logs']);
  });

  it('tries again when creating the index gets a 503', async () => {
    createIndexAnswers = [503];
    await connect();
    expect(requests).toEqual(['PUT /logs', 'PUT /logs']);
  });

  it('accepts an index that turns out to exist already when it tries again', async () => {
    createIndexAnswers = [429, 400];
    await expect(connect()).resolves.toBeDefined();
    expect(requests).toEqual(['PUT /logs', 'PUT /logs', 'GET /logs/_mapping']);
  });

  it('does not check the fields of an index it just created', async () => {
    await connect();
    expect(requests).toEqual(['PUT /logs']);
  });

  it('refuses an existing index that stores a field the wrong way, naming it', async () => {
    createIndexAnswers = [400];
    existingFields = { ...goodFields, message: { type: 'date' } };
    await expect(connect()).rejects.toThrow('message is date, should be text');
  });

  it('refuses an existing index with no fields, naming all of ours', async () => {
    createIndexAnswers = [400];
    existingFields = undefined;
    const refused = connect();
    await expect(refused).rejects.toThrow('service has no type');
    await expect(refused).rejects.toThrow('level has no type');
    await expect(refused).rejects.toThrow('message has no type');
  });

  it('warns that deleting a wrong index also deletes the saved logs', async () => {
    createIndexAnswers = [400];
    existingFields = { ...goodFields, message: { type: 'date' } };
    await expect(connect()).rejects.toThrow('also deletes the logs already saved');
  });

  it('tries again when checking the fields gets a 429', async () => {
    createIndexAnswers = [400];
    mappingAnswers = [429];
    await expect(connect()).resolves.toBeDefined();
    expect(requests).toEqual(['PUT /logs', 'GET /logs/_mapping', 'GET /logs/_mapping']);
  });

  it('stops when the fields cannot be checked', async () => {
    createIndexAnswers = [400];
    mappingAnswers = [404];
    await expect(connect()).rejects.toMatchObject({ statusCode: 404 });
  });

  it('throws on any other 400 when creating the index', async () => {
    createIndexAnswers = [400];
    createIndexErrorType = 'illegal_argument_exception';
    await expect(connect()).rejects.toMatchObject({ statusCode: 400 });
    expect(requests).toHaveLength(1);
  });

  it('throws when creating the index keeps getting a 429', async () => {
    createIndexAnswers = [429, 429, 429];
    await expect(connect()).rejects.toMatchObject({ statusCode: 429 });
    expect(requests).toHaveLength(3);
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
