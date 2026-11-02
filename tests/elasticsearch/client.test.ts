// Runs the real Elasticsearch client against a small stand-in server, to pin down how the client
// behaves when Elasticsearch answers with each kind of failure.
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  canPassWithTime,
  ElasticsearchClient,
  isElasticsearchError,
} from '../../elasticsearch/client.ts';

const fields = { message: { type: 'text' } } as const;
const documents = ['a', 'b', 'c'].map((id) => ({ id, document: { message: `about ${id}` } }));

let server: Server;
let client: ElasticsearchClient;
let requests: string[];
// The lines of the last bulk request.
let bulkLines: unknown[];
// The next status to answer with. Undefined means 200.
let nextStatus: number | undefined;
// What kind of error the answer says it is.
let errorType: string;
// For a bulk request that gets a 200, the status of each document. Any missing one is 201.
let itemAnswers: number[];
// What the stand-in answers about an index's mappings.
let mappingAnswer: unknown;
let behaviour: 'answer' | 'never answer' | 'answer that never ends';

// The client refuses to talk to a server that doesn't send this header.
const headers = { 'content-type': 'application/json', 'x-elastic-product': 'Elasticsearch' };

function bulkPosts() {
  return requests.filter((request) => request.startsWith('POST'));
}

async function errorFrom(call: Promise<unknown>): Promise<unknown> {
  return call.then(
    () => {
      throw new Error('expected the call to fail');
    },
    (error: unknown) => error,
  );
}

beforeEach(async () => {
  requests = [];
  bulkLines = [];
  nextStatus = undefined;
  errorType = 'some_error';
  itemAnswers = [];
  mappingAnswer = { logs: { mappings: { properties: fields } } };
  behaviour = 'answer';
  server = createServer((request, response) => {
    let body = '';
    request.on('data', (chunk) => (body += chunk));
    request.on('end', () => {
      requests.push(`${request.method} ${request.url}`);
      const isBulk = request.method === 'POST';
      if (behaviour === 'never answer') {
        return;
      }
      if (behaviour === 'answer that never ends') {
        response.writeHead(200, headers);
        const timer = setInterval(() => response.write(' '), 20);
        request.socket.on('close', () => clearInterval(timer));
        return;
      }

      const status = nextStatus ?? 200;
      response.writeHead(status, headers);
      if (status !== 200) {
        response.end(JSON.stringify({ error: { type: errorType }, status }));
      } else if (request.method === 'GET') {
        response.end(JSON.stringify(mappingAnswer));
      } else if (isBulk) {
        bulkLines = body
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line));
        const ids = documents.map(({ id }) => id).slice(0, bulkLines.length / 2);
        const items = ids.map((id, position) => {
          const itemStatus = itemAnswers[position] ?? 201;
          const refused = { error: { type: errorType, reason: `no ${id}` } };
          return { index: { _id: id, status: itemStatus, ...(itemStatus === 201 ? {} : refused) } };
        });
        response.end(JSON.stringify({ errors: itemAnswers.length > 0, items }));
      } else {
        response.end('{}');
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  client = ElasticsearchClient.getInstance(`http://127.0.0.1:${port}`, 200);
  await client.init();
});

afterEach(async () => {
  await client.close();
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
});

describe('init and close', () => {
  it('sends nothing to Elasticsearch on init or close', async () => {
    await client.close();
    expect(requests).toEqual([]);
  });

  it('has only one instance', () => {
    const { port } = server.address() as AddressInfo;
    expect(ElasticsearchClient.getInstance(`http://127.0.0.1:${port}`, 200)).toBe(client);
  });

  it('refuses to be asked for with different settings', () => {
    expect(() => ElasticsearchClient.getInstance('http://other', 200)).toThrow(
      'different settings',
    );
  });

  it('keeps the same connection however often init is called', async () => {
    const connection = client['client'];
    expect(connection).toBeDefined();
    await client.init();
    await Promise.all([client.init(), client.init()]);
    expect(client['client']).toBe(connection);
  });

  it('refuses calls before init, with a clear message', async () => {
    await client.close();
    client = ElasticsearchClient.getInstance('http://127.0.0.1:1', 200);
    await expect(client.readIndexFields('logs')).rejects.toThrow('call init() first');
  });

  it('refuses calls and init after close, saying it is closed', async () => {
    await client.close();
    await expect(client.saveDocuments('logs', documents)).rejects.toThrow('is closed');
    await expect(client.init()).rejects.toThrow('is closed');
  });

  it('gives a new instance, with any settings, after close', async () => {
    const closed = client;
    await closed.close();
    client = ElasticsearchClient.getInstance('http://other', 200);
    expect(client).not.toBe(closed);
  });

  it('can be closed twice, or without ever starting, without disturbing a newer instance', async () => {
    const closed = client;
    await closed.close();
    await expect(closed.close()).resolves.toBeUndefined();

    client = ElasticsearchClient.getInstance('http://127.0.0.1:1', 200);
    await closed.close();
    expect(ElasticsearchClient.getInstance('http://127.0.0.1:1', 200)).toBe(client);
  });
});

describe('saveDocuments', () => {
  it('sends one bulk request with the given ids and documents', async () => {
    const answers = await client.saveDocuments('logs', documents.slice(0, 2));
    expect(answers).toEqual([
      { id: 'a', saved: true },
      { id: 'b', saved: true },
    ]);
    expect(bulkPosts()).toHaveLength(1);
    expect(bulkLines).toEqual([
      { index: { _index: 'logs', _id: 'a' } },
      { message: 'about a' },
      { index: { _index: 'logs', _id: 'b' } },
      { message: 'about b' },
    ]);
  });

  it.each([
    [429, 'es_rejected_execution_exception'],
    [400, 'document_parsing_exception'],
    [400, 'index_closed_exception'],
    [404, 'index_not_found_exception'],
    [503, 'unavailable_shards_exception'],
  ])('reports a document refused with %i %s, saying why', async (status, type) => {
    errorType = type;
    itemAnswers = [201, status, 201];
    await expect(client.saveDocuments('logs', documents)).resolves.toEqual([
      { id: 'a', saved: true },
      { id: 'b', saved: false, reason: `${status}: no b` },
      { id: 'c', saved: true },
    ]);
  });

  it.each([429, 502, 503, 504])(
    'makes one attempt and throws on a whole-request %i, which can pass with time',
    async (status) => {
      nextStatus = status;
      const error = await errorFrom(client.saveDocuments('logs', documents));
      expect(error).toMatchObject({ statusCode: status });
      expect(canPassWithTime(error)).toBe(true);
      expect(isElasticsearchError(error)).toBe(true);
      expect(bulkPosts()).toHaveLength(1);
    },
  );

  it.each([400, 401, 403])('throws on a whole-request %i, which needs a person', async (status) => {
    nextStatus = status;
    const error = await errorFrom(client.saveDocuments('logs', documents));
    expect(error).toMatchObject({ statusCode: status });
    expect(canPassWithTime(error)).toBe(false);
    expect(isElasticsearchError(error)).toBe(true);
  });

  it.each(['never answer', 'answer that never ends'] as const)(
    'gives up after the time limit on a server that will %s',
    async (serverBehaviour) => {
      behaviour = serverBehaviour;
      const error = await errorFrom(client.saveDocuments('logs', documents));
      expect(canPassWithTime(error)).toBe(true);
      expect(bulkPosts()).toHaveLength(1);
    },
  );

  it('throws an error that can pass with time when the server cannot be reached', async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    const error = await errorFrom(client.saveDocuments('logs', documents));
    expect(canPassWithTime(error)).toBe(true);
    server = createServer();
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  });
});

describe('the time limit on index calls', () => {
  it.each(['never answer', 'answer that never ends'] as const)(
    'gives up when creating an index on a server that will %s',
    async (serverBehaviour) => {
      behaviour = serverBehaviour;
      const error = await errorFrom(client.createIndex('logs', fields, 0));
      expect(canPassWithTime(error)).toBe(true);
    },
  );

  it.each(['never answer', 'answer that never ends'] as const)(
    'gives up when reading fields from a server that will %s',
    async (serverBehaviour) => {
      behaviour = serverBehaviour;
      const error = await errorFrom(client.readIndexFields('logs'));
      expect(canPassWithTime(error)).toBe(true);
    },
  );
});

describe('createIndex', () => {
  it('answers "created" and sends the fields and replicas', async () => {
    await expect(client.createIndex('logs', fields, 2)).resolves.toBe('created');
    expect(requests).toEqual(['PUT /logs']);
  });

  it('answers "already exists" when Elasticsearch says so', async () => {
    nextStatus = 400;
    errorType = 'resource_already_exists_exception';
    await expect(client.createIndex('logs', fields, 0)).resolves.toBe('already exists');
  });

  it('throws on any other 400', async () => {
    nextStatus = 400;
    errorType = 'illegal_argument_exception';
    await expect(client.createIndex('logs', fields, 0)).rejects.toMatchObject({
      statusCode: 400,
    });
  });
});

describe('readIndexFields', () => {
  it('answers with the fields of each index behind the name', async () => {
    await expect(client.readIndexFields('logs')).resolves.toEqual([fields]);
    expect(requests).toEqual(['GET /logs/_mapping']);
  });

  it('answers with no fields for an index that has none', async () => {
    mappingAnswer = { logs: { mappings: {} } };
    await expect(client.readIndexFields('logs')).resolves.toEqual([{}]);
  });

  it('throws when the index is not there', async () => {
    nextStatus = 404;
    await expect(client.readIndexFields('logs')).rejects.toMatchObject({ statusCode: 404 });
  });
});
