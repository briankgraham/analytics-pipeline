// Runs the real Elasticsearch client against a small stand-in server, to pin down how the client
// behaves when Elasticsearch answers 429 ("too many requests").
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  connectToElasticsearch,
  type ElasticsearchConnection,
} from '../../elasticsearch/save-log.ts';

const log = { service: 'checkout', level: 'error', message: 'card declined' } as const;
const retries = { maxRetries: 2, delayMs: 1, exponential: true, jitter: false };

let server: Server;
let connection: ElasticsearchConnection | undefined;
let requests: string[];
// The statuses still to answer, per kind of request. Once a list is empty, the answer is 200.
let createIndexAnswers: number[];
let saveLogAnswers: number[];
// What kind of error a 400 to creating the index says it is.
let createIndexErrorType: string;

async function connect() {
  const { port } = server.address() as AddressInfo;
  connection = await connectToElasticsearch(`http://127.0.0.1:${port}`, 'logs', 0, retries);
  return connection;
}

beforeEach(async () => {
  requests = [];
  createIndexAnswers = [];
  saveLogAnswers = [];
  createIndexErrorType = 'resource_already_exists_exception';
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  server = createServer((request, response) => {
    request.resume();
    requests.push(`${request.method} ${request.url}`);
    const answers = request.method === 'PUT' ? createIndexAnswers : saveLogAnswers;
    const status = answers.shift() ?? 200;
    // The client refuses to talk to a server that doesn't send this header.
    response.writeHead(status, {
      'content-type': 'application/json',
      'x-elastic-product': 'Elasticsearch',
    });
    // A 400 to creating the index is what Elasticsearch answers when the index is already there.
    const type = request.method === 'PUT' && status === 400 ? createIndexErrorType : 'some_error';
    response.end(JSON.stringify(status === 200 ? {} : { error: { type }, status }));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
});

afterEach(async () => {
  await connection?.close();
  connection = undefined;
  await new Promise((resolve) => server.close(resolve));
  vi.restoreAllMocks();
});

describe('saveLog', () => {
  it('tries again after a 429, saying so, and finishes once a try works', async () => {
    const { saveLog } = await connect();
    saveLogAnswers = [429, 429];
    await expect(saveLog(log)).resolves.toBeUndefined();
    expect(requests.filter((request) => request.startsWith('POST'))).toHaveLength(3);
    expect(console.warn).toHaveBeenCalledTimes(2);
    expect(console.warn).toHaveBeenLastCalledWith('elasticsearch: too busy (429), retry 2 in 2 ms');
  });

  it('throws once the retries run out, so Kafka hands the message over again', async () => {
    const { saveLog } = await connect();
    saveLogAnswers = [429, 429, 429, 429];
    await expect(saveLog(log)).rejects.toMatchObject({ statusCode: 429 });
    expect(requests.filter((request) => request.startsWith('POST'))).toHaveLength(3);
  });

  it('does not wait and try again on an answer that a retry would not fix', async () => {
    const { saveLog } = await connect();
    saveLogAnswers = [400];
    await expect(saveLog(log)).rejects.toMatchObject({ statusCode: 400 });
    expect(requests.filter((request) => request.startsWith('POST'))).toHaveLength(1);
    expect(console.warn).not.toHaveBeenCalled();
  });
});

describe('connectToElasticsearch', () => {
  it('tries again when creating the index gets a 429', async () => {
    createIndexAnswers = [429];
    await connect();
    expect(requests).toEqual(['PUT /logs', 'PUT /logs']);
  });

  it('accepts an index that turns out to exist already when it tries again', async () => {
    createIndexAnswers = [429, 400];
    await expect(connect()).resolves.toBeDefined();
    expect(requests).toEqual(['PUT /logs', 'PUT /logs']);
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
