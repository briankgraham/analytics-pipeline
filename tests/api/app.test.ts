import { errors } from '@elastic/elasticsearch';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Log } from '../../logs/log.ts';
import { createApp } from '../../api/app.ts';
import type { SearchLogs } from '../../search/search-service.ts';

const good = { service: 'checkout', level: 'error', message: 'card declined' };

let server: Server;
let sent: Log[];
let sendLog: (log: Log) => Promise<void>;
let searchLogs: SearchLogs;
let searchedFor: Parameters<SearchLogs>[0][];

// Sends a request to the real middleware and routes, on a free port with a fake sendLog.
async function post(body: string, contentType: string | null = 'application/json') {
  const { port } = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${port}/v1/message`, {
    method: 'POST',
    headers: contentType ? { 'content-type': contentType } : {},
    body,
  });
  return { status: response.status, body: await response.json() };
}

beforeEach(async () => {
  sent = [];
  sendLog = async (log) => {
    sent.push(log);
  };
  searchedFor = [];
  searchLogs = async (criteria) => {
    searchedFor.push(criteria);
    return [good as Log];
  };
  // Keep the expected failures below from printing errors into the test output.
  vi.spyOn(console, 'error').mockImplementation(() => {});
  server = createApp({
    sendLog: (log) => sendLog(log),
    searchLogs: (criteria) => searchLogs(criteria),
  }).listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
});

afterEach(() => {
  server.close();
  vi.restoreAllMocks();
});

describe('POST /v1/message', () => {
  it('sends a good log to Kafka and replies 202', async () => {
    const reply = await post(JSON.stringify(good));
    expect(reply).toEqual({ status: 202, body: { accepted: true } });
    expect(sent).toEqual([good]);
  });

  it('sends only the fields of a log', async () => {
    await post(JSON.stringify({ ...good, extra: 'ignored' }));
    expect(sent).toEqual([good]);
  });

  it('replies 400 and sends nothing when the log is wrong', async () => {
    const reply = await post(JSON.stringify({ ...good, level: 'loud' }));
    expect(reply.status).toBe(400);
    expect(reply.body).toEqual({ error: 'level must be one of: debug, info, warn, error' });
    expect(sent).toEqual([]);
  });

  it.each([
    ['not JSON', '{"service":'],
    ['a JSON string', '"hello"'],
  ])('replies 400 when the body is %s', async (_name, body) => {
    const reply = await post(body);
    expect(reply.status).toBe(400);
    expect(sent).toEqual([]);
  });

  it('replies 400 for an empty body', async () => {
    const reply = await post('');
    expect(reply.status).toBe(400);
    expect(sent).toEqual([]);
  });

  it('replies 415 when the body is not sent as JSON', async () => {
    for (const contentType of ['text/plain', null]) {
      const reply = await post(JSON.stringify(good), contentType);
      expect(reply.status).toBe(415);
    }
    expect(sent).toEqual([]);
  });

  it('replies 413 when the body is over 100 KB', async () => {
    const reply = await post(JSON.stringify({ ...good, message: 'x'.repeat(200_000) }));
    expect(reply.status).toBe(413);
    expect(sent).toEqual([]);
  });

  it('replies 4xx, not 500, when the body parser cannot read the request', async () => {
    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/v1/message`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'content-encoding': 'unknown' },
      body: JSON.stringify(good),
    });
    expect(response.status).toBe(415);
    expect(await response.json()).toEqual({ error: 'the request could not be read' });
  });

  it('replies 503 when Kafka could not store the log', async () => {
    sendLog = async () => {
      throw new Error('This server does not host this topic-partition');
    };
    const reply = await post(JSON.stringify(good));
    expect(reply.status).toBe(503);
    expect(reply.body).toEqual({ error: 'could not store the log right now, try again' });
  });
});

describe('GET /v1/search', () => {
  async function search(urlValues: string) {
    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/v1/search${urlValues}`);
    return { status: response.status, body: await response.json() };
  }

  it('turns the URL values into criteria and answers with the logs found', async () => {
    const reply = await search('?q=card%20declined&level=error');
    expect(reply).toEqual({ status: 200, body: { logs: [good] } });
    expect(searchedFor).toEqual([
      [
        { type: 'words', field: 'message', words: 'card declined' },
        { type: 'exact', field: 'level', value: 'error' },
      ],
    ]);
  });

  it('replies 200 with an empty list when nothing matches', async () => {
    searchLogs = async () => [];
    expect(await search('?q=nothing')).toEqual({ status: 200, body: { logs: [] } });
  });

  it('replies 400 and does not search when the values are wrong', async () => {
    const reply = await search('?level=loud');
    expect(reply).toEqual({
      status: 400,
      body: { error: 'level must be one of: debug, info, warn, error' },
    });
    expect(searchedFor).toEqual([]);
  });

  it('replies 400 when there is nothing to search for', async () => {
    expect((await search('')).status).toBe(400);
    expect(searchedFor).toEqual([]);
  });

  it.each(['__proto__', 'constructor', 'toString'])(
    'refuses the name %s, which an ordinary object would hide',
    async (name) => {
      for (const urlValues of [`?q=card&${name}=x`, `?${name}`, `?${name}=a&${name}=b`]) {
        const reply = await search(urlValues);
        expect(reply.status).toBe(400);
        expect(reply.body).toEqual({
          error: expect.stringContaining(`"${name}" is not something you can search by`),
        });
      }
      expect(searchedFor).toEqual([]);
    },
  );

  it('replies 503 when Elasticsearch could not answer for now', async () => {
    searchLogs = async () => {
      throw new errors.ConnectionError('no living connections');
    };
    expect(await search('?q=card')).toEqual({
      status: 503,
      body: { error: 'could not search right now, try again' },
    });
  });

  it('replies 500, without saying to try again, when the problem needs a person', async () => {
    searchLogs = async () => {
      throw new Error('this index is closed');
    };
    expect(await search('?q=card')).toEqual({
      status: 500,
      body: { error: 'something went wrong on our side' },
    });
  });

  it('replies 405 for another method', async () => {
    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/v1/search`, { method: 'POST' });
    expect(response.status).toBe(405);
  });
});

describe('other requests', () => {
  it('replies 404 for another path', async () => {
    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/logs`, { method: 'POST' });
    expect(response.status).toBe(404);
  });

  it('replies 404 for /message without the version', async () => {
    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/message`, { method: 'POST' });
    expect(response.status).toBe(404);
  });

  it('replies 405 for another method on /v1/message', async () => {
    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/v1/message`);
    expect(response.status).toBe(405);
    expect(response.headers.get('allow')).toBe('POST');
  });
});
