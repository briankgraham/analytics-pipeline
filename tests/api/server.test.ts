import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Log } from '../../logs/log.ts';
import { createServer } from '../../api/server.ts';

const good = { service: 'checkout', level: 'error', message: 'card declined' };

let server: Server;
let sent: Log[];
let sendLog: (log: Log) => Promise<void>;

// Starts the real web server on a free port with a fake sendLog, and sends it a request.
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
  // Keep the expected failures below from printing errors into the test output.
  vi.spyOn(console, 'error').mockImplementation(() => {});
  server = createServer({ sendLog: (log) => sendLog(log) }).listen(0, '127.0.0.1');
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
