import { createServer, IncomingMessage, request, type ClientRequest } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Socket } from 'node:net';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { censor, createLogger, REDACTED, UNSERIALIZABLE } from './index.js';

const secret = 'obs_live_c2VjcmV0LWtleS1tYXRlcmlhbA';

describe('Node HTTP objects (real loopback request)', () => {
  const lines: string[] = [];
  const logger = createLogger({
    service: 'ingest-api',
    destination: { write: (l) => lines.push(l) },
  });
  const server = createServer((req, res) => {
    res.setHeader('set-cookie', `session=${secret}`);
    logger.info({ req, res }, 'request');
    res.end('ok');
  });
  let port = 0;

  beforeEach(async () => {
    lines.length = 0;
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    port = (server.address() as AddressInfo).port;
  });

  afterEach(async () => {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  });

  async function send(): Promise<ClientRequest> {
    return new Promise((resolve, reject) => {
      const client = request(
        {
          host: '127.0.0.1',
          port,
          path: `/v1/search?q=errors&token=${secret}`,
          headers: { Authorization: `Bearer ${secret}`, 'x-request-id': 'req-1' },
        },
        (response) => {
          response.resume();
          response.on('end', () => {
            resolve(client);
          });
        },
      );
      client.on('error', reject);
      client.end();
    });
  }

  it('logs requests and responses as allowlisted shapes without rawHeaders or secrets', async () => {
    const client = await send();
    logger.info({ client, res: { statusCode: 200 } }, 'outgoing');
    const all = lines.join('');
    expect(all).not.toContain(secret);
    expect(all).not.toContain('rawHeaders');
    expect(all).not.toContain('socket');

    const [incoming, outgoing] = lines.map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(incoming?.['req']).toEqual({
      method: 'GET',
      url: `/v1/search?q=errors&token=${REDACTED}`,
      headers: expect.objectContaining({
        authorization: REDACTED,
        'x-request-id': 'req-1',
      }) as object,
    });
    expect(incoming?.['res']).toEqual({ statusCode: 200, headers: { 'set-cookie': REDACTED } });
    expect(outgoing?.['client']).toEqual({
      method: 'GET',
      host: '127.0.0.1',
      path: `/v1/search?q=errors&token=${REDACTED}`,
      headers: expect.objectContaining({ authorization: REDACTED }) as object,
    });
  });
});

describe('other non-plain values', () => {
  it('censors sensitive query parameters and passwords in URL objects', () => {
    const url = new URL(`https://user:${secret}@es.example.com/_search?api_key=${secret}&q=x#frag`);
    const censored = String(censor(url));
    expect(censored).not.toContain(secret);
    expect(censored).toBe(
      `https://user:${REDACTED}@es.example.com/_search?api_key=${REDACTED}&q=x#frag`,
    );
    expect(censored).toContain('q=x');
    expect(censored).toContain('#frag');
    expect(String(censor(new URL('http://es:9200/')))).toBe('http://es:9200/');
  });

  it.each([
    ['//evil.example/x?token=S', '//evil.example/x?token=[redacted]'],
    ['http://u:S@h/p?q=1', 'http://u:[redacted]@h/p?q=1'],
    ['http://u:p@ss@h/x', 'http://u:[redacted]@h/x'],
    ['/a?x=1&api%5Fkey=S&Token=S&flag', '/a?x=1&api%5Fkey=[redacted]&Token=[redacted]&flag'],
    ['/a#access_token=S&state=ok', '/a#access_token=[redacted]&state=ok'],
    ['/a?%E0%A4%A=S', '/a?%E0%A4%A=S'],
    ['/plain/path', '/plain/path'],
  ])('keeps the request target %j verbatim apart from secrets', (url, logged) => {
    const message = new IncomingMessage(new Socket());
    message.url = url;
    expect(censor(message)).toMatchObject({ url: logged });
  });

  it('redacts URL-valued string fields such as Koa request.url', () => {
    expect(
      censor({
        request: { method: 'GET', url: '/v1/search?token=S&q=x', header: {} },
        originalUrl: '/a?apiKey=S',
        href: 'https://es/?password=S',
        path: '/p#token=S',
        name: '/not-a-url?token=S',
        url2: 1,
      }),
    ).toEqual({
      request: { method: 'GET', url: '/v1/search?token=[redacted]&q=x', header: {} },
      originalUrl: '/a?apiKey=[redacted]',
      href: 'https://es/?password=[redacted]',
      path: '/p#token=[redacted]',
      name: '/not-a-url?token=S',
      url2: 1,
    });
    expect(censor({ url: 42 })).toEqual({ url: 42 });
  });

  it('turns an Error whose getters throw into [unserializable]', () => {
    const error = new Error('outer');
    Object.defineProperty(error, 'cause', {
      get(): never {
        throw new Error('boom');
      },
    });
    expect(censor({ error })).toEqual({ error: UNSERIALIZABLE });
  });

  it('handles an IncomingMessage without a url', () => {
    const message = new IncomingMessage(new Socket());
    delete message.url;
    expect(censor(message)).toMatchObject({ url: '' });
  });

  it('turns a throwing getter or toJSON into [unserializable] instead of throwing', () => {
    const getter = {
      get bad(): string {
        throw new Error('boom');
      },
    };
    const throwingToJSON = Object.create({
      toJSON(): never {
        throw new Error('boom');
      },
    }) as object;
    expect(censor({ g: getter, t: throwingToJSON, ok: 1 })).toEqual({
      g: UNSERIALIZABLE,
      t: UNSERIALIZABLE,
      ok: 1,
    });
    const { logger } = { logger: createLogger({ service: 's', destination: { write: () => 0 } }) };
    expect(() => {
      logger.info({ g: getter }, 'still logs');
    }).not.toThrow();
  });
});
