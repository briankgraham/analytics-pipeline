import { pino } from 'pino';
import { describe, expect, it, vi } from 'vitest';
import {
  censor,
  CIRCULAR,
  createLogger,
  MAX_DEPTH,
  REDACTED,
  serializeError,
  TRUNCATED,
  type LoggerOptions,
} from './index.js';

/** An in-memory destination: each write is one JSON line. */
function capture(options: Omit<LoggerOptions, 'destination'> = { service: 'ingest-api' }) {
  const lines: string[] = [];
  const logger = createLogger({ ...options, destination: { write: (line) => lines.push(line) } });
  const records = (): Record<string, unknown>[] =>
    lines.map((line) => JSON.parse(line) as Record<string, unknown>);
  return { logger, lines, records };
}

const secret = 'obs_live_c2VjcmV0LWtleS1tYXRlcmlhbA';

describe('createLogger', () => {
  it('writes one JSON line per record with service, label level, ISO time, and message', () => {
    const { logger, records } = capture();
    logger.info('started');
    const [record] = records();
    expect(record).toMatchObject({ level: 'info', service: 'ingest-api', msg: 'started' });
    expect(typeof record?.['pid']).toBe('number');
    expect(typeof record?.['hostname']).toBe('string');
    expect(new Date(String(record?.['time'])).toISOString()).toBe(record?.['time']);
  });

  it('defaults to info and honours the configured level', () => {
    const quiet = capture();
    quiet.logger.debug('hidden');
    expect(quiet.records()).toEqual([]);

    const verbose = capture({ service: 'indexer', level: 'debug' });
    verbose.logger.debug('shown');
    expect(verbose.records()).toHaveLength(1);

    const silent = capture({ service: 'indexer', level: 'silent' });
    silent.logger.fatal('nothing');
    expect(silent.records()).toEqual([]);
  });

  it('writes to stdout (fd 1) when no destination is given', () => {
    const destination = vi.spyOn(pino, 'destination');
    const logger = createLogger({ service: 'indexer', level: 'silent' });
    expect(destination).toHaveBeenCalledExactlyOnceWith(1);
    expect(logger.level).toBe('silent');
  });

  it('carries child bindings such as request and event IDs', () => {
    const { logger, records } = capture();
    logger.child({ requestId: 'req-1', tenantId: 'acme' }).warn({ eventId: 'e-1' }, 'rejected');
    expect(records()[0]).toMatchObject({
      level: 'warn',
      service: 'ingest-api',
      requestId: 'req-1',
      tenantId: 'acme',
      eventId: 'e-1',
      msg: 'rejected',
    });
  });

  it('keeps a parent’s bindings and level in grandchildren, newer bindings winning', () => {
    const { logger, lines, records } = capture();
    const child = logger.child({ requestId: 'r', stage: 'a' }, { level: 'debug' });
    child.child({ stage: 'b' }).debug('deep');
    expect(records()).toEqual([
      expect.objectContaining({ level: 'debug', requestId: 'r', stage: 'b', msg: 'deep' }),
    ]);
    // On the raw line: JSON.parse would hide duplicated keys.
    expect(lines[0]?.match(/"service":/g)).toHaveLength(1);
    expect(lines[0]?.match(/"stage":/g)).toHaveLength(1);
  });

  describe('errors (allowlisted fields only)', () => {
    it('keeps type, message, stack, code, and a structured cause chain', () => {
      const { logger, records } = capture();
      const root = Object.assign(new Error('root'), { code: 'ECONNREFUSED' });
      logger.error({ err: new TypeError('bad', { cause: root }) }, 'failed');
      const err = records()[0]?.['err'] as Record<string, unknown>;
      expect(err['type']).toBe('TypeError');
      expect(err['message']).toBe('bad');
      expect(String(err['stack'])).toContain('TypeError: bad');
      expect(err['cause']).toMatchObject({ type: 'Error', message: 'root', code: 'ECONNREFUSED' });
    });

    it('drops client metadata such as Elasticsearch request headers, on the error and its cause', () => {
      const { logger, lines } = capture();
      const esError = Object.assign(new Error('Response Error'), {
        meta: { meta: { request: { params: { headers: { authorization: `ApiKey ${secret}` } } } } },
      });
      const wrapped = Object.assign(new Error('index failed', { cause: esError }), {
        headers: { Authorization: secret },
      });
      logger.error({ err: wrapped }, 'bulk failed');
      expect(lines.join('')).not.toContain(secret);
      expect(lines.join('')).toContain('Response Error');
    });

    it('keeps numeric codes and drops non-scalar ones', () => {
      expect(serializeError(Object.assign(new Error('x'), { code: 409 }))).toMatchObject({
        code: 409,
      });
      expect(
        serializeError(Object.assign(new Error('x'), { code: { nested: true } })),
      ).not.toHaveProperty('code');
      const noStack = new Error('x');
      delete noStack.stack;
      expect(serializeError(noStack)).toEqual({ type: 'Error', message: 'x' });
    });

    it('keeps a thrown non-Error, censored, instead of dropping it', () => {
      const { logger, records } = capture();
      logger.warn({ err: 'timeout' }, 'string thrown');
      logger.warn({ err: { reason: 'nope', token: secret } }, 'object thrown');
      logger.warn({ err: new Error('outer', { cause: 'plain cause' }) }, 'string cause');
      expect(records().map((r) => r['err'])).toEqual([
        'timeout',
        { reason: 'nope', token: REDACTED },
        expect.objectContaining({ message: 'outer', cause: 'plain cause' }),
      ]);
    });

    it('bounds a cause chain', () => {
      let error = new Error('e0');
      for (let i = 1; i <= MAX_DEPTH + 3; i++) error = new Error(`e${String(i)}`, { cause: error });
      const line = JSON.stringify(serializeError(error));
      expect(line.match(/"message":/g)).toHaveLength(MAX_DEPTH);
      expect(line).toContain(TRUNCATED);
    });

    it('serializes errors under any key, nested, and in arrays', () => {
      const { logger, lines } = capture();
      const esError = Object.assign(new Error('Response Error'), {
        meta: { meta: { request: { params: { headers: { authorization: `ApiKey ${secret}` } } } } },
      });
      logger.error({ error: esError, ctx: { err: esError }, errs: [esError] }, 'failed');
      expect(lines.join('')).not.toContain(secret);
      expect(lines.join('').match(/Response Error/g)?.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('censoring (keys and Authorization headers are never logged)', () => {
    it.each([
      ['a top-level authorization field', { authorization: `Bearer ${secret}` }],
      ['Node request headers', { req: { headers: { authorization: `Bearer ${secret}` } } }],
      ['Koa ctx.request (singular header)', { request: { header: { authorization: secret } } }],
      ['a capitalised Authorization header', { headers: { Authorization: secret } }],
      ['an x-api-key header', { req: { headers: { 'x-api-key': secret } } }],
      [
        'cookies',
        { req: { headers: { cookie: secret } }, res: { headers: { 'set-cookie': [secret] } } },
      ],
      ['two levels down', { ctx: { req: { headers: { authorization: secret } } } }],
      ['nested client config', { config: { kafka: { sasl: { password: secret } } } }],
      ['apiKey inside an array', { items: [{ id: 1 }, { apiKey: secret }] }],
      ['api_key', { api_key: secret }],
      ['tokens and secrets', { a: { b: { token: secret } }, auth: { clientSecret: secret } }],
    ])('censors %s', (_name, fields) => {
      const { logger, lines } = capture();
      logger.info(fields, 'request');
      expect(lines.join('')).not.toContain(secret);
      expect(lines.join('')).toContain(REDACTED);
    });

    it('censors child bindings', () => {
      const { logger, lines } = capture();
      logger.child({ req: { headers: { Authorization: secret } } }).info('bound');
      logger.child({ requestId: 'r' }).child({ apiKey: secret }).info('grandchild');
      expect(lines).toHaveLength(2);
      expect(lines.join('')).not.toContain(secret);
      expect(JSON.parse(lines[1] ?? '{}')).toMatchObject({ requestId: 'r', apiKey: REDACTED });
    });

    it('keeps ordinary fields, including Kafka record keys, readable', () => {
      const { logger, records } = capture();
      logger.info(
        { key: 'acme:checkout', topic: 'events.raw', partition: 3, keys: ['a'] },
        'consumed',
      );
      expect(records()[0]).toMatchObject({
        key: 'acme:checkout',
        topic: 'events.raw',
        partition: 3,
        keys: ['a'],
      });
    });

    it('never mutates the caller’s objects', () => {
      const headers = { authorization: secret };
      const { logger } = capture();
      logger.info({ req: { headers } }, 'request');
      expect(headers.authorization).toBe(secret);
    });

    it('survives cycles and bounds depth', () => {
      const cyclic: Record<string, unknown> = { name: 'loop' };
      cyclic['self'] = cyclic;
      expect(censor(cyclic)).toEqual({ name: 'loop', self: CIRCULAR });

      const shared = { token: secret };
      expect(censor({ a: shared, b: shared })).toEqual({
        a: { token: REDACTED },
        b: { token: REDACTED },
      });

      let deep: unknown = { leaf: true };
      for (let i = 0; i < MAX_DEPTH + 2; i++) deep = { next: deep };
      expect(JSON.stringify(censor(deep))).toContain(TRUNCATED);
    });

    it('censors objects with toJSON in the form they would be written (Koa ctx.request)', () => {
      // Koa builds ctx.request with Object.create(app.request); its toJSON exposes `header`.
      const koaRequestProto = {
        toJSON(this: { method: string; header: Record<string, string> }) {
          return { method: this.method, header: this.header };
        },
      };
      const request: unknown = Object.assign(Object.create(koaRequestProto) as object, {
        method: 'POST',
        header: { authorization: `Bearer ${secret}` },
      });
      const { logger, records } = capture();
      logger.info({ request }, 'access');
      expect(records()[0]?.['request']).toEqual({
        method: 'POST',
        header: { authorization: REDACTED },
      });
    });

    it('walks other class instances by their own fields', () => {
      class Options {
        constructor(
          readonly host: string,
          readonly authorization: string,
        ) {}
      }
      expect(censor({ client: new Options('es', secret) })).toEqual({
        client: { host: 'es', authorization: REDACTED },
      });
    });

    it('keeps Dates as ISO strings and handles null-prototype objects', () => {
      expect(censor({ at: new Date(0) })).toEqual({ at: '1970-01-01T00:00:00.000Z' });
      expect(censor(Object.assign(Object.create(null) as object, { token: secret }))).toEqual({
        token: REDACTED,
      });
    });

    it('treats a toJSON that returns itself as a cycle', () => {
      const selfish = {
        toJSON(): unknown {
          return this;
        },
      };
      expect(censor({ s: Object.create(selfish) as object })).toEqual({ s: CIRCULAR });
    });
  });
});
