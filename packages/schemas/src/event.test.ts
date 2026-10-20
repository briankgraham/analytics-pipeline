import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import {
  IDENTIFIER_PATTERN,
  IngestBatchSchema,
  LEVELS,
  LogEventSchema,
  MAX_ATTR_KEY_LENGTH,
  MAX_ATTR_KEYS,
  MAX_ATTR_VALUE_LENGTH,
  MAX_BATCH_EVENTS,
  MAX_HOST_LENGTH,
  MAX_MESSAGE_BYTES,
  MAX_TRACE_ID_LENGTH,
  RawEventSchema,
} from './index.js';

const valid = {
  schemaVersion: 1,
  id: '01J9Z8Y7X6W5V4T3S2R1Q0P9N8',
  ts: 1_700_000_000_000,
  service: 'checkout',
  env: 'prod',
  level: 'error',
  message: 'payment failed',
};

const accepts = (event: unknown): boolean => LogEventSchema.safeParse(event).success;

function issueMessages(event: unknown): string[] {
  const result = LogEventSchema.safeParse(event);
  return result.success ? [] : result.error.issues.map((i) => i.message);
}

describe('LogEventSchema', () => {
  it('accepts a minimal event and a full one', () => {
    expect(accepts(valid)).toBe(true);
    expect(
      accepts({
        ...valid,
        host: 'web-1',
        traceId: '4bf92f3577b34da6a3ce929d0e0e4736',
        spanId: '00f067aa0ba902b7',
        durationMs: 12.5,
        attrs: { region: 'eu-west-1', retries: 2, cached: false },
      }),
    ).toBe(true);
  });

  it('strips unknown fields instead of rejecting them (schema evolution)', () => {
    expect(LogEventSchema.parse({ ...valid, futureField: 'x' })).toEqual(valid);
  });

  it('never takes tenantId or receivedAt from the client payload', () => {
    const parsed = LogEventSchema.parse({ ...valid, tenantId: 'evil', receivedAt: 1 });
    expect(parsed).toEqual(valid);
    expect(parsed).not.toHaveProperty('tenantId');
    expect(parsed).not.toHaveProperty('receivedAt');
  });

  it.each(LEVELS)('accepts level %s', (level) => {
    expect(accepts({ ...valid, level })).toBe(true);
  });

  it('accepts boundary values', () => {
    expect(accepts({ ...valid, ts: 0 })).toBe(true);
    expect(accepts({ ...valid, durationMs: 0 })).toBe(true);
    expect(accepts({ ...valid, message: '' })).toBe(true);
    expect(accepts({ ...valid, host: 'h'.repeat(MAX_HOST_LENGTH) })).toBe(true);
    expect(accepts({ ...valid, traceId: 't'.repeat(MAX_TRACE_ID_LENGTH) })).toBe(true);
    expect(accepts({ ...valid, spanId: 's'.repeat(MAX_TRACE_ID_LENGTH) })).toBe(true);
    expect(accepts({ ...valid, attrs: { ['k'.repeat(MAX_ATTR_KEY_LENGTH)]: 1 } })).toBe(true);
  });

  it.each([
    ['schemaVersion 2', { schemaVersion: 2 }],
    ['missing id', { id: undefined }],
    ['lower-case ULID', { id: '01j9z8y7x6w5v4t3s2r1q0p9n8' }],
    ['ULID with I', { id: '01J9Z8Y7X6W5V4T3S2R1Q0P9NI' }],
    ['ULID too short', { id: '01J9Z8Y7X6W5V4T3S2R1Q0P9N' }],
    ['ULID overflowing 48-bit time', { id: '81J9Z8Y7X6W5V4T3S2R1Q0P9N8' }],
    ['fractional ts', { ts: 1.5 }],
    ['negative ts', { ts: -1 }],
    ['ts beyond the JS Date range', { ts: 8.64e15 + 1 }],
    ['string ts', { ts: '1700000000000' }],
    ['service with a colon', { service: 'check:out' }],
    ['upper-case env', { env: 'Prod' }],
    ['unknown level', { level: 'trace' }],
    ['non-string message', { message: 42 }],
    ['negative durationMs', { durationMs: -1 }],
    ['infinite durationMs', { durationMs: Number.POSITIVE_INFINITY }],
    ['empty host', { host: '' }],
    ['host too long', { host: 'h'.repeat(MAX_HOST_LENGTH + 1) }],
    ['traceId too long', { traceId: 't'.repeat(MAX_TRACE_ID_LENGTH + 1) }],
    ['spanId too long', { spanId: 's'.repeat(MAX_TRACE_ID_LENGTH + 1) }],
    ['attrs as an array', { attrs: ['a'] }],
    ['nested attr value', { attrs: { a: { b: 1 } } }],
    ['null attr value', { attrs: { a: null } }],
    ['empty attr key', { attrs: { '': 'x' } }],
    ['attr key too long', { attrs: { ['k'.repeat(MAX_ATTR_KEY_LENGTH + 1)]: 1 } }],
  ])('rejects %s', (_name, patch) => {
    expect(accepts({ ...valid, ...patch })).toBe(false);
  });

  it('rejects a __proto__ attr key instead of dropping it silently', () => {
    const attrs: unknown = JSON.parse('{"__proto__":"x","a":1}');
    expect(issueMessages({ ...valid, attrs })).toContain('reserved_attr_key');
  });

  it('accepts every generated identifier as service and env, and rejects everything else', () => {
    fc.assert(
      fc.property(fc.stringMatching(IDENTIFIER_PATTERN), (id) => {
        expect(accepts({ ...valid, service: id, env: id })).toBe(true);
      }),
    );
    fc.assert(
      fc.property(
        fc.string({ minLength: 1 }).filter((s) => !IDENTIFIER_PATTERN.test(s)),
        (bad) => {
          expect(accepts({ ...valid, service: bad })).toBe(false);
        },
      ),
    );
  });

  it('measures the message limit in UTF-8 bytes, rejecting with message_too_large', () => {
    expect(accepts({ ...valid, message: 'a'.repeat(MAX_MESSAGE_BYTES) })).toBe(true);
    expect(issueMessages({ ...valid, message: 'a'.repeat(MAX_MESSAGE_BYTES + 1) })).toEqual([
      'message_too_large',
    ]);
    // 'é' is 2 bytes: 4096 of them fit exactly, one more character does not.
    expect(accepts({ ...valid, message: 'é'.repeat(MAX_MESSAGE_BYTES / 2) })).toBe(true);
    expect(accepts({ ...valid, message: 'é'.repeat(MAX_MESSAGE_BYTES / 2) + 'a' })).toBe(false);
    // An emoji is 4 bytes but 2 UTF-16 code units.
    expect(accepts({ ...valid, message: '😀'.repeat(MAX_MESSAGE_BYTES / 4) })).toBe(true);
    expect(accepts({ ...valid, message: '😀'.repeat(MAX_MESSAGE_BYTES / 4) + 'a' })).toBe(false);
  });

  it('caps attrs at 32 keys and 256-character string values', () => {
    const attrs = (n: number): Record<string, number> =>
      Object.fromEntries(Array.from({ length: n }, (_, i) => [`k${String(i)}`, i]));
    expect(accepts({ ...valid, attrs: attrs(MAX_ATTR_KEYS) })).toBe(true);
    expect(issueMessages({ ...valid, attrs: attrs(MAX_ATTR_KEYS + 1) })).toEqual([
      'too_many_attrs',
    ]);
    expect(accepts({ ...valid, attrs: { a: 'x'.repeat(MAX_ATTR_VALUE_LENGTH) } })).toBe(true);
    expect(accepts({ ...valid, attrs: { a: 'x'.repeat(MAX_ATTR_VALUE_LENGTH + 1) } })).toBe(false);
  });
});

describe('RawEventSchema', () => {
  const raw = { ...valid, tenantId: 'acme', receivedAt: 1_700_000_000_100 };

  it('adds the server-assigned tenantId and receivedAt', () => {
    expect(RawEventSchema.parse(raw)).toEqual(raw);
  });

  it.each([
    ['missing tenantId', { tenantId: undefined }],
    ['tenantId with a colon', { tenantId: 'ac:me' }],
    ['fractional receivedAt', { receivedAt: 1.5 }],
  ])('rejects %s', (_name, patch) => {
    expect(RawEventSchema.safeParse({ ...raw, ...patch }).success).toBe(false);
  });
});

describe('IngestBatchSchema', () => {
  it('bounds the batch size but leaves each event unparsed for per-event rejection', () => {
    const events = (n: number): unknown[] => Array.from({ length: n }, () => ({ junk: true }));
    expect(IngestBatchSchema.safeParse({ events: events(1) }).success).toBe(true);
    expect(IngestBatchSchema.safeParse({ events: events(MAX_BATCH_EVENTS) }).success).toBe(true);
    expect(IngestBatchSchema.safeParse({ events: events(MAX_BATCH_EVENTS + 1) }).success).toBe(
      false,
    );
    expect(IngestBatchSchema.safeParse({ events: [] }).success).toBe(false);
    expect(IngestBatchSchema.safeParse({}).success).toBe(false);
  });
});
