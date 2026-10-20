import type { z } from 'zod';
import { describe, expect, it } from 'vitest';
import {
  AlertSchema,
  DlqEnvelopeSchema,
  LateEventSchema,
  MAX_COUNT,
  RejectedEventSchema,
  WINDOW_MS,
  WindowRecordSchema,
} from './index.js';

/** The issue messages for an input; [] means it parsed. Pins *which* rule rejected it. */
function issues(schema: z.ZodType, input: unknown): string[] {
  const result = schema.safeParse(input);
  return result.success ? [] : result.error.issues.map((i) => i.message);
}

const rawEvent = {
  schemaVersion: 1,
  id: '01J9Z8Y7X6W5V4T3S2R1Q0P9N8',
  ts: 1_700_000_040_500,
  service: 'checkout',
  env: 'prod',
  level: 'info',
  message: 'ok',
  tenantId: 'acme',
  receivedAt: 1_700_000_040_600,
};
const source = { topic: 'events.raw', partition: 3, offset: '42' };

describe('WindowRecordSchema', () => {
  const start = 1_699_999_980_000; // a multiple of 60_000
  const record = {
    tenantId: 'acme',
    service: 'checkout',
    env: 'prod',
    windowStart: start,
    windowEnd: start + WINDOW_MS,
    count: 10,
    countsByLevel: { debug: 1, info: 5, warn: 1, error: 2, fatal: 1 },
    durationCount: 4,
    p50: 12,
    p95: 40,
    p99: 41,
    rejectedCount: 3,
    closed: false,
    revision: 26,
  };
  const check = (patch: Record<string, unknown>): string[] =>
    issues(WindowRecordSchema, { ...record, ...patch });

  it('accepts a consistent record, open or closed', () => {
    expect(check({})).toEqual([]);
    expect(check({ closed: true, revision: 27 })).toEqual([]);
  });

  it('allows null percentiles when no event carried a duration', () => {
    expect(check({ durationCount: 0, p50: null, p95: null, p99: null })).toEqual([]);
  });

  it.each([
    ['countsByLevel not summing to count', { count: 11, revision: 28 }, 'must sum to count'],
    ['revision not matching the formula', { revision: 25 }, 'revision must equal'],
    ['an open revision on a closed record', { closed: true }, 'revision must equal'],
    ['window not one minute long', { windowEnd: start + WINDOW_MS + 1 }, 'windowEnd must be'],
    [
      'window start off the minute grid',
      { windowStart: start + 1, windowEnd: start + 1 + WINDOW_MS },
      'multiple of 60000',
    ],
    ['durationCount above count', { durationCount: 11 }, 'cannot exceed count'],
    ['percentiles without durations', { durationCount: 0 }, 'present exactly when'],
    ['durations without percentiles', { p95: null }, 'present exactly when'],
    ['percentiles out of order', { p50: 50 }, 'p50 <= p95 <= p99'],
    ['negative rejectedCount', { rejectedCount: -1 }, 'expected number to be >=0'],
    [
      'missing level count',
      { countsByLevel: { debug: 1, info: 5, warn: 1, error: 3 } },
      'expected number, received undefined',
    ],
    ['tenant with a colon', { tenantId: 'a:b' }, 'Invalid string'],
  ])('rejects %s', (_name, patch, message) => {
    const found = check(patch);
    expect(found).toHaveLength(1);
    expect(found[0]).toContain(message);
  });

  it('returns a failure, never throws, for counts at and beyond the bound', () => {
    const huge = {
      count: MAX_COUNT,
      countsByLevel: { debug: MAX_COUNT, info: 0, warn: 0, error: 0, fatal: 0 },
      durationCount: 0,
      p50: null,
      p95: null,
      p99: null,
      rejectedCount: MAX_COUNT,
    };
    expect(check({ ...huge, revision: 4 * MAX_COUNT })).toEqual([]);
    const over = { ...huge, count: Number.MAX_SAFE_INTEGER, revision: 0 };
    expect(() => WindowRecordSchema.safeParse({ ...record, ...over })).not.toThrow();
    expect(check(over).length).toBeGreaterThan(0);
  });
});

describe('AlertSchema', () => {
  const alert = {
    id: 'error-rate:acme:checkout:prod:1699999980000',
    ruleId: 'error-rate',
    tenantId: 'acme',
    service: 'checkout',
    env: 'prod',
    breachStartWindow: 1_699_999_980_000,
    lastWindowStart: 1_700_000_040_000,
    errorRate: 0.3,
    threshold: 0.05,
  };
  const check = (patch: Record<string, unknown>): string[] =>
    issues(AlertSchema, { ...alert, ...patch });

  it('accepts an alert whose id is derived from its fields', () => {
    expect(check({})).toEqual([]);
  });

  it.each([
    ['id not matching its fields', { id: 'error-rate:acme:checkout:prod:0' }, 'id must be derived'],
    ['last window before the breach', { lastWindowStart: 1_699_999_920_000 }, 'precedes'],
    [
      'breach start off the grid',
      { breachStartWindow: 5, id: 'error-rate:acme:checkout:prod:5' },
      'multiple of 60000',
    ],
    ['last window off the grid', { lastWindowStart: 1_700_000_040_007 }, 'multiple of 60000'],
    ['error rate above 1', { errorRate: 1.5 }, 'expected number to be <=1'],
    ['negative threshold', { threshold: -0.1 }, 'expected number to be >=0'],
  ])('rejects %s', (_name, patch, message) => {
    const found = check(patch);
    expect(found).toHaveLength(1);
    expect(found[0]).toContain(message);
  });
});

describe('side-topic records', () => {
  const envelope = {
    originalBase64: 'eyJiYWQiOg==',
    stage: 'indexer',
    error: 'Unexpected end of JSON input',
    attempts: 3,
    source,
    failedAt: 1_700_000_041_000,
  };

  it('accepts a DLQ envelope with the original bytes and source position', () => {
    expect(issues(DlqEnvelopeSchema, envelope)).toEqual([]);
  });

  it.each([
    ['bytes not base64', { originalBase64: 'not base64!' }],
    ['zero attempts', { attempts: 0 }],
    ['numeric offset', { source: { ...source, offset: 42 } }],
    ['topic named ..', { source: { ...source, topic: '..' } }],
    ['error over 4 KB', { error: 'x'.repeat(4097) }],
  ])('rejects a DLQ envelope with %s', (_name, patch) => {
    expect(issues(DlqEnvelopeSchema, { ...envelope, ...patch })).toHaveLength(1);
  });

  it('rejected events carry the event, a known reason, and their source', () => {
    expect(issues(RejectedEventSchema, { event: rawEvent, reason: 'window_cap', source })).toEqual(
      [],
    );
    expect(issues(RejectedEventSchema, { event: rawEvent, reason: 'dedup_cap', source })).toEqual(
      [],
    );
    expect(
      issues(RejectedEventSchema, { event: rawEvent, reason: 'too_late', source }),
    ).toHaveLength(1);
  });

  describe('late events', () => {
    const windowStart = 1_700_000_040_000; // contains rawEvent.ts
    const late = { event: rawEvent, windowStart, watermark: windowStart + WINDOW_MS, source };
    const check = (patch: Record<string, unknown>): string[] =>
      issues(LateEventSchema, { ...late, ...patch });

    it('accept the finalized window and the watermark that closed it', () => {
      expect(check({})).toEqual([]);
    });

    it.each([
      [
        'watermark before the window end',
        { watermark: windowStart + WINDOW_MS - 1 },
        'watermark at or past',
      ],
      [
        'a window that does not contain the event',
        { windowStart: windowStart + WINDOW_MS, watermark: windowStart + 2 * WINDOW_MS },
        "event's own window",
      ],
      ['a window start off the grid', { windowStart: 5 }, 'multiple of 60000'],
    ])('reject %s', (_name, patch, message) => {
      const found = check(patch);
      expect(found).toHaveLength(1);
      expect(found[0]).toContain(message);
    });
  });
});
