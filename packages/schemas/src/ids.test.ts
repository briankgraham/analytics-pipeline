import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import {
  alertId,
  checkpointId,
  dlqKey,
  eventDocId,
  IDENTIFIER_PATTERN,
  revision,
  WINDOW_MS,
  windowId,
} from './index.js';

const identifier = fc.stringMatching(IDENTIFIER_PATTERN);
const ulid = fc.stringMatching(/^[0-7][0-9A-HJKMNP-TV-Z]{25}$/);
const windowStart = fc.integer({ min: 0, max: 68_374_080 }).map((minute) => minute * WINDOW_MS);

describe('composite IDs', () => {
  it('builds the documented shapes', () => {
    expect(eventDocId('acme', '01J9Z8Y7X6W5V4T3S2R1Q0P9N8')).toBe(
      'acme:01J9Z8Y7X6W5V4T3S2R1Q0P9N8',
    );
    expect(windowId('acme', 'checkout', 'prod', 1_700_000_040_000)).toBe(
      'acme:checkout:prod:1700000040000',
    );
    expect(alertId('error-rate', 'acme', 'checkout', 'prod', 1_700_000_040_000)).toBe(
      'error-rate:acme:checkout:prod:1700000040000',
    );
    expect(checkpointId('aggregator', 7)).toBe('aggregator:7');
    expect(checkpointId('alert-evaluator', 7)).toBe('alert-evaluator:7');
    expect(dlqKey('events.raw', 3, '12345')).toBe('events.raw:3:12345');
  });

  it('scopes event doc IDs by tenant: the same event id from two tenants never collides', () => {
    fc.assert(
      fc.property(identifier, identifier, ulid, (a, b, id) => {
        fc.pre(a !== b);
        expect(eventDocId(a, id)).not.toBe(eventDocId(b, id));
      }),
    );
  });

  it('is unambiguous: a window ID splits back into exactly its parts', () => {
    fc.assert(
      fc.property(
        identifier,
        identifier,
        identifier,
        windowStart,
        (tenant, service, env, start) => {
          expect(windowId(tenant, service, env, start).split(':')).toEqual([
            tenant,
            service,
            env,
            String(start),
          ]);
        },
      ),
    );
  });

  it('is unambiguous: an alert ID splits back into exactly its parts', () => {
    fc.assert(
      fc.property(
        identifier,
        identifier,
        identifier,
        identifier,
        windowStart,
        (rule, tenant, service, env, start) => {
          expect(alertId(rule, tenant, service, env, start).split(':')).toEqual([
            rule,
            tenant,
            service,
            env,
            String(start),
          ]);
        },
      ),
    );
  });

  it.each([
    ['tenant with a colon', () => eventDocId('a:b', '01J9Z8Y7X6W5V4T3S2R1Q0P9N8')],
    ['non-ULID event id', () => eventDocId('acme', 'not-a-ulid')],
    ['upper-case service', () => windowId('acme', 'Checkout', 'prod', 0)],
    ['empty env', () => windowId('acme', 'checkout', '', 0)],
    ['negative window start', () => windowId('acme', 'checkout', 'prod', -1)],
    ['fractional window start', () => windowId('acme', 'checkout', 'prod', 1.5)],
    ['window start off the minute grid', () => windowId('acme', 'checkout', 'prod', 1_000)],
    ['alert breach start off the grid', () => alertId('r', 'acme', 'checkout', 'prod', 1_000)],
    ['rule id with a colon', () => alertId('a:b', 'acme', 'checkout', 'prod', 0)],
    ['negative partition', () => checkpointId('aggregator', -1)],
    ['fractional partition', () => checkpointId('aggregator', 0.5)],
    ['non-decimal offset', () => dlqKey('events.raw', 0, '0x10')],
    ['offset with leading zero', () => dlqKey('events.raw', 0, '01')],
    ['topic with a colon', () => dlqKey('events:raw', 0, '1')],
  ])('rejects %s', (_name, build) => {
    expect(build).toThrow(RangeError);
  });

  it('accepts offsets beyond Number.MAX_SAFE_INTEGER (offsets are strings, never numbers)', () => {
    expect(dlqKey('events.raw', 0, '9007199254740993')).toBe('events.raw:0:9007199254740993');
  });
});

describe('revision', () => {
  it('is (count + rejectedCount) * 2 + closed', () => {
    expect(revision(0, 0, false)).toBe(0);
    expect(revision(0, 0, true)).toBe(1);
    expect(revision(10, 3, false)).toBe(26);
    expect(revision(10, 3, true)).toBe(27);
  });

  it('strictly increases when an event is folded, a rejection is counted, or the window closes', () => {
    const count = fc.integer({ min: 0, max: 1_000_000 });
    fc.assert(
      fc.property(count, count, (c, r) => {
        const open = revision(c, r, false);
        expect(revision(c + 1, r, false)).toBeGreaterThan(open);
        expect(revision(c, r + 1, false)).toBeGreaterThan(open);
        expect(revision(c, r, true)).toBeGreaterThan(open);
        // A later open update never loses to an earlier closed record of the same window: closing
        // only happens once, after the last update, so this ordering is the one that matters.
        expect(revision(c + 1, r, false)).toBeGreaterThan(revision(c, r, true));
      }),
    );
  });

  it.each([
    [-1, 0],
    [0, -1],
    [1.5, 0],
    [0, Number.NaN],
    [Number.MAX_SAFE_INTEGER, 0],
  ])('rejects invalid counts (%s, %s)', (count, rejected) => {
    expect(() => revision(count, rejected, false)).toThrow(RangeError);
  });
});
