import { describe, expect, it } from 'vitest';
import { readLog } from '../../logs/log.ts';

const good = { service: 'checkout', level: 'error', message: 'card declined' };

describe('readLog', () => {
  it('accepts a log', () => {
    expect(readLog(good)).toEqual({ ok: true, log: good });
  });

  it('accepts every level', () => {
    for (const level of ['debug', 'info', 'warn', 'error']) {
      expect(readLog({ ...good, level }).ok).toBe(true);
    }
  });

  it('drops fields that are not part of a log', () => {
    expect(readLog({ ...good, password: 'hunter2' })).toEqual({ ok: true, log: good });
  });

  it.each([
    ['null', null],
    ['an array', [good]],
    ['text', 'hello'],
    ['a number', 5],
  ])('rejects %s', (_name, input) => {
    expect(readLog(input)).toEqual({ ok: false, problem: 'a log must be a JSON object' });
  });

  it.each([
    ['missing', undefined],
    ['not text', 5],
    ['empty', ''],
    ['only spaces', '   '],
    ['too long', 'x'.repeat(101)],
  ])('rejects a service that is %s', (_name, service) => {
    expect(readLog({ ...good, service }).ok).toBe(false);
  });

  it.each([
    ['missing', undefined],
    ['not text', 3],
    ['unknown', 'fatal'],
    ['the wrong case', 'ERROR'],
  ])('rejects a level that is %s', (_name, level) => {
    expect(readLog({ ...good, level })).toEqual({
      ok: false,
      problem: 'level must be one of: debug, info, warn, error',
    });
  });

  it.each([
    ['missing', undefined],
    ['not text', { text: 'hi' }],
    ['empty', ''],
    ['only spaces', '  '],
    ['too long', 'x'.repeat(10_001)],
  ])('rejects a message that is %s', (_name, message) => {
    expect(readLog({ ...good, message }).ok).toBe(false);
  });

  it('accepts the longest allowed message', () => {
    expect(readLog({ ...good, message: 'x'.repeat(10_000) }).ok).toBe(true);
  });
});
