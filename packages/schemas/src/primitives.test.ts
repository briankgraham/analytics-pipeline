import { describe, expect, it } from 'vitest';
import { EpochMs, MAX_EPOCH_MS, Topic, utf8ByteLength, WindowStart } from './index.js';

describe('utf8ByteLength', () => {
  it.each([
    ['', 0],
    ['abc', 3],
    ['é', 2], // U+00E9
    ['߿', 2], // U+07FF, the last 2-byte code point
    ['ࠀ', 3], // U+0800, the first 3-byte code point
    ['日本', 6],
    ['😀', 4], // astral: a surrogate pair
    ['\uD800', 3], // a lone surrogate encodes as U+FFFD
    ['a😀é', 7],
  ])('counts %j as %i bytes', (text, bytes) => {
    expect(utf8ByteLength(text)).toBe(bytes);
  });
});

describe('primitives', () => {
  it('bounds epoch ms to the JS Date range', () => {
    expect(EpochMs.safeParse(MAX_EPOCH_MS).success).toBe(true);
    expect(EpochMs.safeParse(MAX_EPOCH_MS + 1).success).toBe(false);
  });

  it('accepts only window starts on the minute grid', () => {
    expect(WindowStart.safeParse(0).success).toBe(true);
    expect(WindowStart.safeParse(120_000).success).toBe(true);
    expect(WindowStart.safeParse(120_001).success).toBe(false);
  });

  it.each(['events.raw', 'a', 'x'.repeat(249), '...', 'a..b'])('accepts topic %j', (topic) => {
    expect(Topic.safeParse(topic).success).toBe(true);
  });

  it.each(['.', '..', '', 'x'.repeat(250), 'events:raw', 'events raw'])(
    'rejects topic %j',
    (topic) => {
      expect(Topic.safeParse(topic).success).toBe(false);
    },
  );
});
