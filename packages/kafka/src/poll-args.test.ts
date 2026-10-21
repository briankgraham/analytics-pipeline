import { describe, expect, it } from 'vitest';
import { checkPollArgs } from './poll-args.js';

describe('checkPollArgs', () => {
  it('accepts a positive integer max and a finite non-negative timeout', () => {
    expect(() => {
      checkPollArgs(1, 0);
    }).not.toThrow();
  });

  it.each([
    [0, 0],
    [-1, 0],
    [1.5, 0],
    [Number.NaN, 0],
    [1, -1],
    [1, Number.NaN],
    [1, Number.POSITIVE_INFINITY],
  ])('refuses max %d with timeout %d', (max, timeoutMs) => {
    expect(() => {
      checkPollArgs(max, timeoutMs);
    }).toThrow(RangeError);
  });
});
