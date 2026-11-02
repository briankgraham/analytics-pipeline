import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { longestWaitMs, withRetries } from '../../retries/with-retries.ts';

const settings = { maxRetries: 3, delayMs: 100, exponential: true, jitter: false };
const retryAnything = { ...settings, shouldRetry: () => true, onRetry: () => {} };

// A function that fails the given number of times, then returns 'done'.
function failing(times: number) {
  let failures = 0;
  return vi.fn(async (..._args: unknown[]) => {
    if (failures < times) {
      failures += 1;
      throw new Error(`failure ${failures}`);
    }
    return 'done';
  });
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('withRetries', () => {
  it('calls the function once when it works, passing its arguments and returning its result', async () => {
    const fn = failing(0);
    await expect(withRetries(fn, retryAnything)('a', 1)).resolves.toBe('done');
    expect(fn).toHaveBeenCalledOnce();
    expect(fn).toHaveBeenCalledWith('a', 1);
  });

  it('passes the same arguments on every try', async () => {
    const fn = failing(1);
    const result = withRetries(fn, retryAnything)('a');
    await vi.runAllTimersAsync();
    await expect(result).resolves.toBe('done');
    expect(fn.mock.calls).toEqual([['a'], ['a']]);
  });

  it.each([
    ['doubles the wait when exponential', true, [100, 200, 400]],
    ['keeps the wait the same when not exponential', false, [100, 100, 100]],
  ])('%s', async (_name, exponential, waits) => {
    const onRetry = vi.fn();
    const fn = failing(3);
    const result = withRetries(fn, { ...retryAnything, exponential, onRetry })();
    await vi.runAllTimersAsync();
    await expect(result).resolves.toBe('done');
    expect(onRetry.mock.calls.map(([, retry, waitMs]) => [retry, waitMs])).toEqual(
      waits.map((waitMs, index) => [index + 1, waitMs]),
    );
  });

  it.each([
    ['grows the range when exponential', true, 0.5, [50, 100, 200]],
    ['keeps the range the same when not exponential', false, 0.5, [50, 50, 50]],
    ['can wait no time at all', true, 0, [0, 0, 0]],
    ['stays under the longest wait', true, 0.999, [99, 199, 399]],
  ])('with jitter, %s', async (_name, exponential, random, waits) => {
    vi.spyOn(Math, 'random').mockReturnValue(random);
    const onRetry = vi.fn();
    const result = withRetries(failing(3), {
      ...retryAnything,
      exponential,
      jitter: true,
      onRetry,
    })();
    await vi.runAllTimersAsync();
    await expect(result).resolves.toBe('done');
    expect(onRetry.mock.calls.map(([, , waitMs]) => waitMs)).toEqual(waits);
  });

  it('with jitter, really waits for the time it picked', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const fn = failing(1);
    const result = withRetries(fn, { ...retryAnything, jitter: true })();
    await vi.advanceTimersByTimeAsync(49);
    expect(fn).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fn).toHaveBeenCalledTimes(2);
    await expect(result).resolves.toBe('done');
  });

  it('really waits before trying again', async () => {
    const fn = failing(1);
    const result = withRetries(fn, retryAnything)();
    await vi.advanceTimersByTimeAsync(99);
    expect(fn).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fn).toHaveBeenCalledTimes(2);
    await expect(result).resolves.toBe('done');
  });

  it('throws the last error when the retries run out', async () => {
    const fn = failing(10);
    const result = withRetries(fn, retryAnything)();
    const failed = expect(result).rejects.toThrow('failure 4');
    await vi.runAllTimersAsync();
    await failed;
    expect(fn).toHaveBeenCalledTimes(4);
  });

  it('waits for onRetry, and stops with its error if it throws', async () => {
    const fn = failing(1);
    const onRetry = async () => {
      throw new Error('must stop');
    };
    await expect(withRetries(fn, { ...retryAnything, onRetry })()).rejects.toThrow('must stop');
    expect(fn).toHaveBeenCalledOnce();
  });

  it('tries only once when maxRetries is 0', async () => {
    const fn = failing(1);
    await expect(withRetries(fn, { ...retryAnything, maxRetries: 0 })()).rejects.toThrow(
      'failure 1',
    );
    expect(fn).toHaveBeenCalledOnce();
  });

  it('throws right away, with no waiting, when shouldRetry says no', async () => {
    const onRetry = vi.fn();
    const shouldRetry = vi.fn(() => false);
    const fn = failing(1);
    await expect(withRetries(fn, { ...settings, shouldRetry, onRetry })()).rejects.toThrow(
      'failure 1',
    );
    expect(fn).toHaveBeenCalledOnce();
    expect(shouldRetry).toHaveBeenCalledOnce();
    expect(onRetry).not.toHaveBeenCalled();
  });
});

describe('longestWaitMs', () => {
  it('finds the longest single wait', () => {
    expect(longestWaitMs(settings)).toBe(400);
    expect(longestWaitMs({ ...settings, exponential: false })).toBe(100);
    expect(longestWaitMs({ ...settings, maxRetries: 0 })).toBe(0);
    expect(longestWaitMs({ ...settings, jitter: true })).toBe(400);
  });
});
