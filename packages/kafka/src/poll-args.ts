/**
 * Both adapters refuse the same poll arguments. The native binding reads a `max` of 0 (or NaN) as
 * its deprecated flowing mode, which never settles the poll, and a negative timeout as "forever".
 */
export function checkPollArgs(max: number, timeoutMs: number): void {
  if (!Number.isSafeInteger(max) || max < 1) {
    throw new RangeError(`poll max must be a positive integer, got ${String(max)}`);
  }
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new RangeError(`poll timeoutMs must be a finite number >= 0, got ${String(timeoutMs)}`);
  }
}
