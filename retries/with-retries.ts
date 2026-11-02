// Tries a failing call again, waiting between tries. This file knows nothing about Elasticsearch, so
// it can wrap any function that returns a promise.

export type RetrySettings = {
  // How many extra tries after the first one. 0 means never try again.
  maxRetries: number;
  // How long to wait before the first retry, in milliseconds.
  delayMs: number;
  // true doubles the wait after each retry (500, 1000, 2000...). false keeps it the same every time.
  exponential: boolean;
};

type RetryOptions = RetrySettings & {
  // Says whether an error can pass with time. If not, it's thrown right away, with no waiting.
  shouldRetry: (error: unknown) => boolean;
  // Called just before each wait, so the caller can say what's happening. `retry` counts from 1.
  onRetry: (error: unknown, retry: number, waitMs: number) => void;
};

// Takes a function and returns a new one that's called the same way but tries again on failure. When
// the retries run out, it throws the last error.
export function withRetries<Args extends unknown[], Result>(
  fn: (...args: Args) => Promise<Result>,
  options: RetryOptions,
): (...args: Args) => Promise<Result> {
  const { maxRetries, delayMs, exponential, shouldRetry, onRetry } = options;

  return async (...args) => {
    for (let retry = 1; ; retry++) {
      try {
        return await fn(...args);
      } catch (error) {
        if (retry > maxRetries || !shouldRetry(error)) {
          throw error;
        }
        const waitMs = exponential ? delayMs * 2 ** (retry - 1) : delayMs;
        onRetry(error, retry, waitMs);
        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }
    }
  };
}

// The longest these settings can spend waiting, when every retry is used.
export function longestTotalWaitMs({ maxRetries, delayMs, exponential }: RetrySettings): number {
  return exponential ? delayMs * (2 ** maxRetries - 1) : delayMs * maxRetries;
}
