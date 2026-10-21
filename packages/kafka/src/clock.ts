/** Injected time (docs/design.md, Determinism): domain and plumbing never call Date.now() directly. */
export interface Clock {
  now(): number;
}

export const systemClock: Clock = { now: () => Date.now() };

/** A clock tests move by hand. */
export class FakeClock implements Clock {
  private current: number;

  constructor(start = 0) {
    this.current = start;
  }

  now(): number {
    return this.current;
  }

  advance(ms: number): void {
    if (!Number.isFinite(ms) || ms < 0) throw new RangeError(`cannot advance by ${String(ms)} ms`);
    this.current += ms;
  }
}
