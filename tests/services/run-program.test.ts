import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { runProgram, type StopProgram } from '../../services/run-program.ts';

// Keeps hold of what runProgram listens for, instead of adding listeners to the test's own process.
const listeners = new Map<string, (...args: unknown[]) => void>();
const realExitCode = process.exitCode;

function standInService() {
  return {
    init: vi.fn(async () => {}),
    close: vi.fn(async () => {}),
  };
}

beforeEach(() => {
  listeners.clear();
  const listen = (event: string | symbol, listener: (...args: unknown[]) => void) => {
    listeners.set(String(event), listener);
    return process;
  };
  vi.spyOn(process, 'on').mockImplementation(listen);
  vi.spyOn(process, 'once').mockImplementation(listen);
  vi.spyOn(console, 'error').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.useFakeTimers();
});

afterEach(() => {
  process.exitCode = realExitCode;
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('runProgram', () => {
  it('starts the service and leaves it running', async () => {
    const service = standInService();
    await runProgram('test', () => service);
    expect(service.init).toHaveBeenCalledOnce();
    expect(service.close).not.toHaveBeenCalled();
  });

  it.each(['SIGINT', 'SIGTERM'])('closes on %s, as a normal exit', async (signal) => {
    const service = standInService();
    await runProgram('test', () => service);
    listeners.get(signal)?.();
    expect(service.close).toHaveBeenCalledOnce();
    expect(process.exitCode).toBe(realExitCode);
  });

  it('closes and reports a failure when start-up fails', async () => {
    const service = standInService();
    service.init.mockRejectedValue(new Error('port in use'));
    await runProgram('test', () => service);
    expect(service.close).toHaveBeenCalledOnce();
    expect(process.exitCode).toBe(1);
  });

  it.each(['uncaughtException', 'unhandledRejection'])(
    'closes and reports a failure on %s',
    async (event) => {
      const service = standInService();
      await runProgram('test', () => service);
      listeners.get(event)?.(new Error('nothing caught this'));
      expect(service.close).toHaveBeenCalledOnce();
      expect(process.exitCode).toBe(1);
    },
  );

  it('closes and reports a failure when the program asks to stop', async () => {
    const service = standInService();
    let stop: StopProgram = () => {};
    await runProgram('test', (stopProgram) => {
      stop = stopProgram;
      return service;
    });
    stop(new Error('gave up'));
    expect(service.close).toHaveBeenCalledOnce();
    expect(process.exitCode).toBe(1);
  });

  it('closes only once, whatever happens while closing', async () => {
    const service = standInService();
    await runProgram('test', () => service);
    listeners.get('SIGINT')?.();
    listeners.get('SIGTERM')?.();
    listeners.get('uncaughtException')?.(new Error('during closing'));
    expect(service.close).toHaveBeenCalledOnce();
    expect(process.exitCode).toBe(1);
  });

  it('does not count start-up failing as an error when closing interrupted it', async () => {
    const service = standInService();
    let failStartUp: (error: Error) => void = () => {};
    service.init.mockReturnValue(new Promise((_, reject) => (failStartUp = reject)));
    const running = runProgram('test', () => service);
    listeners.get('SIGINT')?.();
    failStartUp(new Error('closed'));
    await running;
    expect(process.exitCode).toBe(realExitCode);
    expect(console.warn).toHaveBeenCalledWith('test: start-up did not finish:', expect.any(Error));
  });

  it('reports a failure when closing fails', async () => {
    const service = standInService();
    service.close.mockRejectedValue(new Error('stuck'));
    await runProgram('test', () => service);
    listeners.get('SIGINT')?.();
    await vi.advanceTimersByTimeAsync(0);
    expect(process.exitCode).toBe(1);
  });

  it('stops anyway when closing never finishes', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation(() => undefined as never);
    const service = standInService();
    service.close.mockReturnValue(new Promise(() => {}));
    await runProgram('test', () => service);
    listeners.get('SIGINT')?.();
    expect(exit).not.toHaveBeenCalled();
    vi.advanceTimersByTime(120_000);
    expect(exit).toHaveBeenCalledWith(1);
  });
});
