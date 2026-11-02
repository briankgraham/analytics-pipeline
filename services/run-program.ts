// The one path every program uses to start and stop. It starts the program's service, and closes it
// for any reason we can control: Ctrl+C, a plain `kill`, a failure during start-up, an error nothing
// else caught, or the program itself asking to stop.
import type { Service } from './service.ts';

// Closing waits for work in progress. If it hasn't finished by then, it's stuck, so stop anyway. Logs
// that weren't finished are read again from Kafka on the next start.
const longestCloseMs = 120_000;

// Lets a program stop itself because of an error it can't recover from.
export type StopProgram = (error: unknown) => void;

// `createService` gets the function that stops the program, for the service to call if it needs to.
// Resolves once start-up has finished or failed. Closing may still be going on after that.
export async function runProgram(
  name: string,
  createService: (stop: StopProgram) => Service,
): Promise<void> {
  let closing = false;

  // Filled in just below, before anything can call it.
  let service: Service | undefined;

  const close = () => {
    if (closing) {
      return;
    }
    closing = true;
    setTimeout(() => {
      console.error(`${name}: still closing after ${longestCloseMs} ms, stopping anyway`);
      process.exit(1);
    }, longestCloseMs).unref();
    service?.close().catch((error: unknown) => {
      console.error(`${name}: could not close cleanly:`, error);
      process.exitCode = 1;
    });
  };

  const stop: StopProgram = (error) => {
    console.error(`${name}: stopping because of an error:`, error);
    process.exitCode = 1;
    close();
  };

  service = createService(stop);

  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.once(signal, close);
  }
  process.on('uncaughtException', stop);
  process.on('unhandledRejection', stop);

  try {
    await service.init();
  } catch (error) {
    if (closing) {
      // Usually this is just closing interrupting start-up, so it doesn't count as a failure. It may be a
      // real problem, though, so say what it was.
      console.warn(`${name}: start-up did not finish:`, error);
    } else {
      stop(error);
    }
  }
}
