// How we save logs, built on the base layer in client.ts. The worker gets a plain `saveLogs` function,
// so it can be tested without Elasticsearch.
import type { Log } from '../logs/log.ts';
import { withRetries, type RetrySettings } from '../retries/with-retries.ts';
import {
  canPassWithTime,
  isElasticsearchError,
  warnAboutRetry,
  type ElasticsearchCalls,
} from './client.ts';

// `id` becomes the log's id in Elasticsearch. Sending the same id again overwrites the log, so a log
// that's sent twice is still stored once.
export type LogToSave = { id: string; log: Log };
export type LogNotSaved = { id: string; reason: string };

// Anything Elasticsearch didn't save counts as not saved, whatever the reason. The logs reaching this
// file have already passed readLog, so most refusals say something is wrong with Elasticsearch or the
// index, not with the log, and the log must not be thrown away. The exception is a 400, which means
// Elasticsearch looked at that log and refused it: it's reported separately, as a bad request.
export type SaveAnswer = {
  savedCount: number;
  // Not saved, and any retries have been used up.
  unsavedLogs: LogNotSaved[];
  // Elasticsearch answered 400 for these. They were sent once and never again, so a bad log doesn't
  // keep hitting the database. The caller decides where they go.
  badRequestLogs: LogNotSaved[];
};

// Saves logs with one bulk request, then retries only the ones that weren't saved, except those that
// got a 400, which are set aside after the first answer. `keepAlive` is called before each wait, so
// Kafka knows the worker is still working. Problems with Elasticsearch are reported in the answer. It
// only throws if keepAlive does, or if the client isn't running.
export type SaveLogs = (logs: LogToSave[], keepAlive: () => Promise<void>) => Promise<SaveAnswer>;

// Thrown inside a round of saving when some logs are left, so that withRetries waits and goes again.
class LogsStillWaiting extends Error {}

// Returns the function that saves logs into the given index.
export function createLogSaver(
  client: ElasticsearchCalls,
  index: string,
  retries: RetrySettings,
): SaveLogs {
  return async (logs, keepAlive) => {
    let waiting = logs;
    let savedCount = 0;
    const reasons = new Map<string, string>();
    const badRequestLogs: LogNotSaved[] = [];

    // Sends every log still waiting, and keeps only those Elasticsearch didn't save.
    const sendWaitingLogs = async () => {
      const documents = waiting.map(({ id, log }) => ({ id, document: log }));
      const answers = await client.saveDocuments(index, documents);

      for (const answer of answers) {
        if (answer.saved) {
          savedCount += 1;
        } else if (answer.badRequest) {
          badRequestLogs.push({ id: answer.id, reason: answer.reason });
        } else {
          reasons.set(answer.id, answer.reason);
        }
      }
      const unsavedIds = new Set(
        answers.filter((answer) => !answer.saved && !answer.badRequest).map(({ id }) => id),
      );
      waiting = waiting.filter(({ id }) => unsavedIds.has(id));

      if (waiting.length > 0) {
        throw new LogsStillWaiting(`${waiting.length} logs not saved yet`);
      }
    };

    const sendWithRetries = withRetries(sendWaitingLogs, {
      ...retries,
      shouldRetry: (error) => error instanceof LogsStillWaiting || canPassWithTime(error),
      onRetry: async (error, retry, waitMs) => {
        warnAboutRetry(error, retry, waitMs);
        await keepAlive();
      },
    });

    let requestProblem = '';
    try {
      await sendWithRetries();
    } catch (error) {
      if (!(error instanceof LogsStillWaiting) && !isElasticsearchError(error)) {
        throw error;
      }
      requestProblem = error.message;
    }

    return {
      savedCount,
      badRequestLogs,
      unsavedLogs: waiting.map(({ id }) => ({
        id,
        reason: reasons.get(id) ?? requestProblem,
      })),
    };
  };
}
