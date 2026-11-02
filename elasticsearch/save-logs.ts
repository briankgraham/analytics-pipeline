// The only file that talks to Elasticsearch. The worker gets a plain `saveLogs` function, so it can be
// tested without a running Elasticsearch.
import { Client, errors } from '@elastic/elasticsearch';
import type { Log } from '../logs/log.ts';
import { withRetries, type RetrySettings } from '../retries/with-retries.ts';

// `id` becomes the log's id in Elasticsearch. Sending the same id again overwrites the log, so a log
// that's sent twice is still stored once.
export type LogToSave = { id: string; log: Log };
export type LogNotSaved = { id: string; reason: string };

// Anything Elasticsearch didn't save counts as not saved, whatever the reason. The logs reaching this
// file have already passed readLog, so a refusal says something is wrong with Elasticsearch or the
// index, not with the log, and the log must not be thrown away.
export type SaveAnswer = {
  savedCount: number;
  // Not saved, and any retries have been used up.
  unsavedLogs: LogNotSaved[];
};

// Saves logs with one bulk request, then retries only the ones that weren't saved. `keepAlive` is
// called before each wait, so Kafka knows the worker is still working. It only throws if keepAlive
// does; problems with Elasticsearch are reported in the answer.
export type SaveLogs = (logs: LogToSave[], keepAlive: () => Promise<void>) => Promise<SaveAnswer>;

export type ElasticsearchConnection = {
  saveLogs: SaveLogs;
  close: () => Promise<void>;
};

// How Elasticsearch should store each field of a log. `keyword` keeps the whole value as one exact
// piece, good for filters like "only errors". `text` splits it into words, so a search for "card"
// finds "card declined".
const logFields = {
  service: { type: 'keyword' },
  level: { type: 'keyword' },
  message: { type: 'text' },
} as const;

// Thrown inside a round of saving when some logs are left, so that withRetries waits and goes again.
class LogsStillWaiting extends Error {}

// Answers that can pass with time: too busy (429), a gateway or server that's briefly unavailable
// (502, 503, 504), and not getting an answer at all. Anything else, like a 401, needs a person.
function canPassWithTime(error: unknown): boolean {
  if (error instanceof errors.ResponseError) {
    return [429, 502, 503, 504].includes(error.statusCode ?? 0);
  }
  return (
    error instanceof LogsStillWaiting ||
    error instanceof errors.ConnectionError ||
    error instanceof errors.TimeoutError ||
    error instanceof errors.NoLivingConnectionsError
  );
}

function warnAboutRetry(error: unknown, retry: number, waitMs: number): void {
  const why = error instanceof Error ? error.message : String(error);
  console.warn(`elasticsearch: ${why}, retry ${retry} in ${waitMs} ms`);
}

// Finds which of our fields an existing index stores differently from logFields, in words. Other
// fields don't matter, because we never send them.
export function describeWrongFields(properties: Record<string, { type?: string }>): string[] {
  return Object.entries(logFields).flatMap(([name, { type }]) => {
    const actual = properties[name]?.type;
    if (actual === type) {
      return [];
    }
    const found =
      actual === undefined ? 'has no type (it is missing, or holds an object)' : `is ${actual}`;
    return [`${name} ${found}, should be ${type}`];
  });
}

// Connects to Elasticsearch, creates the index if it's missing, and returns a function that saves
// logs into it. Throws if Elasticsearch still can't be reached after the retries. An index that
// already exists is left as it is, but it must store our fields the way we expect. If it doesn't,
// good logs would be refused, so this throws and the worker doesn't start.
export async function connectToElasticsearch(
  url: string,
  index: string,
  replicas: number,
  requestTimeoutMs: number,
  retries: RetrySettings,
): Promise<ElasticsearchConnection> {
  // maxRetries: 0 turns off the client's own instant retries, which would multiply with ours and
  // hit a struggling Elasticsearch with no wait at all.
  const client = new Client({ node: url, maxRetries: 0, requestTimeout: requestTimeoutMs });

  const retryOptions = { ...retries, shouldRetry: canPassWithTime, onRetry: warnAboutRetry };
  const createIndex = withRetries(
    () =>
      client.indices.create({
        index,
        settings: { number_of_replicas: replicas },
        mappings: { properties: logFields },
      }),
    retryOptions,
  );
  const readExistingFields = withRetries(() => client.indices.getMapping({ index }), retryOptions);

  try {
    try {
      await createIndex();
    } catch (error) {
      const alreadyExists =
        error instanceof errors.ResponseError &&
        error.body?.error?.type === 'resource_already_exists_exception';
      if (!alreadyExists) {
        throw error;
      }
      const existing = await readExistingFields();
      const wrongFields = Object.values(existing).flatMap(({ mappings }) =>
        describeWrongFields((mappings.properties ?? {}) as Record<string, { type?: string }>),
      );
      if (wrongFields.length > 0) {
        throw new Error(
          `the "${index}" index stores fields the wrong way (${wrongFields.join('; ')}). ` +
            "Good logs would be refused, and a field can't be changed in place. Deleting the index also " +
            'deletes the logs already saved in it; see README.md for how to save them again from Kafka.',
        );
      }
    }
  } catch (error) {
    await client.close();
    throw error;
  }

  return {
    // A just-saved log shows up in searches within about a second.
    saveLogs: async (logs, keepAlive) => {
      let waiting = logs;
      let savedCount = 0;
      const reasons = new Map<string, string>();

      // Sends every log still waiting, and sorts them by the answer Elasticsearch gave for each one.
      const sendWaitingLogs = async () => {
        const operations = waiting.flatMap(({ id, log }) => [
          { index: { _index: index, _id: id } },
          log,
        ]);
        // The signal limits the whole request. The client's requestTimeout only limits each wait for
        // part of an answer, so a slow request could outlast it.
        const { items } = await client.bulk(
          { operations },
          { signal: AbortSignal.timeout(requestTimeoutMs) },
        );

        waiting = waiting.filter(({ id }, position) => {
          const answer = items[position]?.index;
          const status = answer?.status ?? 0;
          if (status >= 200 && status < 300) {
            savedCount += 1;
            return false;
          }
          const reason = answer?.error?.reason ?? answer?.error?.type ?? `status ${status}`;
          reasons.set(id, `${status}: ${reason}`);
          return true;
        });

        if (waiting.length > 0) {
          throw new LogsStillWaiting(`${waiting.length} logs not saved yet`);
        }
      };

      const sendWithRetries = withRetries(sendWaitingLogs, {
        ...retries,
        shouldRetry: canPassWithTime,
        onRetry: async (error, retry, waitMs) => {
          warnAboutRetry(error, retry, waitMs);
          await keepAlive();
        },
      });

      let requestProblem = '';
      try {
        await sendWithRetries();
      } catch (error) {
        const fromElasticsearch =
          error instanceof LogsStillWaiting || error instanceof errors.ElasticsearchClientError;
        if (!fromElasticsearch) {
          throw error;
        }
        requestProblem = error.message;
      }

      return {
        savedCount,
        unsavedLogs: waiting.map(({ id }) => ({
          id,
          reason: reasons.get(id) ?? requestProblem,
        })),
      };
    },
    close: () => client.close(),
  };
}
