// What our logs index looks like, built on the base layer in client.ts.
import { withRetries, type RetrySettings } from '../retries/with-retries.ts';
import {
  canPassWithTime,
  warnAboutRetry,
  type ElasticsearchCalls,
  type ExistingFields,
  type FieldTypes,
} from './client.ts';

// How Elasticsearch should store each field of a log. `keyword` keeps the whole value as one exact
// piece, good for filters like "only errors". `text` splits it into words, so a search for "card"
// finds "card declined".
const logFields = {
  service: { type: 'keyword' },
  level: { type: 'keyword' },
  message: { type: 'text' },
} as const satisfies FieldTypes;

// Finds which of our fields an existing index stores differently from logFields, in words. Other
// fields don't matter, because we never send them.
export function describeWrongFields(fields: ExistingFields): string[] {
  return Object.entries(logFields).flatMap(([name, { type }]) => {
    const actual = fields[name]?.type;
    if (actual === type) {
      return [];
    }
    const found =
      actual === undefined ? 'has no type (it is missing, or holds an object)' : `is ${actual}`;
    return [`${name} ${found}, should be ${type}`];
  });
}

// Creates the logs index if it's missing. Throws if Elasticsearch still can't be reached after the
// retries. An index that already exists is left as it is, but it must store our fields the way we
// expect. If it doesn't, good logs would be refused, so this throws and the worker doesn't start.
export async function setUpLogsIndex(
  client: ElasticsearchCalls,
  index: string,
  replicas: number,
  retries: RetrySettings,
): Promise<void> {
  const retryOptions = { ...retries, shouldRetry: canPassWithTime, onRetry: warnAboutRetry };
  const createIndex = withRetries(
    () => client.createIndex(index, logFields, replicas),
    retryOptions,
  );
  const readIndexFields = withRetries(() => client.readIndexFields(index), retryOptions);

  if ((await createIndex()) === 'created') {
    return;
  }

  const wrongFields = (await readIndexFields()).flatMap(describeWrongFields);
  if (wrongFields.length > 0) {
    throw new Error(
      `the "${index}" index stores fields the wrong way (${wrongFields.join('; ')}). ` +
        "Good logs would be refused, and a field can't be changed in place. Deleting the index also " +
        'deletes the logs already saved in it; see README.md for how to save them again from Kafka.',
    );
  }
}
