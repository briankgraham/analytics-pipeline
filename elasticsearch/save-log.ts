// The only file that talks to Elasticsearch. The worker gets a plain `saveLog` function, so it can be
// tested without a running Elasticsearch.
import { Client, errors } from '@elastic/elasticsearch';
import type { Log } from '../logs/log.ts';

export type SaveLog = (log: Log) => Promise<void>;

export type ElasticsearchConnection = {
  saveLog: SaveLog;
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

// Connects to Elasticsearch, creates the index if it's missing, and returns a function that saves
// one log into it. Throws if Elasticsearch can't be reached (the client tries for several seconds
// first). An index that already exists is left as it is.
export async function connectToElasticsearch(
  url: string,
  index: string,
  replicas: number,
): Promise<ElasticsearchConnection> {
  const client = new Client({ node: url });

  try {
    await client.indices.create({
      index,
      settings: { number_of_replicas: replicas },
      mappings: { properties: logFields },
    });
  } catch (error) {
    const alreadyExists =
      error instanceof errors.ResponseError &&
      error.body?.error?.type === 'resource_already_exists_exception';
    if (!alreadyExists) {
      await client.close();
      throw error;
    }
  }

  return {
    // Finishes once Elasticsearch has stored the log. Throws if it couldn't, for example when
    // Elasticsearch is down. A just-saved log shows up in searches within about a second.
    saveLog: async (log) => {
      await client.index({ index, document: log });
    },
    close: () => client.close(),
  };
}
