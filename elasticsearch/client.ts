// The base layer for Elasticsearch, and the only file that uses the Elasticsearch library. It knows
// nothing about logs, and each call makes one attempt and reports what happened. Decisions like what
// an index should look like, or how long to keep trying, belong to the files that build on this one.
import { Client, errors } from '@elastic/elasticsearch';
import type { Service } from '../services/service.ts';

// How Elasticsearch should store each field of a document.
export type FieldTypes = Readonly<Record<string, { readonly type: 'keyword' | 'text' }>>;

// How an existing index stores its fields. A field that holds an object has no type of its own.
export type ExistingFields = Record<string, { type?: string }>;

// `id` becomes the document's id. Sending the same id again overwrites the document.
export type DocumentToSave = { id: string; document: object };

export type DocumentAnswer =
  { id: string; saved: true } | { id: string; saved: false; reason: string };

// Says whether an error from a call here can pass with time: too busy (429), a gateway or server
// that's briefly unavailable (502, 503, 504), and not getting an answer at all. Anything else, like a
// 401, needs a person.
export function canPassWithTime(error: unknown): boolean {
  if (error instanceof errors.ResponseError) {
    return [429, 502, 503, 504].includes(error.statusCode ?? 0);
  }
  return (
    error instanceof errors.ConnectionError ||
    error instanceof errors.TimeoutError ||
    error instanceof errors.NoLivingConnectionsError
  );
}

// Says whether an error came from a call here, as opposed to some other part of the program.
export function isElasticsearchError(error: unknown): error is Error {
  return error instanceof errors.ElasticsearchClientError;
}

// Prints that a call is about to be tried again, for use with withRetries.
export function warnAboutRetry(error: unknown, retry: number, waitMs: number): void {
  const why = error instanceof Error ? error.message : String(error);
  console.warn(`elasticsearch: ${why}, retry ${retry} in ${waitMs} ms`);
}

const closedMessage = 'ElasticsearchClient is closed: get a new one with getInstance()';

export class ElasticsearchClient implements Service {
  private static instance: ElasticsearchClient | undefined;

  private client: Client | undefined;
  private closing: Promise<void> | undefined;
  private closed = false;
  private readonly url: string;
  private readonly requestTimeoutMs: number;

  private constructor(url: string, requestTimeoutMs: number) {
    this.url = url;
    this.requestTimeoutMs = requestTimeoutMs;
  }

  // Returns the one ElasticsearchClient. `requestTimeoutMs` limits how long any one call may take.
  // Throws if it's asked for with different settings, because those would otherwise be silently ignored.
  static getInstance(url: string, requestTimeoutMs: number): ElasticsearchClient {
    const existing = ElasticsearchClient.instance;
    if (existing === undefined) {
      ElasticsearchClient.instance = new ElasticsearchClient(url, requestTimeoutMs);
      return ElasticsearchClient.instance;
    }
    if (existing.url !== url || existing.requestTimeoutMs !== requestTimeoutMs) {
      throw new Error('ElasticsearchClient is already running with different settings');
    }
    return existing;
  }

  // Sets up the connection. Calling it again keeps the same connection. Nothing is sent to Elasticsearch
  // until the first call, so this can't fail because Elasticsearch is down.
  async init(): Promise<void> {
    if (this.closed) {
      throw new Error(closedMessage);
    }
    // maxRetries: 0 turns off the client's own instant retries, which would multiply with any retries
    // made above this layer and hit a struggling Elasticsearch with no wait at all.
    this.client ??= new Client({
      node: this.url,
      maxRetries: 0,
      requestTimeout: this.requestTimeoutMs,
    });
  }

  // Closes the connection and lets go of the one instance. Calling it again gives the same answer, when
  // the first call finishes.
  close(): Promise<void> {
    this.closing ??= this.closeConnection();
    return this.closing;
  }

  private async closeConnection(): Promise<void> {
    this.closed = true;
    if (ElasticsearchClient.instance === this) {
      ElasticsearchClient.instance = undefined;
    }
    const client = this.client;
    this.client = undefined;
    await client?.close();
  }

  // Throws for any problem other than the index already existing.
  async createIndex(
    index: string,
    fields: FieldTypes,
    replicas: number,
  ): Promise<'created' | 'already exists'> {
    try {
      await this.started().indices.create(
        {
          index,
          settings: { number_of_replicas: replicas },
          mappings: { properties: fields },
        },
        this.withTimeLimit(),
      );
      return 'created';
    } catch (error) {
      const alreadyExists =
        error instanceof errors.ResponseError &&
        error.body?.error?.type === 'resource_already_exists_exception';
      if (alreadyExists) {
        return 'already exists';
      }
      throw error;
    }
  }

  // Answers with one list of fields for each real index behind the name.
  async readIndexFields(index: string): Promise<ExistingFields[]> {
    const answer = await this.started().indices.getMapping({ index }, this.withTimeLimit());
    return Object.values(answer).map(
      ({ mappings }) => (mappings.properties ?? {}) as ExistingFields,
    );
  }

  // Sends one bulk request and answers once per document, in the order they were given. Throws if the
  // request as a whole failed. A just-saved document shows up in searches within about a second.
  async saveDocuments(index: string, documents: DocumentToSave[]): Promise<DocumentAnswer[]> {
    const operations = documents.flatMap(({ id, document }) => [
      { index: { _index: index, _id: id } },
      document,
    ]);
    const { items } = await this.started().bulk({ operations }, this.withTimeLimit());

    return documents.map(({ id }, position) => {
      const answer = items[position]?.index;
      const status = answer?.status ?? 0;
      if (status >= 200 && status < 300) {
        return { id, saved: true };
      }
      const reason = answer?.error?.reason ?? answer?.error?.type ?? `status ${status}`;
      return { id, saved: false, reason: `${status}: ${reason}` };
    });
  }

  private started(): Client {
    if (this.closed) {
      throw new Error(closedMessage);
    }
    if (this.client === undefined) {
      throw new Error('ElasticsearchClient is not running: call init() first');
    }
    return this.client;
  }

  // The signal limits the whole request. The client's requestTimeout only limits each wait for part of
  // an answer, so a slow request could outlast it.
  private withTimeLimit() {
    return { signal: AbortSignal.timeout(this.requestTimeoutMs) };
  }
}

// The calls the files above this one use, so their tests can stand in for Elasticsearch.
export type ElasticsearchCalls = Pick<
  ElasticsearchClient,
  'createIndex' | 'readIndexFields' | 'saveDocuments'
>;
