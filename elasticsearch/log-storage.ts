// Where the worker keeps logs in Elasticsearch: the service that puts logs-index.ts and save-logs.ts
// together on top of the base layer in client.ts.
import type { RetrySettings } from '../retries/with-retries.ts';
import type { Service } from '../services/service.ts';
import type { ElasticsearchCalls } from './client.ts';
import { setUpLogsIndex } from './logs-index.ts';
import { createLogSaver, type SaveLogs } from './save-logs.ts';

const closedMessage = 'LogStorage is closed: get a new one with getInstance()';

export class LogStorage implements Service {
  private static instance: LogStorage | undefined;

  private readonly client: ElasticsearchCalls;
  private readonly index: string;
  private readonly replicas: number;
  private readonly retries: RetrySettings;
  private settingUp: Promise<void> | undefined;
  private ready = false;
  private closed = false;

  // Saves logs into the given index. Throws if init hasn't finished, or after close, because the index
  // may not be there or may be wrong.
  readonly saveLogs: SaveLogs;

  private constructor(
    client: ElasticsearchCalls,
    index: string,
    replicas: number,
    retries: RetrySettings,
  ) {
    this.client = client;
    this.index = index;
    this.replicas = replicas;
    this.retries = retries;
    const saveToIndex = createLogSaver(client, index, retries);
    this.saveLogs = async (logs, keepAlive) => {
      if (this.closed) {
        throw new Error(closedMessage);
      }
      if (!this.ready) {
        throw new Error('LogStorage is not running: call init() first');
      }
      return saveToIndex(logs, keepAlive);
    };
  }

  // Returns the one LogStorage. Throws if it's asked for with different settings, because those would
  // otherwise be silently ignored.
  static getInstance(
    client: ElasticsearchCalls,
    index: string,
    replicas: number,
    retries: RetrySettings,
  ): LogStorage {
    const existing = LogStorage.instance;
    if (existing === undefined) {
      LogStorage.instance = new LogStorage(client, index, replicas, retries);
      return LogStorage.instance;
    }
    const sameSettings =
      existing.client === client &&
      existing.index === index &&
      existing.replicas === replicas &&
      existing.retries.maxRetries === retries.maxRetries &&
      existing.retries.delayMs === retries.delayMs &&
      existing.retries.exponential === retries.exponential &&
      existing.retries.jitter === retries.jitter;
    if (!sameSettings) {
      throw new Error(
        'LogStorage is already running with different settings or a different client',
      );
    }
    return existing;
  }

  // Creates the logs index or checks the one that's there. Throws if it can't, or if it's wrong. Calling
  // it again does nothing new, even while the first call is still working. After a failure, the next
  // call tries again.
  init(): Promise<void> {
    if (this.closed) {
      return Promise.reject(new Error(closedMessage));
    }
    this.settingUp ??= setUpLogsIndex(this.client, this.index, this.replicas, this.retries).then(
      () => {
        this.ready = !this.closed;
      },
      (error: unknown) => {
        this.settingUp = undefined;
        throw error;
      },
    );
    return this.settingUp;
  }

  // Waits for any init still working, then lets go of the one instance. Close this before the client,
  // because it has nothing else of its own to close: main.ts closes the client it was given.
  async close(): Promise<void> {
    this.closed = true;
    this.ready = false;
    await this.settingUp?.catch(() => {});
    if (LogStorage.instance === this) {
      LogStorage.instance = undefined;
    }
  }
}
