// The worker. It owns everything it needs and starts it in a fixed order: the clients first, then the
// index and the failed topic it depends on, and reading last, so no log arrives before everything is
// ready. Closing goes the other way.
import { isDeepStrictEqual } from 'node:util';
import { ElasticsearchClient } from '../elasticsearch/client.ts';
import { setUpLogsIndex } from '../elasticsearch/logs-index.ts';
import { createLogSaver } from '../elasticsearch/save-logs.ts';
import { KafkaClient } from '../kafka/client.ts';
import { createFailedLogSender } from '../kafka/send-failed-logs.ts';
import type { StopProgram } from '../services/run-program.ts';
import type { Service } from '../services/service.ts';
import { createBatchSaver } from './save-batch.ts';
import type { WorkerSettings } from './settings.ts';

const closedMessage = 'Worker is closed: get a new one with getInstance()';

export class Worker implements Service {
  private static instance: Worker | undefined;

  private readonly settings: WorkerSettings;
  private readonly stopProgram: StopProgram;
  // The services this worker has started, oldest first, so close knows what to close.
  private readonly startedServices: Service[] = [];
  private starting: Promise<void> | undefined;
  private closing: Promise<void> | undefined;
  private closed = false;

  private constructor(settings: WorkerSettings, stopProgram: StopProgram) {
    this.settings = settings;
    this.stopProgram = stopProgram;
  }

  // Returns the one Worker. `stopProgram` is called if reading from Kafka stops for good. Throws if it's
  // asked for with anything different, because that would otherwise be silently ignored.
  static getInstance(settings: WorkerSettings, stopProgram: StopProgram): Worker {
    const existing = Worker.instance;
    if (existing === undefined) {
      Worker.instance = new Worker(settings, stopProgram);
      return Worker.instance;
    }
    if (!isDeepStrictEqual(existing.settings, settings) || existing.stopProgram !== stopProgram) {
      throw new Error('Worker is already running with different settings');
    }
    return existing;
  }

  // Starts everything. Calling it again does nothing new, even while the first call is still working.
  // After a failure, whatever did start stays open until close, and the next call tries again.
  init(): Promise<void> {
    if (this.closed) {
      return Promise.reject(new Error(closedMessage));
    }
    this.starting ??= this.start().catch((error: unknown) => {
      this.starting = undefined;
      throw error;
    });
    return this.starting;
  }

  private async start(): Promise<void> {
    const { settings } = this;

    const elasticsearch = ElasticsearchClient.getInstance(
      settings.elasticsearchUrl,
      settings.requestTimeoutMs,
    );
    await this.startService(elasticsearch);
    await setUpLogsIndex(elasticsearch, settings.index, settings.replicas, settings.retries);
    this.throwIfClosed();
    const kafka = KafkaClient.getInstance(settings.kafkaBrokers, 'worker');
    await this.startService(kafka);
    const sendFailedLogs = await createFailedLogSender(kafka, settings.failedTopic);
    this.throwIfClosed();

    const batchSaver = createBatchSaver({
      saveLogs: createLogSaver(elasticsearch, settings.index, settings.retries),
      sendFailedLogs,
      bulkSize: settings.bulkSize,
      moveStuckLogAfterRounds: settings.moveStuckLogAfterRounds,
      moveToFailedAfterMs: settings.moveToFailedAfterMs,
      alertAfterMs: settings.alertAfterMs,
    });
    await kafka.startReading({
      topic: settings.topic,
      group: settings.group,
      pauseMs: settings.pauseMs,
      handleBatch: batchSaver.handleBatch,
      onPartitionsAssigned: batchSaver.keepOnlyPartitions,
      onGiveUp: this.stopProgram,
    });
    console.log(
      `worker: reading "${settings.topic}" from Kafka, saving to the "${settings.index}" index`,
    );
  }

  // Waits for any start-up still working, then finishes the current bulk request and its retries, tells
  // Kafka how far it got, and closes the clients. Logs not reached yet are read again next time. Calling
  // it again gives the same answer, when the first call finishes.
  close(): Promise<void> {
    this.closing ??= this.closeEverything();
    return this.closing;
  }

  private async closeEverything(): Promise<void> {
    this.closed = true;
    if (Worker.instance === this) {
      Worker.instance = undefined;
    }
    await this.starting?.catch(() => {});

    // Newest first, so Kafka stops handing over logs before Elasticsearch closes. One that fails to close
    // is no reason to leave the others open.
    let everythingClosed = true;
    for (const service of this.startedServices.toReversed()) {
      try {
        await service.close();
      } catch (error) {
        console.error('worker: could not close a service:', error);
        everythingClosed = false;
      }
    }
    if (!everythingClosed) {
      throw new Error('not everything closed cleanly');
    }
  }

  // Starts a service, and remembers it first, so one that only partly started still gets closed.
  private async startService(service: Service): Promise<void> {
    if (!this.startedServices.includes(service)) {
      this.startedServices.push(service);
    }
    await service.init();
  }

  private throwIfClosed(): void {
    if (this.closed) {
      throw new Error(closedMessage);
    }
  }
}
