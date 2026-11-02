// Starts the worker on your machine: `pnpm worker`. Kafka and Elasticsearch must be running (see
// README.md).
import { readFileSync } from 'node:fs';
import { readWorkerSettings } from './settings.ts';
import { createBatchSaver } from './save-batch.ts';
import { connectToElasticsearch } from '../elasticsearch/save-logs.ts';
import { readLogsFromKafka } from '../kafka/read-logs.ts';
import { connectToFailedTopic } from '../kafka/send-failed-logs.ts';

const settings = readWorkerSettings(
  readFileSync(new URL('../config/local.yml', import.meta.url), 'utf8'),
);
const elasticsearch = await connectToElasticsearch(
  settings.elasticsearchUrl,
  settings.index,
  settings.replicas,
  settings.requestTimeoutMs,
  settings.retries,
);
const failedTopic = await connectToFailedTopic(settings.kafkaBrokers, settings.failedTopic);
const batchSaver = createBatchSaver({
  saveLogs: elasticsearch.saveLogs,
  sendFailedLogs: failedTopic.sendFailedLogs,
  bulkSize: settings.bulkSize,
  moveStuckLogAfterRounds: settings.moveStuckLogAfterRounds,
  moveToFailedAfterMs: settings.moveToFailedAfterMs,
  alertAfterMs: settings.alertAfterMs,
});
const kafka = await readLogsFromKafka({
  brokers: settings.kafkaBrokers,
  topic: settings.topic,
  group: settings.group,
  pauseMs: settings.pauseMs,
  handleBatch: batchSaver.handleBatch,
  onPartitionsAssigned: batchSaver.keepOnlyPartitions,
  onGiveUp: (error) => {
    console.error('worker: stopped reading from Kafka:', error);
    process.exit(1);
  },
});

console.log(
  `worker: reading "${settings.topic}" from Kafka, saving to the "${settings.index}" index`,
);

// On Ctrl+C or a plain `kill`, finish the current bulk request and its retries, tell Kafka how far we
// got, and close. Logs not reached yet are read again next time.
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => {
    kafka
      .close()
      .then(() => failedTopic.close())
      .then(() => elasticsearch.close())
      .catch((error: unknown) => console.error('worker: could not close cleanly:', error));
  });
}
