// Starts the worker on your machine: `pnpm worker`. Kafka and Elasticsearch must be running (see
// README.md).
import { readFileSync } from 'node:fs';
import { readWorkerSettings } from './settings.ts';
import { saveMessage } from './save-message.ts';
import { connectToElasticsearch } from '../elasticsearch/save-log.ts';
import { readLogsFromKafka } from '../kafka/read-logs.ts';

const settings = readWorkerSettings(
  readFileSync(new URL('../config/local.yml', import.meta.url), 'utf8'),
);
const elasticsearch = await connectToElasticsearch(
  settings.elasticsearchUrl,
  settings.index,
  settings.replicas,
);
const kafka = await readLogsFromKafka({
  brokers: settings.kafkaBrokers,
  topic: settings.topic,
  group: settings.group,
  handleMessage: (text, where) => saveMessage(text, where, elasticsearch.saveLog),
  onGiveUp: (error) => {
    console.error('worker: stopped reading from Kafka:', error);
    process.exit(1);
  },
});

console.log(
  `worker: reading "${settings.topic}" from Kafka, saving to the "${settings.index}" index`,
);

// On Ctrl+C or a plain `kill`, finish the current message, tell Kafka how far we got, and close.
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => {
    kafka
      .close()
      .then(() => elasticsearch.close())
      .catch((error: unknown) => console.error('worker: could not close cleanly:', error));
  });
}
