// Starts the API on your machine: `pnpm api`. Kafka must be running (see README.md).
import { readFileSync } from 'node:fs';
import { createServer } from './server.ts';
import { readApiSettings } from './settings.ts';
import { connectToKafka } from '../kafka/send-log.ts';

const settings = readApiSettings(
  readFileSync(new URL('../config/local.yml', import.meta.url), 'utf8'),
);
const kafka = await connectToKafka(settings.kafkaBrokers, settings.topic);

// Listening on 127.0.0.1 means only this machine can reach the API.
const server = createServer({ sendLog: kafka.sendLog }).listen(settings.port, '127.0.0.1', () => {
  console.log(
    `api: listening on http://127.0.0.1:${settings.port}, sending logs to "${settings.topic}"`,
  );
});

// On Ctrl+C or a plain `kill`, stop taking requests and let Kafka know we're leaving.
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => {
    server.close(() => {
      kafka.close().catch((error: unknown) => console.error('could not close Kafka:', error));
    });
  });
}
