// Starts the worker on your machine: `pnpm worker`. Kafka and Elasticsearch must be running (see
// README.md).
import { readFileSync } from 'node:fs';
import { readWorkerSettings } from './settings.ts';
import { Worker } from './worker.ts';
import { runProgram } from '../services/run-program.ts';

const settings = readWorkerSettings(
  readFileSync(new URL('../config/local.yml', import.meta.url), 'utf8'),
);

await runProgram('worker', (stop) => Worker.getInstance(settings, stop));
