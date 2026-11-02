// Starts the API on your machine: `pnpm api`. Kafka must be running (see README.md).
import { readFileSync } from 'node:fs';
import { ApiServer } from './server.ts';
import { readApiSettings } from './settings.ts';
import { runProgram } from '../services/run-program.ts';

const settings = readApiSettings(
  readFileSync(new URL('../config/local.yml', import.meta.url), 'utf8'),
);

await runProgram('api', () => ApiServer.getInstance(settings));
