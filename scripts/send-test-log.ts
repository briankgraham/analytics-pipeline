// Sends one test log to the running API: `pnpm send-test-log`.
import { readFileSync } from 'node:fs';
import { readApiSettings } from '../api/settings.ts';

const { port } = readApiSettings(
  readFileSync(new URL('../config/local.yml', import.meta.url), 'utf8'),
);

const response = await fetch(`http://127.0.0.1:${port}/v1/message`, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ service: 'checkout', level: 'error', message: 'card declined' }),
});

console.log(`the API answered ${response.status}: ${await response.text()}`);
if (response.ok) {
  console.log(
    `with the worker running, search for it: curl 'http://127.0.0.1:${port}/v1/search?q=card'`,
  );
} else {
  process.exitCode = 1;
}
