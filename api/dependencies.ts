import type { SendLog } from '../kafka/send-log.ts';

// Everything the API's routes need from outside. main.ts builds it once, with the real versions;
// tests build it with fakes. A new feature adds a field here. Each route takes only what it uses.
export type Dependencies = {
  sendLog: SendLog;
};
