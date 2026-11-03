import type { SendLog } from './services/message-service.ts';
import type { SearchLogs } from './services/search-service.ts';

// Everything the API's routes need from outside. server.ts builds it once, with the real versions;
// tests build it with fakes. A new feature adds a field here. Each route takes only what it uses.
export type Dependencies = {
  sendLog: SendLog;
  searchLogs: SearchLogs;
};
