export const levels = ['debug', 'info', 'warn', 'error'] as const;

export type Log = {
  service: string;
  level: (typeof levels)[number];
  message: string;
};

// Kafka refuses messages over 1 MB by default, so a log's text has to stay well under that.
const longestService = 100;
const longestMessage = 10_000;

export type ReadLogResult = { ok: true; log: Log } | { ok: false; problem: string };

// Checks something that arrived from outside (for example a parsed request body) and returns it as
// a Log, or a plain sentence saying what is wrong. Fields that aren't part of a log are dropped, so
// they never reach Kafka.
export function readLog(input: unknown): ReadLogResult {
  if (typeof input !== 'object' || input === null || Array.isArray(input)) {
    return { ok: false, problem: 'a log must be a JSON object' };
  }
  const { service, level, message } = input as Record<string, unknown>;

  if (typeof service !== 'string' || service.trim() === '') {
    return { ok: false, problem: 'service must be text saying which app sent the log' };
  }
  if (service.length > longestService) {
    return { ok: false, problem: `service must be at most ${longestService} characters` };
  }
  if (!levels.some((known) => known === level)) {
    return { ok: false, problem: `level must be one of: ${levels.join(', ')}` };
  }
  if (typeof message !== 'string' || message.trim() === '') {
    return { ok: false, problem: 'message must be text' };
  }
  if (message.length > longestMessage) {
    return { ok: false, problem: `message must be at most ${longestMessage} characters` };
  }

  return { ok: true, log: { service, level: level as Log['level'], message } };
}
