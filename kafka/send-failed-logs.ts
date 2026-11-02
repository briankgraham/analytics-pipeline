// Writes logs the worker can't save to the failed topic, where they're kept safe for longer than in
// the logs topic. Built on the base layer in client.ts. Nothing reads that topic yet.
import type { Log } from '../logs/log.ts';
import type { KafkaCalls } from './client.ts';

export type FailedLog = {
  // The log's position in the logs topic, like logs-2-4711.
  id: string;
  log: Log;
  reason: string;
};

// Finishes only after Kafka has stored every log, and throws if it couldn't. After a throw, some may
// be stored already, so a log can end up in the failed topic twice, but never not at all.
export type SendFailedLogs = (logs: FailedLog[]) => Promise<void>;

// Kafka refuses a set of messages for one partition over 1 MB by default. Keeping every send well under
// that means a big group of logs can always be moved.
const mostBytesInOneSend = 500_000;

// Splits messages into lists of at most mostBytes each. A single message bigger than that gets a list
// of its own.
export function splitBySize(messages: string[], mostBytes: number): string[][] {
  const pieces: string[][] = [];
  let piece: string[] = [];
  let pieceBytes = 0;
  for (const message of messages) {
    const bytes = Buffer.byteLength(message);
    if (piece.length > 0 && pieceBytes + bytes > mostBytes) {
      pieces.push(piece);
      piece = [];
      pieceBytes = 0;
    }
    piece.push(message);
    pieceBytes += bytes;
  }
  if (piece.length > 0) {
    pieces.push(piece);
  }
  return pieces;
}

// Returns the function that sends logs to the given topic. Sending only fails once a log needs moving,
// which could be days from now and is the worst moment to find out the topic is missing. So this
// checks now, and throws if it isn't there.
export async function createFailedLogSender(
  client: KafkaCalls,
  topic: string,
): Promise<SendFailedLogs> {
  if (!(await client.topicExists(topic))) {
    throw new Error(`the ${topic} topic doesn't exist in Kafka. Have you run \`pnpm topics\`?`);
  }

  return async (logs) => {
    const messages = logs.map((failed) => JSON.stringify(failed));
    for (const piece of splitBySize(messages, mostBytesInOneSend)) {
      await client.sendMessages(topic, piece);
    }
  };
}
