// Writes logs the worker can't save to the failed topic, where they're kept safe for longer than in
// the logs topic. Nothing reads that topic yet.
import { Kafka, Partitioners } from 'kafkajs';
import type { Log } from '../logs/log.ts';

export type FailedLog = {
  // The log's position in the logs topic, like logs-2-4711.
  id: string;
  log: Log;
  reason: string;
};

// Finishes only after Kafka has stored every log, and throws if it couldn't. After a throw, some may
// be stored already, so a log can end up in the failed topic twice, but never not at all.
export type SendFailedLogs = (logs: FailedLog[]) => Promise<void>;

export type FailedLogsConnection = {
  sendFailedLogs: SendFailedLogs;
  close: () => Promise<void>;
};

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

export async function connectToFailedTopic(
  brokers: string[],
  topic: string,
): Promise<FailedLogsConnection> {
  const kafka = new Kafka({ clientId: 'worker', brokers });

  // Sending only fails once a log needs moving, which could be days from now and is the worst moment
  // to find out the topic is missing. So check now, and stop the worker if it isn't there.
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.fetchTopicMetadata({ topics: [topic] });
  } catch (error) {
    if ((error as { type?: unknown }).type === 'UNKNOWN_TOPIC_OR_PARTITION') {
      throw new Error(`the ${topic} topic doesn't exist in Kafka. Have you run \`pnpm topics\`?`, {
        cause: error,
      });
    }
    throw error;
  } finally {
    await admin.disconnect();
  }

  const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();

  return {
    sendFailedLogs: async (logs) => {
      const messages = logs.map((failed) => JSON.stringify(failed));
      for (const piece of splitBySize(messages, mostBytesInOneSend)) {
        await producer.send({ topic, messages: piece.map((value) => ({ value })) });
      }
    },
    close: () => producer.disconnect(),
  };
}
