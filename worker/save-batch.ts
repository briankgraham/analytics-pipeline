import type { LogNotSaved, SaveLogs } from '../elasticsearch/save-logs.ts';
import type { HandleBatch, MessageBatch, MessageFromKafka } from '../kafka/client.ts';
import type { FailedLog, SendFailedLogs } from '../kafka/send-failed-logs.ts';
import { readLog, type Log } from '../logs/log.ts';

type BatchSaverOptions = {
  saveLogs: SaveLogs;
  sendFailedLogs: SendFailedLogs;
  // How many logs go into one bulk request.
  bulkSize: number;
  // How long to wait before trying again when nothing in a bulk request could be saved.
  waitBeforeTryingAgainMs: number;
  // A log that still can't be saved moves to the failed topic once it's this old, before Kafka
  // deletes it from the logs topic.
  moveToFailedAfterMs: number;
  // After this long unable to get through a partition, each new wait prints an alert.
  alertAfterMs: number;
};

export type BatchSaver = {
  handleBatch: HandleBatch;
  // Forgets the trouble of partitions this worker no longer reads, so old news isn't mistaken for new
  // if one comes back later.
  keepOnlyPartitions: (partitions: number[]) => void;
};

// What's known about a partition that's having trouble. It's forgotten when a batch finishes.
type PartitionTrouble = {
  troubleSinceMs: number;
  // Logs already moved to the failed topic but not marked done, because an unsaved log sits before
  // them. They're read again after the wait, and must not be sent or moved again. Each id is kept
  // with its timestamp, so a new message that reuses the id (see kafka/client.ts) isn't skipped.
  moved: Map<string, number>;
};

// A message whose text turned out to be a good log.
type ReadableMessage = MessageFromKafka & { log: Log };

// How much of a bad message to print in a warning.
const longestTextInWarning = 1000;

// A wait is cut into pieces this long, with a word to Kafka after each. Kafka drops a worker that stays
// silent for 30 seconds, and a worker that is shutting down notices within one piece.
const waitPieceMs = 1_000;

function warnAboutBadMessage(id: string, why: string, text: string | null): void {
  const shown = text === null ? '' : `: ${text.slice(0, longestTextInWarning)}`;
  console.warn(`worker: skipped the message ${id}: ${why}${shown}`);
}

// Turns a message's text into a log. Returns null, with a warning, if it isn't a good log, because
// trying again would never work and would hold up every message behind it. This is the only place
// that throws a log away.
function readMessage({ id, text }: MessageFromKafka): Log | null {
  if (text === null) {
    warnAboutBadMessage(id, 'it is empty', text);
    return null;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    warnAboutBadMessage(id, 'it is not JSON', text);
    return null;
  }

  const result = readLog(parsed);
  if (!result.ok) {
    warnAboutBadMessage(id, result.problem, text);
    return null;
  }
  return result.log;
}

// Returns the function that handles each batch from Kafka. A batch is saved in groups of up to
// bulkSize logs, and each log ends up in one of four ways:
//   - bad data (not a good log): a warning is printed and the worker moves on
//   - saved in Elasticsearch
//   - moved to the failed topic, because Elasticsearch refused it with a 400, or it kept being refused
//     (after the retries) while other logs in its request saved, or it's about to be deleted from Kafka
//   - not saved yet: nothing in its request saved, which looks like Elasticsearch being down. Everything
//     before it is marked done, the worker waits, and Kafka hands it over again with any newer logs.
//     The partition is never paused.
export function createBatchSaver(options: BatchSaverOptions): BatchSaver {
  const { saveLogs, sendFailedLogs, bulkSize, waitBeforeTryingAgainMs } = options;
  const { moveToFailedAfterMs, alertAfterMs } = options;
  const troubleByPartition = new Map<number, PartitionTrouble>();

  // Decides which logs that couldn't be saved should move to the failed topic now. If other logs in the
  // same request saved, Elasticsearch is working, so a log it kept refusing after the retries is the
  // problem, and it moves. If nothing saved, Elasticsearch is the likely problem, so the logs wait,
  // unless they are about to be deleted from Kafka.
  function chooseLogsToMove(
    unsavedLogs: LogNotSaved[],
    messagesById: Map<string, ReadableMessage>,
    otherLogsSaved: boolean,
  ): FailedLog[] {
    const logsToMove: FailedLog[] = [];
    for (const { id, reason } of unsavedLogs) {
      const message = messagesById.get(id);
      if (message === undefined) {
        continue;
      }
      if (Date.now() - message.timestampMs >= moveToFailedAfterMs) {
        logsToMove.push({
          id,
          log: message.log,
          reason: `Kafka will delete it soon; last problem: ${reason}`,
        });
      } else if (otherLogsSaved) {
        logsToMove.push({
          id,
          log: message.log,
          reason: `Elasticsearch kept refusing it after the retries, while it saved other logs; last problem: ${reason}`,
        });
      }
    }
    return logsToMove;
  }

  // Saves one group of messages and returns the logs that still aren't dealt with.
  async function saveGroup(
    messages: MessageFromKafka[],
    batch: MessageBatch,
    trouble: PartitionTrouble,
  ): Promise<LogNotSaved[]> {
    const messagesById = new Map<string, ReadableMessage>();
    for (const message of messages) {
      if (trouble.moved.get(message.id) === message.timestampMs) {
        continue;
      }
      const log = readMessage(message);
      if (log !== null) {
        messagesById.set(message.id, { ...message, log });
      }
    }
    if (messagesById.size === 0) {
      return [];
    }

    const logsToSave = [...messagesById.values()].map(({ id, log }) => ({ id, log }));
    const { savedCount, unsavedLogs, badRequestLogs } = await saveLogs(logsToSave, batch.keepAlive);

    // Elasticsearch refused these with a 400 that blames the log, so they go to the failed topic now.
    // Waiting would only mean asking again and getting the same answer.
    const badRequestsToMove: FailedLog[] = [];
    for (const { id, reason } of badRequestLogs) {
      const message = messagesById.get(id);
      if (message !== undefined) {
        badRequestsToMove.push({
          id,
          log: message.log,
          reason: `Elasticsearch refused it: ${reason}`,
        });
      }
    }
    const logsToMove = [
      ...badRequestsToMove,
      ...chooseLogsToMove(unsavedLogs, messagesById, savedCount > 0),
    ];
    if (logsToMove.length === 0) {
      return unsavedLogs;
    }
    // The last try may have taken a while, so speak up before talking to Kafka again.
    await batch.keepAlive();
    try {
      await sendFailedLogs(logsToMove);
    } catch (error) {
      // The logs stay in the logs topic and aren't marked done, so nothing is lost.
      console.error('worker: could not write to the failed topic:', error);
      return [...unsavedLogs, ...badRequestLogs];
    }
    for (const { id, reason } of logsToMove) {
      const timestampMs = messagesById.get(id)?.timestampMs;
      if (timestampMs !== undefined) {
        trouble.moved.set(id, timestampMs);
      }
      console.error(`worker: moved ${id} to the failed topic: ${reason}`);
    }
    return unsavedLogs.filter(({ id }) => !trouble.moved.has(id));
  }

  // Waits, saying a word to Kafka now and then so the worker isn't dropped from its group. Stops early
  // if the worker is shutting down.
  async function waitBeforeTryingAgain(batch: MessageBatch): Promise<void> {
    let leftMs = waitBeforeTryingAgainMs;
    while (leftMs > 0 && !batch.shouldStop()) {
      const pieceMs = Math.min(leftMs, waitPieceMs);
      await new Promise((resolve) => setTimeout(resolve, pieceMs));
      leftMs -= pieceMs;
      await batch.keepAlive();
    }
  }

  const handleBatch: HandleBatch = async (batch) => {
    const trouble = troubleByPartition.get(batch.partition) ?? {
      troubleSinceMs: Date.now(),
      moved: new Map<string, number>(),
    };

    for (let start = 0; start < batch.messages.length; start += bulkSize) {
      // The worker is shutting down, or this partition's position changed. Whatever isn't marked
      // done is handed over again later.
      if (batch.shouldStop()) {
        return;
      }

      const group = batch.messages.slice(start, start + bulkSize);
      const stillUnsaved = await saveGroup(group, batch, trouble);
      const stillUnsavedIds = new Set(stillUnsaved.map(({ id }) => id));

      // Kafka only remembers one position per partition, so "done" can't go past the first log that
      // isn't saved. Logs after it that did save are sent again later, which overwrites them. Logs after
      // it that moved to the failed topic are remembered in `moved` and passed over next time.
      const firstUnsaved = group.findIndex(({ id }) => stillUnsavedIds.has(id));
      const lastDone = firstUnsaved === -1 ? group.at(-1) : group[firstUnsaved - 1];
      if (lastDone !== undefined) {
        batch.markDone(lastDone.offset);
      }

      if (firstUnsaved !== -1) {
        troubleByPartition.set(batch.partition, trouble);
        console.error(
          `worker: ${stillUnsaved.length} logs in partition ${batch.partition} are not saved yet: ${stillUnsaved[0]?.reason}`,
        );
        const troubleMs = Date.now() - trouble.troubleSinceMs;
        if (troubleMs >= alertAfterMs) {
          console.error(
            `worker: ALERT: partition ${batch.partition} has not got through a batch in ${Math.round(troubleMs / 60_000)} minutes`,
          );
        }
        // Returning without marking these logs done makes Kafka hand them over again straight away,
        // so the wait has to happen here.
        await waitBeforeTryingAgain(batch);
        return;
      }
      await batch.keepAlive();
    }

    troubleByPartition.delete(batch.partition);
  };

  return {
    handleBatch,
    keepOnlyPartitions: (partitions) => {
      for (const partition of troubleByPartition.keys()) {
        if (!partitions.includes(partition)) {
          troubleByPartition.delete(partition);
        }
      }
    },
  };
}
