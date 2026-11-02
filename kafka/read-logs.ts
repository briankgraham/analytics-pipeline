// The only file that reads from Kafka. The worker gets batches of plain messages, so the part that
// handles them can be tested without a running Kafka.
import { Kafka } from 'kafkajs';

export type MessageFromKafka = {
  // Where the message sits in Kafka, like logs-2-4711 (topic, partition, offset). A message keeps it
  // when it's read again. No two messages share one, unless the topic is deleted and created again:
  // its offsets then restart from 0.
  id: string;
  offset: string;
  // The message's content, or null if it has none.
  text: string | null;
  // When the message was put into Kafka.
  timestampMs: number;
};

export type MessageBatch = {
  partition: number;
  messages: MessageFromKafka[];
  // Tells Kafka the worker is still working. Throws when the worker must stop and rejoin its group;
  // let that error through, because kafkajs uses it to rejoin.
  keepAlive: () => Promise<void>;
  // Records that this message, and all before it in the partition, won't need reading again. Kafka
  // is told when the handler returns, so after a crash mid-batch these are read again.
  markDone: (offset: string) => void;
  // True when handling should stop early: the worker is closing, or the partition's position changed.
  shouldStop: () => boolean;
};

// 'finished' moves on to the next batch. 'pause' stops reading this partition for a while, then
// carries on from the first message that isn't marked done. Throw only for unexpected problems.
export type HandleBatch = (batch: MessageBatch) => Promise<'finished' | 'pause'>;

export type KafkaReader = {
  close: () => Promise<void>;
};

type ReadOptions = {
  brokers: string[];
  topic: string;
  group: string;
  // How long a pause lasts. The worker stays in its group during a pause.
  pauseMs: number;
  handleBatch: HandleBatch;
  // Called each time the worker joins its group, with the partitions it now reads.
  onPartitionsAssigned: (partitions: number[]) => void;
  // Called if kafkajs stops reading for good (it restarts by itself after most errors).
  onGiveUp: (error: Error) => void;
};

// Starts reading the topic and returns once reading has begun. Kafka remembers, per group, which
// messages are done, so a worker that crashes reads the rest again on restart. A group reading for
// the first time starts at the oldest message still in the topic. A batch holds whatever is available,
// so nothing waits for a batch to fill up.
export async function readLogsFromKafka(options: ReadOptions): Promise<KafkaReader> {
  const { brokers, topic, group, pauseMs, handleBatch, onPartitionsAssigned, onGiveUp } = options;
  const consumer = new Kafka({ clientId: 'worker', brokers }).consumer({ groupId: group });
  const resumeTimers = new Set<NodeJS.Timeout>();

  consumer.on(consumer.events.CRASH, ({ payload }) => {
    if (!payload.restart) {
      onGiveUp(payload.error);
    }
  });

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    onPartitionsAssigned(payload.memberAssignment[topic] ?? []);
  });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    // Without this, kafkajs marks the whole batch done when the handler returns, even after a pause.
    eachBatchAutoResolve: false,
    eachBatch: async (kafkaBatch) => {
      const { batch, resolveOffset, heartbeat, pause, isRunning, isStale } = kafkaBatch;

      const answer = await handleBatch({
        partition: batch.partition,
        messages: batch.messages.map((message) => ({
          id: `${topic}-${batch.partition}-${message.offset}`,
          offset: message.offset,
          text: message.value === null ? null : message.value.toString('utf8'),
          timestampMs: Number(message.timestamp),
        })),
        keepAlive: heartbeat,
        markDone: resolveOffset,
        shouldStop: () => !isRunning() || isStale(),
      });

      // A worker that's closing doesn't pause: it's stopping anyway, and a timer started now would keep
      // the program running after everything else has closed.
      if (answer === 'pause' && isRunning()) {
        console.warn(`worker: pausing partition ${batch.partition} for ${pauseMs} ms`);
        const resume = pause();
        const timer = setTimeout(() => {
          resumeTimers.delete(timer);
          resume();
        }, pauseMs);
        resumeTimers.add(timer);
      }
    },
  });

  return {
    close: async () => {
      for (const timer of resumeTimers) {
        clearTimeout(timer);
      }
      await consumer.disconnect();
    },
  };
}
