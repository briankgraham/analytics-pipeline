// The only file that reads from Kafka. The worker gets each message as plain text, so the part that
// handles a message can be tested without a running Kafka.
import { Kafka } from 'kafkajs';

// Handles one message. `text` is the message's content, or null if it has none. `where` says where
// the message sits in Kafka (topic, partition, offset), for warnings. If it throws, kafkajs tries the
// same message again, and keeps trying, so it must throw only for problems that can pass (like
// Elasticsearch being down), never for a bad message.
export type HandleMessage = (text: string | null, where: string) => Promise<void>;

export type KafkaReader = {
  close: () => Promise<void>;
};

type ReadOptions = {
  brokers: string[];
  topic: string;
  group: string;
  handleMessage: HandleMessage;
  // Called if kafkajs stops reading for good (it restarts by itself after most errors).
  onGiveUp: (error: Error) => void;
};

// Starts reading the topic, one message at a time, and returns once reading has begun. Kafka
// remembers, per group, which messages are done. A message counts as done only after handleMessage
// finishes without throwing, so a worker that crashes mid-message reads that message again on
// restart. A group reading for the first time starts at the oldest message still in the topic.
export async function readLogsFromKafka(options: ReadOptions): Promise<KafkaReader> {
  const { brokers, topic, group, handleMessage, onGiveUp } = options;
  const consumer = new Kafka({ clientId: 'worker', brokers }).consumer({ groupId: group });

  consumer.on(consumer.events.CRASH, ({ payload }) => {
    if (!payload.restart) {
      onGiveUp(payload.error);
    }
  });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const text = message.value === null ? null : message.value.toString('utf8');
      await handleMessage(text, `${topic} partition ${partition} offset ${message.offset}`);
    },
  });

  return { close: () => consumer.disconnect() };
}
