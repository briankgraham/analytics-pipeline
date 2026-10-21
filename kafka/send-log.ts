// The only file that talks to Kafka. The rest of the API gets a plain `sendLog` function, so it
// can be tested without a running Kafka.
import { Kafka, Partitioners } from 'kafkajs';
import type { Log } from '../logs/log.ts';

export type SendLog = (log: Log) => Promise<void>;

export type KafkaConnection = {
  sendLog: SendLog;
  close: () => Promise<void>;
};

// Connects to Kafka and returns a function that puts one log into the topic. The function finishes
// only after Kafka has stored the log (kafkajs waits for every copy Kafka keeps to confirm), and
// throws if Kafka couldn't store it, for example when the topic doesn't exist.
export async function connectToKafka(brokers: string[], topic: string): Promise<KafkaConnection> {
  // Naming the partitioner (how logs are spread over the topic's partitions) is what silences
  // kafkajs's "switched default partitioner" notice. On Node 24 kafkajs also prints a harmless
  // "TimeoutNegativeWarning" once; that is a bug inside kafkajs, not a problem with our code.
  const producer = new Kafka({ clientId: 'api', brokers }).producer({
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();

  return {
    sendLog: async (log) => {
      await producer.send({ topic, messages: [{ value: JSON.stringify(log) }] });
    },
    close: () => producer.disconnect(),
  };
}
