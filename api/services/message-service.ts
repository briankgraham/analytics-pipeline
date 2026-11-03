// The service for POST /v1/message: how the API puts a log into Kafka, built on the base layer in
// kafka/client.ts. The rest of the API gets a plain `sendLog` function, so it can be tested without a
// running Kafka.
import type { Log } from '../../logs/log.ts';
import type { KafkaCalls } from '../../kafka/client.ts';

// Finishes only after Kafka has stored the log, and throws if it couldn't.
export type SendLog = (log: Log) => Promise<void>;

// Returns the function that puts one log into the given topic.
export function createLogSender(client: KafkaCalls, topic: string): SendLog {
  return (log) => client.sendMessages(topic, [JSON.stringify(log)]);
}
