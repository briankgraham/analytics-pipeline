// The base layer for Kafka, and the only file that uses the Kafka library. It knows nothing about logs:
// it sends text to a topic and hands over batches of text it reads. Deciding what that text means
// belongs to the files that build on this one.
import { isDeepStrictEqual } from 'node:util';
import { Kafka, Partitioners, type Consumer, type Producer } from 'kafkajs';
import type { Service } from '../services/service.ts';

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
  // Tells Kafka the reader is still working. Throws when the reader must stop and rejoin its group;
  // let that error through, because kafkajs uses it to rejoin.
  keepAlive: () => Promise<void>;
  // Records that this message, and all before it in the partition, won't need reading again. Kafka
  // is told when the handler returns, so after a crash mid-batch these are read again.
  markDone: (offset: string) => void;
  // True when handling should stop early: the client is closing, or the partition's position changed.
  shouldStop: () => boolean;
};

// 'finished' moves on to the next batch. 'pause' stops reading this partition for a while, then
// carries on from the first message that isn't marked done. Throw only for unexpected problems.
export type HandleBatch = (batch: MessageBatch) => Promise<'finished' | 'pause'>;

export type ReadOptions = {
  topic: string;
  group: string;
  // How long a pause lasts. The reader stays in its group during a pause.
  pauseMs: number;
  handleBatch: HandleBatch;
  // Called each time the reader joins its group, with the partitions it now reads.
  onPartitionsAssigned: (partitions: number[]) => void;
  // Called if kafkajs stops reading for good (it restarts by itself after most errors).
  onGiveUp: (error: Error) => void;
};

const closedMessage = 'KafkaClient is closed: get a new one with getInstance()';
const notRunningMessage = 'KafkaClient is not running: call init() first';

export class KafkaClient implements Service {
  private static instance: KafkaClient | undefined;

  private kafka: Kafka | undefined;
  private producer: Producer | undefined;
  private consumer: Consumer | undefined;
  private connecting: Promise<void> | undefined;
  private closing: Promise<void> | undefined;
  private ready = false;
  private closed = false;
  private readonly resumeTimers = new Set<NodeJS.Timeout>();
  private readonly brokers: string[];
  private readonly clientId: string;

  private constructor(brokers: string[], clientId: string) {
    this.brokers = brokers;
    this.clientId = clientId;
  }

  // Returns the one KafkaClient. `clientId` is the name Kafka knows this program by. Throws if it's
  // asked for with different settings, because those would otherwise be silently ignored.
  static getInstance(brokers: string[], clientId: string): KafkaClient {
    const existing = KafkaClient.instance;
    if (existing === undefined) {
      KafkaClient.instance = new KafkaClient(brokers, clientId);
      return KafkaClient.instance;
    }
    if (!isDeepStrictEqual(existing.brokers, brokers) || existing.clientId !== clientId) {
      throw new Error('KafkaClient is already running with different settings');
    }
    return existing;
  }

  // Connects to Kafka for sending. Calling it again keeps the same connection, even while the first
  // call is still working. After a failure, the next call tries again.
  init(): Promise<void> {
    if (this.closed) {
      return Promise.reject(new Error(closedMessage));
    }
    this.connecting ??= this.connect().catch((error: unknown) => {
      this.connecting = undefined;
      throw error;
    });
    return this.connecting;
  }

  private async connect(): Promise<void> {
    this.kafka ??= new Kafka({ clientId: this.clientId, brokers: this.brokers });
    // Naming the partitioner (how messages are spread over a topic's partitions) is what silences
    // kafkajs's "switched default partitioner" notice. On Node 24 kafkajs also prints a harmless
    // "TimeoutNegativeWarning" once; that is a bug inside kafkajs, not a problem with our code.
    this.producer ??= this.kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
    await this.producer.connect();
    this.ready = true;
  }

  // Stops reading first, so a batch still being handled can finish and send what it needs to, then
  // closes the connection for sending. Nothing new can start once this is called. Calling it again
  // gives the same answer, when the first call finishes.
  close(): Promise<void> {
    this.closing ??= this.closeConnections();
    return this.closing;
  }

  private async closeConnections(): Promise<void> {
    this.closed = true;
    if (KafkaClient.instance === this) {
      KafkaClient.instance = undefined;
    }
    for (const timer of this.resumeTimers) {
      clearTimeout(timer);
    }
    this.resumeTimers.clear();
    await this.connecting?.catch(() => {});

    try {
      await this.consumer?.disconnect();
    } catch (error) {
      // Printed here because if sending fails to close as well, only that error is passed on.
      console.error('kafka: could not stop reading:', error);
      throw error;
    } finally {
      // Reading failing to stop is no reason to leave the connection for sending open.
      this.ready = false;
      await this.producer?.disconnect();
    }
  }

  // Finishes only after Kafka has stored the messages (kafkajs waits for every copy Kafka keeps to
  // confirm). Throws if Kafka couldn't store them, for example when the topic doesn't exist. After a
  // throw, some may be stored already. This keeps working while reading stops during close, because a
  // batch still being handled may need it.
  async sendMessages(topic: string, messages: string[]): Promise<void> {
    if (!this.ready || this.producer === undefined) {
      throw new Error(this.closed ? closedMessage : notRunningMessage);
    }
    await this.producer.send({ topic, messages: messages.map((value) => ({ value })) });
  }

  // Says whether the topic exists. Asking doesn't create it.
  async topicExists(topic: string): Promise<boolean> {
    const { kafka } = this.started();
    const admin = kafka.admin();
    await admin.connect();
    try {
      await admin.fetchTopicMetadata({ topics: [topic] });
      return true;
    } catch (error) {
      if ((error as { type?: unknown }).type === 'UNKNOWN_TOPIC_OR_PARTITION') {
        return false;
      }
      throw error;
    } finally {
      await admin.disconnect();
    }
  }

  // Starts reading the topic and returns once reading has begun. There's one reader, so asking again
  // once it has begun throws. After a failure, the next call tries again. Kafka remembers, per group, which messages are done, so a reader that crashes reads the
  // rest again on restart. A group reading for the first time starts at the oldest message still in the
  // topic. A batch holds whatever is available, so nothing waits for a batch to fill up.
  async startReading(options: ReadOptions): Promise<void> {
    const { topic, group, pauseMs, handleBatch, onPartitionsAssigned, onGiveUp } = options;
    const { kafka } = this.started();
    if (this.consumer !== undefined) {
      throw new Error('KafkaClient is already reading');
    }
    const consumer = kafka.consumer({ groupId: group });
    this.consumer = consumer;

    consumer.on(consumer.events.CRASH, ({ payload }) => {
      if (!payload.restart) {
        onGiveUp(payload.error);
      }
    });

    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      onPartitionsAssigned(payload.memberAssignment[topic] ?? []);
    });

    try {
      await consumer.connect();
      await consumer.subscribe({ topic, fromBeginning: true });
      await this.runConsumer(consumer, options);
    } catch (error) {
      this.consumer = undefined;
      await consumer.disconnect().catch(() => {});
      throw error;
    }
  }

  private async runConsumer(consumer: Consumer, options: ReadOptions): Promise<void> {
    const { topic, pauseMs, handleBatch } = options;
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

        // A reader that's closing doesn't pause: it's stopping anyway, and a timer started now would
        // keep the program running after everything else has closed.
        if (answer === 'pause' && isRunning()) {
          console.warn(`kafka: pausing partition ${batch.partition} for ${pauseMs} ms`);
          const resume = pause();
          const timer = setTimeout(() => {
            this.resumeTimers.delete(timer);
            resume();
          }, pauseMs);
          this.resumeTimers.add(timer);
        }
      },
    });
  }

  private started(): { kafka: Kafka; producer: Producer } {
    if (this.closed) {
      throw new Error(closedMessage);
    }
    if (!this.ready || this.kafka === undefined || this.producer === undefined) {
      throw new Error(notRunningMessage);
    }
    return { kafka: this.kafka, producer: this.producer };
  }
}

// The calls the files above this one use, so their tests can stand in for Kafka.
export type KafkaCalls = Pick<KafkaClient, 'sendMessages' | 'topicExists'>;
