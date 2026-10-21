import './native-augment.js';
// The package is CommonJS: Node's ESM loader detects only KafkaJS and RdKafka as named exports,
// so everything else comes off RdKafka (importing CODES directly fails outside Vitest).
import { KafkaJS, RdKafka } from '@confluentinc/kafka-javascript';
import {
  AssignmentGate,
  consumerErrorHandler,
  fromNativeOffset,
  fromPartitionResults,
  kafkaJsLogger,
  NO_REBALANCE_LISTENER,
  onRebalance,
  producerSettings,
  settle,
  settleVoid,
  toConsumedRecord,
  toKafkaError,
  toNativeOffset,
  toTopicPartitions,
  withStartOffsets,
  withTimeout,
  type KafkaError,
  type KafkaLog,
  type NativeMessage,
  type NativePartitionResult,
  type ProducerOptions,
} from './native.js';
import type {
  ConsumedRecord,
  Consumer,
  Producer,
  ProducerRecord,
  RebalanceListener,
  TopicPartition,
  TopicPartitionOffset,
  Watermarks,
} from './ports.js';
import { checkPollArgs } from './poll-args.js';

const { CODES, KafkaConsumer } = RdKafka;
const tpKey = (tp: TopicPartition): string => `${tp.topic}/${String(tp.partition)}`;
type KafkaConsumer = RdKafka.KafkaConsumer;

/** How long admin-style requests (commits, seeks, offset lookups) may take. */
export const REQUEST_TIMEOUT_MS = 10_000;

export interface ConfluentKafkaOptions {
  brokers: readonly string[];
  /** Kafka client.id: the service name, so broker logs and quotas can tell clients apart. */
  clientId: string;
  /** Client warnings and errors go here (a pino Logger fits), never to the console. */
  log: KafkaLog;
}

/**
 * The real Kafka behind our ports (ADR-0003). Producers use the KafkaJS-compatible API; consumers
 * use the native API, the only one with group-less assign() and a pull-style consume(n).
 */
export class ConfluentKafka {
  private readonly kafkaJs: KafkaJS.Kafka;

  constructor(private readonly options: ConfluentKafkaOptions) {
    this.kafkaJs = new KafkaJS.Kafka({
      kafkaJS: {
        brokers: [...options.brokers],
        clientId: options.clientId,
        logger: kafkaJsLogger(options.log),
      },
    });
  }

  producer(options: ProducerOptions = {}): Producer {
    return new ConfluentProducer(
      this.kafkaJs.producer({
        ...producerSettings(options),
        kafkaJS: { idempotent: true, acks: -1 },
      }),
    );
  }

  consumer(groupId: string): Consumer {
    return new ConfluentConsumer(this.options, groupId);
  }
}

class ConfluentProducer implements Producer {
  /** Memoised: shutdown paths can race (a signal and an error), and each must settle. */
  private disconnecting: Promise<void> | null = null;

  constructor(private readonly producer: KafkaJS.Producer) {}

  connect(): Promise<void> {
    return this.producer.connect();
  }

  async send(records: readonly ProducerRecord[]): Promise<void> {
    const byTopic = new Map<string, KafkaJS.Message[]>();
    for (const record of records) {
      const messages = byTopic.get(record.topic) ?? [];
      byTopic.set(record.topic, messages);
      messages.push(
        record.partition === undefined
          ? { key: record.key, value: record.value }
          : { key: record.key, value: record.value, partition: record.partition },
      );
    }
    await this.producer.sendBatch({
      topicMessages: [...byTopic].map(([topic, messages]) => ({ topic, messages })),
    });
  }

  disconnect(): Promise<void> {
    this.disconnecting ??= this.producer.disconnect();
    return this.disconnecting;
  }
}

class ConfluentConsumer implements Consumer {
  private readonly native: KafkaConsumer;
  private readonly gate = new AssignmentGate();
  private listener: RebalanceListener = NO_REBALANCE_LISTENER;
  /** Sticky: a failed rebalance callback leaves the consumer unusable; every poll reports it. */
  private failure: KafkaError | null = null;
  private polling = false;
  private disconnecting: Promise<void> | null = null;
  private committing = false;
  /** Partitions the caller paused; the rebalance's own pause/resume must not undo them. */
  private readonly paused = new Set<string>();
  /** Partitions whose onAssigned has not resolved: natively paused whatever the caller does. */
  private readonly pending = new Set<string>();

  constructor(options: ConfluentKafkaOptions, groupId: string) {
    this.native = new KafkaConsumer(
      {
        'metadata.broker.list': options.brokers.join(','),
        'client.id': options.clientId,
        'group.id': groupId,
        // Offsets are committed explicitly, only after the work they cover is durable.
        'enable.auto.commit': false,
        'enable.auto.offset.store': false,
        rebalance_cb: (error: { code: number }, assignment: TopicPartition[]) => {
          // librdkafka calls this without a try/catch: nothing may escape into native code.
          try {
            onRebalance(
              error.code,
              {
                assign: CODES.ERRORS.ERR__ASSIGN_PARTITIONS,
                revoke: CODES.ERRORS.ERR__REVOKE_PARTITIONS,
              },
              {
                assign: () => {
                  this.startAssigned(assignment);
                },
                revoke: () => {
                  this.revoke(assignment);
                },
              },
            );
          } catch (failure) {
            this.failure ??= toKafkaError(failure, 'rebalance');
          }
        },
      },
      { 'auto.offset.reset': 'earliest' },
    );
    // Without this, consume(n) waits the full timeout before every record, not just the first.
    this.native.setDefaultIsTimeoutOnlyForFirstMessage(true);
    this.native.on('event.error', consumerErrorHandler(options.log));
  }

  /**
   * Take ownership paused, ask the listener where each partition starts (it may load a
   * checkpoint), then re-assign at those offsets and resume. Seeking inside the callback is not
   * reliable in librdkafka (no fetch position yet), so start offsets go through assign().
   */
  private startAssigned(assignment: TopicPartition[]): void {
    this.native.assign(assignment);
    this.native.pause(assignment);
    for (const tp of assignment) this.pending.add(tpKey(tp));
    const signal = this.gate.next();
    this.listener
      .onAssigned(toTopicPartitions(assignment), signal)
      .then((starts) => {
        this.gate.ifCurrent(signal, () => {
          this.native.assign(withStartOffsets(assignment, starts));
          this.pending.clear();
          // Resume only what the caller has not paused meanwhile (backpressure must hold).
          const held = assignment.filter((tp) => this.paused.has(tpKey(tp)));
          this.native.resume(assignment.filter((tp) => !this.paused.has(tpKey(tp))));
          this.native.pause(held);
        });
      })
      // After then(): an invalid start offset or a failed re-assign must fail polls, not escape.
      .catch((failure: unknown) => {
        // A superseded listener may honour its signal by rejecting: that is not a failure.
        if (!signal.aborted) this.failure ??= toKafkaError(failure, 'onAssigned');
      });
  }

  private revoke(assignment: TopicPartition[]): void {
    this.gate.next();
    // A rebalance clears pause state, as it does in the fake.
    this.paused.clear();
    this.pending.clear();
    try {
      this.listener.onRevoked(toTopicPartitions(assignment));
    } finally {
      this.native.unassign();
    }
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.native.connect({}, settleVoid('connect', resolve, reject));
    });
  }

  subscribe(topics: readonly string[], listener: RebalanceListener): void {
    this.listener = listener;
    this.native.subscribe([...topics]);
  }

  assign(partitions: readonly TopicPartitionOffset[]): void {
    this.native.assign(
      partitions.map((p) => ({
        topic: p.topic,
        partition: p.partition,
        offset: toNativeOffset(p.offset),
      })),
    );
  }

  assignment(): readonly TopicPartition[] {
    return toTopicPartitions(this.native.assignments());
  }

  async poll(max: number, timeoutMs: number): Promise<ConsumedRecord[]> {
    if (this.failure !== null) throw this.failure;
    checkPollArgs(max, timeoutMs);
    if (this.polling) throw new Error('a poll is already in flight');
    this.polling = true;
    return new Promise<ConsumedRecord[]>((resolve, reject) => {
      this.native.setDefaultConsumeTimeout(timeoutMs);
      this.native.consume(
        max,
        settle(
          'poll',
          (messages: NativeMessage[]) => {
            resolve(messages.map(toConsumedRecord));
          },
          reject,
        ),
      );
    }).finally(() => {
      this.polling = false;
    });
  }

  commit(offsets: readonly TopicPartitionOffset[]): Promise<void> {
    if (this.committing) return Promise.reject(new Error('a commit is already in flight'));
    // Inside the executor: an invalid offset rejects (before the flag is set) instead of throwing.
    const committed = new Promise<void>((resolve, reject) => {
      const native = offsets.map((o) => ({
        topic: o.topic,
        partition: o.partition,
        offset: toNativeOffset(o.offset),
      }));
      this.native.commitCb(native, (error) => {
        // Cleared by the native result, not by the timeout: a timed-out commit still occupies a
        // threadpool worker, so retries must not pile up behind it.
        this.committing = false;
        settleVoid('commit', resolve, reject)(error);
      });
      // Set only once commitCb did not throw (it throws synchronously when disconnected).
      this.committing = true;
    });
    return withTimeout(committed, REQUEST_TIMEOUT_MS, 'commit');
  }

  committed(partitions: readonly TopicPartition[]): Promise<TopicPartitionOffset[]> {
    return new Promise((resolve, reject) => {
      this.native.committed(
        [...partitions],
        REQUEST_TIMEOUT_MS,
        settle(
          'committed',
          (results: NativePartitionResult[]) => {
            resolve(
              fromPartitionResults(results, 'committed').flatMap(({ topic, partition, offset }) =>
                offset === undefined ? [] : [{ topic, partition, offset }],
              ),
            );
          },
          reject,
        ),
      );
    });
  }

  seek(position: TopicPartitionOffset): Promise<void> {
    // Inside the executor: an invalid offset rejects instead of throwing.
    return new Promise((resolve, reject) => {
      const target = {
        topic: position.topic,
        partition: position.partition,
        offset: toNativeOffset(position.offset),
      };
      this.native.seek(target, REQUEST_TIMEOUT_MS, settleVoid('seek', resolve, reject));
    });
  }

  pause(partitions: readonly TopicPartition[]): void {
    this.native.pause([...partitions]);
    for (const tp of partitions) this.paused.add(tpKey(tp));
  }

  resume(partitions: readonly TopicPartition[]): void {
    for (const tp of partitions) this.paused.delete(tpKey(tp));
    // A pending partition starts reading only once its listener decided where (startAssigned).
    this.native.resume(partitions.filter((tp) => !this.pending.has(tpKey(tp))));
  }

  watermarks(partition: TopicPartition): Promise<Watermarks> {
    return new Promise((resolve, reject) => {
      this.native.queryWatermarkOffsets(
        partition.topic,
        partition.partition,
        REQUEST_TIMEOUT_MS,
        settle(
          'watermarks',
          (offsets: { lowOffset: number; highOffset: number }) => {
            resolve({
              low: fromNativeOffset(offsets.lowOffset),
              high: fromNativeOffset(offsets.highOffset),
            });
          },
          reject,
        ),
      );
    });
  }

  async offsetsForTimes(
    partitions: readonly TopicPartition[],
    timestamp: number,
  ): Promise<TopicPartitionOffset[]> {
    // librdkafka rejects an empty list ("Invalid argument"); the port answers it with nothing.
    if (partitions.length === 0) return [];
    const found = await new Promise<NativePartitionResult[]>((resolve, reject) => {
      this.native.offsetsForTimes(
        partitions.map((p) => ({ topic: p.topic, partition: p.partition, offset: timestamp })),
        REQUEST_TIMEOUT_MS,
        settle('offsetsForTimes', resolve, reject),
      );
    });
    // No record that recent (librdkafka answers -1): the port defines that as the high watermark.
    return Promise.all(
      fromPartitionResults(found, 'offsetsForTimes').map(async (o) => ({
        topic: o.topic,
        partition: o.partition,
        offset: o.offset ?? (await this.watermarks(o)).high,
      })),
    );
  }

  disconnect(): Promise<void> {
    // A second native disconnect() while one runs never calls back, so every caller shares one.
    this.disconnecting ??= new Promise((resolve, reject) => {
      this.native.disconnect(settleVoid('disconnect', resolve, reject));
    });
    return this.disconnecting;
  }
}
