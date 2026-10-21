import { OFFSET_PATTERN } from '@obs/schemas';
import { systemClock, type Clock } from './clock.js';
import { partitionFor } from './murmur2.js';
import { checkPollArgs } from './poll-args.js';
import type {
  ConsumedRecord,
  Consumer,
  Offset,
  Producer,
  ProducerRecord,
  RebalanceListener,
  TopicPartition,
  TopicPartitionOffset,
  Watermarks,
} from './ports.js';

interface Stored {
  readonly key: Buffer;
  readonly value: Buffer;
  readonly timestamp: number;
}

const nextMacrotask = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));
const tpKey = (tp: TopicPartition): string => `${tp.topic}/${String(tp.partition)}`;
const toBuffer = (value: Buffer | string): Buffer => Buffer.from(value);

/** The same offset validation the real adapter applies at its boundary. */
function checkedOffset(offset: Offset): bigint {
  if (!OFFSET_PATTERN.test(offset) || BigInt(offset) > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new RangeError(`offset ${JSON.stringify(offset)} is not a safe non-negative integer`);
  }
  return BigInt(offset);
}

/** A consumer group: members in join order, committed offsets, and an eager-rebalance generation. */
class Group {
  readonly members: MemoryConsumer[] = [];
  readonly committed = new Map<string, Offset>();
  generation = 0;

  /** Eager rebalance: every member gives up everything now, and rejoins on its next poll. */
  rebalance(): void {
    this.generation++;
    for (const member of this.members) member.revokeAll();
  }

  /** Range assignment: the sorted partitions, split into contiguous runs in member join order. */
  shareOf(member: MemoryConsumer, all: readonly TopicPartition[]): TopicPartition[] {
    const index = this.members.indexOf(member);
    const size = Math.ceil(all.length / this.members.length);
    return all.slice(index * size, (index + 1) * size);
  }
}

/**
 * An in-memory Kafka for unit tests, pinned to the real adapter by the shared contract suite.
 * Deliberate simplifications: no retention (the low watermark is always 0), a failed send stores
 * nothing, and poll never waits.
 */
export class InMemoryKafka {
  private readonly topics = new Map<string, Stored[][]>();
  private readonly groups = new Map<string, Group>();
  private readonly sendFailures: Error[] = [];

  constructor(private readonly clock: Clock = systemClock) {}

  createTopic(topic: string, partitions: number): void {
    if (this.topics.has(topic)) throw new Error(`topic ${topic} already exists`);
    if (!Number.isInteger(partitions) || partitions < 1) {
      throw new RangeError(`partitions must be a positive integer, got ${String(partitions)}`);
    }
    this.topics.set(
      topic,
      Array.from({ length: partitions }, () => []),
    );
  }

  /** The next `send` from any producer rejects with `error` and stores nothing. */
  failNextSend(error: Error): void {
    this.sendFailures.push(error);
  }

  /** Simulates a broker-initiated rebalance of `groupId` (members revoke, then rejoin). */
  rebalance(groupId: string): void {
    this.group(groupId).rebalance();
  }

  producer(): Producer {
    return new MemoryProducer(this);
  }

  consumer(groupId: string): Consumer {
    return new MemoryConsumer(this, groupId);
  }

  /** @internal */
  group(groupId: string): Group {
    let group = this.groups.get(groupId);
    if (group === undefined) {
      group = new Group();
      this.groups.set(groupId, group);
    }
    return group;
  }

  /** @internal */
  partitionsOf(topic: string): Stored[][] {
    const partitions = this.topics.get(topic);
    if (partitions === undefined) throw new Error(`unknown topic ${topic}`);
    return partitions;
  }

  /** @internal */
  log(tp: TopicPartition): Stored[] {
    const log = this.partitionsOf(tp.topic)[tp.partition];
    if (log === undefined) throw new RangeError(`unknown partition ${tpKey(tp)}`);
    return log;
  }

  /** @internal */
  append(records: readonly ProducerRecord[]): void {
    const failure = this.sendFailures.shift();
    if (failure !== undefined) throw failure;
    const placed = records.map((record) => {
      const count = this.partitionsOf(record.topic).length;
      const partition = record.partition ?? partitionFor(record.key, count);
      return { record, log: this.log({ topic: record.topic, partition }) };
    });
    const timestamp = this.clock.now();
    for (const { record, log } of placed) {
      log.push({ key: toBuffer(record.key), value: toBuffer(record.value), timestamp });
    }
  }
}

class MemoryProducer implements Producer {
  private connected = false;

  constructor(private readonly kafka: InMemoryKafka) {}

  connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }

  send(records: readonly ProducerRecord[]): Promise<void> {
    // Inside then(): a throw from validation or an injected failure becomes the rejection.
    return Promise.resolve().then(() => {
      if (!this.connected) throw new Error('producer is not connected');
      this.kafka.append(records);
    });
  }

  disconnect(): Promise<void> {
    this.connected = false;
    return Promise.resolve();
  }
}

interface Position {
  readonly tp: TopicPartition;
  next: bigint;
}

class MemoryConsumer implements Consumer {
  private connected = false;
  private readonly positions = new Map<string, Position>();
  private readonly paused = new Set<string>();
  private subscription: { topics: readonly string[]; listener: RebalanceListener } | null = null;
  /** The group generation this member last joined; behind the group's means "rejoin". */
  private joinedGeneration = -1;
  /** A share whose onAssigned has not resolved yet: owned (and listed) but not readable. */
  private pending: {
    readonly share: TopicPartition[];
    readonly controller: AbortController;
    /** Settles once the listener's decision is applied, has failed, or was superseded. */
    readonly settled: Promise<undefined>;
  } | null = null;
  /** Sticky: a failed group listener leaves the consumer unusable; every poll reports it. */
  private failure: { readonly error: unknown } | null = null;
  private polling = false;
  private committing = false;

  constructor(
    private readonly kafka: InMemoryKafka,
    private readonly groupId: string,
  ) {}

  private get group(): Group {
    return this.kafka.group(this.groupId);
  }

  connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }

  subscribe(topics: readonly string[], listener: RebalanceListener): void {
    this.requireConnected();
    for (const topic of topics) this.kafka.partitionsOf(topic);
    this.subscription = { topics, listener };
    this.group.members.push(this);
    this.group.rebalance();
  }

  assign(partitions: readonly TopicPartitionOffset[]): void {
    this.requireConnected();
    const next = partitions.map((p) => {
      this.kafka.log(p);
      return { tp: { topic: p.topic, partition: p.partition }, next: checkedOffset(p.offset) };
    });
    this.positions.clear();
    for (const position of next) this.positions.set(tpKey(position.tp), position);
  }

  assignment(): readonly TopicPartition[] {
    return [...[...this.positions.values()].map(({ tp }) => tp), ...(this.pending?.share ?? [])];
  }

  // Never waits for records: tests stay instant and deterministic (the port allows returning before
  // timeoutMs).
  async poll(max: number, timeoutMs: number): Promise<ConsumedRecord[]> {
    this.requireConnected();
    this.throwIfFailed();
    checkPollArgs(max, timeoutMs);
    if (this.polling) throw new Error('a poll is already in flight');
    this.polling = true;
    try {
      this.joinIfBehind();
      // Waits for the listener (not for timeoutMs) so tests stay deterministic; a rebalance that
      // supersedes it ends the wait, as it does for the real adapter.
      await this.pending?.settled;
      this.throwIfFailed();
      const records = this.take(max);
      // Yield a macrotask so a poll loop over an empty fake never starves the event loop.
      if (records.length === 0) await nextMacrotask();
      return records;
    } finally {
      this.polling = false;
    }
  }

  /** Round-robin, one record per partition per pass, like interleaved fetches. */
  private take(max: number): ConsumedRecord[] {
    const records: ConsumedRecord[] = [];
    const active = [...this.positions]
      .filter(([key]) => !this.paused.has(key))
      .map(([, position]) => ({ position, log: this.kafka.log(position.tp) }));
    // auto.offset.reset=earliest: a position past the end resets to the first record.
    for (const { position, log } of active) {
      if (position.next > BigInt(log.length)) position.next = 0n;
    }
    let progressed = true;
    while (progressed && records.length < max) {
      progressed = false;
      for (const { position, log } of active) {
        const stored = records.length < max ? log[Number(position.next)] : undefined;
        if (stored === undefined) continue;
        records.push({ ...position.tp, offset: String(position.next), ...stored });
        position.next++;
        progressed = true;
      }
    }
    return records;
  }

  commit(offsets: readonly TopicPartitionOffset[]): Promise<void> {
    if (this.committing) return Promise.reject(new Error('a commit is already in flight'));
    this.committing = true;
    return Promise.resolve()
      .then(() => {
        this.requireConnected();
        // All or nothing, like the real adapter: validate the whole batch before applying any of it.
        for (const o of offsets) checkedOffset(o.offset);
        for (const o of offsets) this.group.committed.set(tpKey(o), o.offset);
      })
      .finally(() => {
        this.committing = false;
      });
  }

  committed(partitions: readonly TopicPartition[]): Promise<TopicPartitionOffset[]> {
    return Promise.resolve().then(() => {
      this.requireConnected();
      return partitions.flatMap((tp) => {
        const offset = this.group.committed.get(tpKey(tp));
        return offset === undefined ? [] : [{ topic: tp.topic, partition: tp.partition, offset }];
      });
    });
  }

  seek(position: TopicPartitionOffset): Promise<void> {
    return Promise.resolve().then(() => {
      this.requireConnected();
      const owned = this.positions.get(tpKey(position));
      if (owned === undefined) throw new Error(`${tpKey(position)} is not assigned`);
      owned.next = checkedOffset(position.offset);
    });
  }

  pause(partitions: readonly TopicPartition[]): void {
    this.requireConnected();
    for (const tp of partitions) this.paused.add(tpKey(tp));
  }

  resume(partitions: readonly TopicPartition[]): void {
    this.requireConnected();
    for (const tp of partitions) this.paused.delete(tpKey(tp));
  }

  watermarks(partition: TopicPartition): Promise<Watermarks> {
    return Promise.resolve().then(() => {
      this.requireConnected();
      return { low: '0', high: String(this.kafka.log(partition).length) };
    });
  }

  offsetsForTimes(
    partitions: readonly TopicPartition[],
    timestamp: number,
  ): Promise<TopicPartitionOffset[]> {
    return Promise.resolve().then(() => {
      this.requireConnected();
      return partitions.map((tp) => {
        const log = this.kafka.log(tp);
        const index = log.findIndex((record) => record.timestamp >= timestamp);
        return { ...tp, offset: String(index === -1 ? log.length : index) };
      });
    });
  }

  disconnect(): Promise<void> {
    if (this.subscription !== null) {
      const { members } = this.group;
      // Revoke this member, then let the others rejoin without it.
      this.revokeAll();
      members.splice(members.indexOf(this), 1);
      this.group.rebalance();
    }
    this.positions.clear();
    this.paused.clear();
    this.subscription = null;
    this.connected = false;
    return Promise.resolve();
  }

  /** @internal Eager revoke: called by the group for every member when it rebalances. */
  revokeAll(): void {
    const subscription = this.subscription;
    const owned = this.assignment();
    if (subscription === null || owned.length === 0) return;
    // A listener still deciding where to start is superseded: its result will be ignored.
    this.pending?.controller.abort();
    this.pending = null;
    this.positions.clear();
    this.paused.clear();
    try {
      subscription.listener.onRevoked(owned);
    } catch (error) {
      this.failure ??= { error };
    }
  }

  /**
   * Rejoin after a rebalance: own this member's share at once, and start reading it where the
   * listener says once it resolves (in the background, like the real adapter, so polls go on).
   */
  private joinIfBehind(): void {
    const subscription = this.subscription;
    const group = this.group;
    if (subscription === null || this.joinedGeneration === group.generation) return;
    this.joinedGeneration = group.generation;
    const all = subscription.topics.flatMap((topic) =>
      this.kafka.partitionsOf(topic).map((_log, partition) => ({ topic, partition })),
    );
    const share = group.shareOf(this, all);
    if (share.length === 0) return;
    const controller = new AbortController();
    const { promise: settled, resolve: settle } = Promise.withResolvers<undefined>();
    controller.signal.addEventListener('abort', () => {
      settle(undefined);
    });
    // Owned before the listener runs, so a rebalance it triggers revokes (and aborts) this share.
    this.pending = { share, controller, settled };
    // The executor turns a listener that throws instead of rejecting into a rejection.
    new Promise<readonly TopicPartitionOffset[]>((resolve) => {
      resolve(subscription.listener.onAssigned(share, controller.signal));
    })
      .then((starts) => {
        // A rebalance while the listener ran supersedes this assignment.
        if (controller.signal.aborted) return;
        const startAt = new Map(starts.map((s) => [tpKey(s), s.offset]));
        // Validate every offset before taking any partition, as the real adapter's assign() does.
        const positions = share.map((tp) => ({
          tp,
          next: checkedOffset(startAt.get(tpKey(tp)) ?? group.committed.get(tpKey(tp)) ?? '0'),
        }));
        this.pending = null;
        for (const position of positions) this.positions.set(tpKey(position.tp), position);
      })
      .catch((error: unknown) => {
        // A superseded listener may honour its signal by rejecting: that is not a failure.
        if (!controller.signal.aborted) this.failure ??= { error };
      })
      .finally(() => {
        settle(undefined);
      });
  }

  private throwIfFailed(): void {
    if (this.failure !== null) throw this.failure.error;
  }

  private requireConnected(): void {
    if (!this.connected) throw new Error('consumer is not connected');
  }
}
