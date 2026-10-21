// The Kafka surface services use (ADR-0003). Services depend on these interfaces only: the
// confluent adapter implements them for real, InMemoryKafka for unit tests, and one contract suite
// (src/testing/contract.ts) pins both to the same semantics.

/**
 * A Kafka offset. Always a decimal string: offsets are int64, and JSON numbers lose precision past
 * 2^53. Committed and start offsets name the *next* record to read (Kafka's convention).
 */
export type Offset = string;

export interface TopicPartition {
  readonly topic: string;
  readonly partition: number;
}

export interface TopicPartitionOffset extends TopicPartition {
  readonly offset: Offset;
}

export interface ProducerRecord {
  readonly topic: string;
  /** Routing key; the partition is murmur2(key) unless `partition` is given. */
  readonly key: string;
  readonly value: Buffer | string;
  /** Explicit partition (the aggregator writes to its input partition number). */
  readonly partition?: number;
}

export interface ConsumedRecord extends TopicPartitionOffset {
  readonly key: Buffer | null;
  readonly value: Buffer | null;
  /** Record timestamp, epoch ms (producer create time). */
  readonly timestamp: number;
}

export interface Producer {
  connect(): Promise<void>;
  /**
   * Resolves once the broker has acknowledged every record (acks=all, idempotent). Rejects if any
   * record failed, or was not acknowledged within the producer's delivery timeout (30 s unless
   * configured); callers retry the whole batch, which is safe because consumers dedupe by ID.
   */
  send(records: readonly ProducerRecord[]): Promise<void>;
  /** Idempotent: concurrent and repeated calls all settle with the same result. */
  disconnect(): Promise<void>;
}

export interface Watermarks {
  /** First retained offset. */
  readonly low: Offset;
  /** Offset the next produced record will get; a caught-up consumer's position equals it. */
  readonly high: Offset;
}

/** Group membership callbacks. */
export interface RebalanceListener {
  /**
   * Partitions this member now owns. Resolves with start offsets for any partition that should not
   * start at its committed offset (or earliest): the aggregator restores its checkpoint here and
   * starts at the checkpoint's `nextOffset`. No record from these partitions is returned until it
   * resolves. If it rejects (or resolves with an invalid offset) while current, every later `poll`
   * rejects.
   *
   * A rebalance can land while it is still running: the partitions are then revoked (`onRevoked`
   * is called for them, and `assignment()` already lists them while it runs), `signal` aborts, and
   * whatever it resolves or rejects with is ignored, so it may honour the signal by rejecting. The
   * listener must not keep state it loads after `signal` aborted; a new `onAssigned` follows for
   * whatever this member owns next.
   */
  onAssigned(
    partitions: readonly TopicPartition[],
    signal: AbortSignal,
  ): Promise<readonly TopicPartitionOffset[]>;
  /** Partitions this member no longer owns; anything it holds for them must be dropped. */
  onRevoked(partitions: readonly TopicPartition[]): void;
}

/**
 * One poll and one commit at a time: a second `poll` or `commit` while one is in flight rejects.
 */
export interface Consumer {
  connect(): Promise<void>;
  /** Joins the consumer group for `topics`. */
  subscribe(topics: readonly string[], listener: RebalanceListener): void;
  /** Group-less: reads exactly these partitions from these offsets (query-api's live map). */
  assign(partitions: readonly TopicPartitionOffset[]): void;
  assignment(): readonly TopicPartition[];
  /**
   * Up to `max` records from owned, unpaused partitions, in offset order per partition. Waits up to
   * `timeoutMs` for the *first* record, then returns what is available; [] if nothing arrived.
   * Rejects with a RangeError unless `max` is a positive integer and `timeoutMs` a finite number
   * >= 0 (the native client reads 0 as a never-settling stream and a negative timeout as forever).
   * A start or seek offset past the high watermark or below the low watermark resets to the
   * earliest retained record (auto.offset.reset=earliest).
   */
  poll(max: number, timeoutMs: number): Promise<ConsumedRecord[]>;
  /**
   * Commits group offsets (each = the next offset to read); rejects on failure or timeout. A commit
   * that timed out may still be running natively, and counts as in flight until it finishes.
   */
  commit(offsets: readonly TopicPartitionOffset[]): Promise<void>;
  /** Committed group offsets; partitions the group never committed are omitted. */
  committed(partitions: readonly TopicPartition[]): Promise<TopicPartitionOffset[]>;
  /** The next poll for an owned partition starts at `offset` (not for use inside onAssigned). */
  seek(position: TopicPartitionOffset): Promise<void>;
  /**
   * Holds until `resume`, including for partitions whose `onAssigned` is still running. A rebalance
   * clears pause state: newly assigned partitions start unpaused.
   */
  pause(partitions: readonly TopicPartition[]): void;
  resume(partitions: readonly TopicPartition[]): void;
  watermarks(partition: TopicPartition): Promise<Watermarks>;
  /**
   * For each partition, the first offset whose record timestamp is >= `timestamp`, or the high
   * watermark if there is none.
   */
  offsetsForTimes(
    partitions: readonly TopicPartition[],
    timestamp: number,
  ): Promise<TopicPartitionOffset[]>;
  /**
   * Leaves the group (revoking its partitions) and closes; any later call fails, except disconnect,
   * which is idempotent (concurrent and repeated calls all settle with the same result).
   */
  disconnect(): Promise<void>;
}
