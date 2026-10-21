// Pure helpers for the confluent adapter. All of the adapter's branching lives here, so it is unit
// tested directly; the adapter classes in confluent.ts are thin and run by the contract suite.
import { OFFSET_PATTERN } from '@obs/schemas';
import type { ConsumedRecord, Offset, RebalanceListener, TopicPartition } from './ports.js';

/** A native client error with its librdkafka code kept (the native objects are not Errors). */
export class KafkaError extends Error {
  override readonly name = 'KafkaError';

  constructor(
    message: string,
    readonly code: number | undefined,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

interface NativeErrorLike {
  message?: unknown;
  code?: unknown;
}

export function toKafkaError(error: unknown, context: string): KafkaError {
  const like: NativeErrorLike = typeof error === 'object' && error !== null ? error : {};
  const message = typeof like.message === 'string' ? like.message : String(error);
  const code = typeof like.code === 'number' ? like.code : undefined;
  return new KafkaError(`${context}: ${message}`, code, { cause: error });
}

/** A node-style (err, value) callback that settles a promise; native errors become KafkaErrors. */
export function settle<T>(
  context: string,
  resolve: (value: T) => void,
  reject: (error: KafkaError) => void,
): (error: unknown, value: T) => void {
  return (error, value) => {
    if (error) {
      reject(toKafkaError(error, context));
      return;
    }
    // Called from native code: a throw while mapping the value must reject, never escape.
    try {
      resolve(value);
    } catch (failure) {
      reject(toKafkaError(failure, context));
    }
  };
}

/** `settle` for callbacks that pass only an error. */
export function settleVoid(
  context: string,
  resolve: () => void,
  reject: (error: KafkaError) => void,
): (error: unknown) => void {
  return (error) => {
    if (error) reject(toKafkaError(error, context));
    else resolve();
  };
}

/**
 * The native binding carries offsets as JS numbers. Ours are strings; convert at the boundary and
 * refuse anything a number cannot represent exactly instead of silently rounding.
 */
export function toNativeOffset(offset: Offset): number {
  const value = Number(offset);
  if (!OFFSET_PATTERN.test(offset) || !Number.isSafeInteger(value)) {
    throw new RangeError(`offset ${JSON.stringify(offset)} is not a safe non-negative integer`);
  }
  return value;
}

/** The inbound direction: a native offset as ours, refusing anything already rounded past 2^53. */
export function fromNativeOffset(offset: number): Offset {
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new RangeError(`native offset ${String(offset)} is not a safe non-negative integer`);
  }
  return String(offset);
}

/**
 * A per-partition result from the binding (committed, offsetsForTimes). A partition that failed
 * arrives as a bare Error, and "no offset" as a negative sentinel or no `offset` at all.
 */
export type NativePartitionResult = { topic: string; partition: number; offset?: number } | Error;

/** Our offsets per partition, `undefined` where there is none; throws if any partition failed. */
export function fromPartitionResults(
  results: readonly NativePartitionResult[],
  context: string,
): { topic: string; partition: number; offset: Offset | undefined }[] {
  return results.map((result) => {
    if (result instanceof Error) throw toKafkaError(result, context);
    const { topic, partition, offset } = result;
    return {
      topic,
      partition,
      offset: offset === undefined || offset < 0 ? undefined : fromNativeOffset(offset),
    };
  });
}

export interface NativeMessage {
  topic: string;
  partition: number;
  offset: number;
  key?: Buffer | string | null | undefined;
  value: Buffer | null;
  timestamp?: number | undefined;
}

export function toConsumedRecord(message: NativeMessage): ConsumedRecord {
  const key = message.key ?? null;
  return {
    topic: message.topic,
    partition: message.partition,
    offset: fromNativeOffset(message.offset),
    key: typeof key === 'string' ? Buffer.from(key) : key,
    value: message.value,
    // The broker always stamps records; 0 only if a client library omits the field.
    timestamp: message.timestamp ?? 0,
  };
}

export const toTopicPartitions = (
  assignment: readonly { topic: string; partition: number }[],
): TopicPartition[] => assignment.map(({ topic, partition }) => ({ topic, partition }));

export interface RebalanceActions {
  assign(): void;
  revoke(): void;
}

/**
 * With a user `rebalance_cb`, librdkafka leaves assign/unassign to us. Assignment and revocation
 * are the only two codes it delivers here; anything else is a broken assumption, so it throws.
 */
export function onRebalance(
  code: number,
  codes: { assign: number; revoke: number },
  actions: RebalanceActions,
): void {
  if (code === codes.assign) actions.assign();
  else if (code === codes.revoke) actions.revoke();
  else throw new KafkaError(`unexpected rebalance code ${String(code)}`, code);
}

/**
 * How long a record may wait for its acknowledgement, retries included, before send() rejects.
 * librdkafka's default is 5 minutes, far too long for ingest to hold a request during an outage.
 */
export const DELIVERY_TIMEOUT_MS = 30_000;

export interface ProducerOptions {
  /** librdkafka `linger.ms`; 0 for latency-sensitive producers such as ingest (ADR-0003). */
  lingerMs?: number;
  /** librdkafka `message.timeout.ms`; defaults to DELIVERY_TIMEOUT_MS. */
  deliveryTimeoutMs?: number;
}

/** librdkafka producer settings shared by every producer (ADR-0003). */
export function producerSettings(options: ProducerOptions): Record<string, string | number> {
  return {
    // Explicit, not relied on: the KafkaJS-compatible layer only defaults to murmur2_random when
    // given kafkaJS options, and the native default (CRC32) would move keys to other partitions.
    partitioner: 'murmur2_random',
    'message.timeout.ms': options.deliveryTimeoutMs ?? DELIVERY_TIMEOUT_MS,
    ...(options.lingerMs === undefined ? {} : { 'linger.ms': options.lingerMs }),
  };
}

/** The logging surface the adapter needs; a pino Logger satisfies it. */
export interface KafkaLog {
  warn(fields: Record<string, unknown>, message: string): void;
  error(fields: Record<string, unknown>, message: string): void;
}

/** Routes the KafkaJS-compatible client's logging to our logger (warnings and errors only). */
export function kafkaJsLogger(
  log: KafkaLog,
  namespace = 'kafka',
): {
  info: (message: string, extra?: object) => void;
  debug: (message: string, extra?: object) => void;
  warn: (message: string, extra?: object) => void;
  error: (message: string, extra?: object) => void;
  namespace: (name: string) => ReturnType<typeof kafkaJsLogger>;
  setLogLevel: () => void;
} {
  const ignore = (): void => undefined;
  return {
    info: ignore,
    debug: ignore,
    warn: (message, extra) => {
      log.warn({ kafka: { namespace, ...extra } }, message);
    },
    error: (message, extra) => {
      log.error({ kafka: { namespace, ...extra } }, message);
    },
    namespace: (name) => kafkaJsLogger(log, `${namespace}.${name}`),
    setLogLevel: ignore,
  };
}

/** The native consumer's `event.error` handler. */
export function consumerErrorHandler(log: KafkaLog): (error: unknown) => void {
  return (error) => {
    log.error({ err: toKafkaError(error, 'consumer') }, 'kafka consumer error');
  };
}

/** Rejects with a KafkaError if `promise` has not settled within `timeoutMs`. */
export function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  context: string,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new KafkaError(`${context} timed out after ${String(timeoutMs)} ms`, undefined));
    }, timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => {
    clearTimeout(timer);
  });
}

/**
 * Rebalances supersede each other. A start-offset decision that resolves after a later rebalance
 * belongs to an assignment that no longer exists and must not be applied.
 */
export class AssignmentGate {
  private current = new AbortController();

  /** Supersedes the current assignment (aborting its signal) and returns the next one's. */
  next(): AbortSignal {
    this.current.abort();
    this.current = new AbortController();
    return this.current.signal;
  }

  /** Runs `apply` only if no rebalance happened since `signal` was issued. */
  ifCurrent(signal: AbortSignal, apply: () => void): void {
    if (!signal.aborted) apply();
  }
}

/** The assignment with start offsets applied; partitions without one keep committed/earliest. */
export function withStartOffsets(
  assignment: readonly { topic: string; partition: number }[],
  starts: readonly { topic: string; partition: number; offset: Offset }[],
): { topic: string; partition: number; offset?: number }[] {
  const byPartition = new Map(starts.map((s) => [`${s.topic}/${String(s.partition)}`, s.offset]));
  return assignment.map(({ topic, partition }) => {
    const offset = byPartition.get(`${topic}/${String(partition)}`);
    return offset === undefined
      ? { topic, partition }
      : { topic, partition, offset: toNativeOffset(offset) };
  });
}

/** A consumer's listener before subscribe(): group callbacks cannot fire then, so it does nothing. */
export const NO_REBALANCE_LISTENER: RebalanceListener = {
  onAssigned: () => Promise.resolve([]),
  onRevoked: () => undefined,
};
