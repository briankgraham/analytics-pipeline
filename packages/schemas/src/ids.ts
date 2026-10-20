import {
  IDENTIFIER_PATTERN,
  OFFSET_PATTERN,
  TOPIC_PATTERN,
  ULID_PATTERN,
  WINDOW_MS,
} from './primitives.js';

// Composite IDs are ES `_id`s, Kafka keys, and alert ids. Every part is validated so a `:` (or any
// malformed part) can never make two different keys collide.

function part(value: string, pattern: RegExp, name: string): string {
  if (!pattern.test(value)) throw new RangeError(`${name} ${JSON.stringify(value)} is invalid`);
  return value;
}

function nonNegativeInt(value: number, name: string): string {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer, got ${String(value)}`);
  }
  return String(value);
}

const id = (tenantId: string): string => part(tenantId, IDENTIFIER_PATTERN, 'identifier');

function windowStart(ms: number): string {
  const text = nonNegativeInt(ms, 'windowStart');
  if (ms % WINDOW_MS !== 0) throw new RangeError(`windowStart ${text} is not on the minute grid`);
  return text;
}

/** Raw event `_id` and the key of `events.late` / `events.rejected`: tenant-scoped (client ids are not unique across tenants). */
export function eventDocId(tenantId: string, eventId: string): string {
  return `${id(tenantId)}:${part(eventId, ULID_PATTERN, 'event id')}`;
}

/** `aggregates-v1` `_id` and the `aggregates` record key. */
export function windowId(
  tenantId: string,
  service: string,
  env: string,
  windowStartMs: number,
): string {
  return `${id(tenantId)}:${id(service)}:${id(env)}:${windowStart(windowStartMs)}`;
}

/** Deterministic alert id: a re-emitted alert after a restart has the same id, so `create` dedupes it. */
export function alertId(
  ruleId: string,
  tenantId: string,
  service: string,
  env: string,
  breachStartWindowMs: number,
): string {
  return `${id(ruleId)}:${windowId(tenantId, service, env, breachStartWindowMs)}`;
}

export const CHECKPOINT_OWNERS = ['aggregator', 'alert-evaluator'] as const;
export type CheckpointOwner = (typeof CHECKPOINT_OWNERS)[number];

/** `{service}:{partition}`: aggregator and alert-evaluator share partition numbers, never a doc. */
export function checkpointId(owner: CheckpointOwner, partition: number): string {
  return `${owner}:${nonNegativeInt(partition, 'partition')}`;
}

/** `events.dlq` key: undecodable input has no event id, so the source position identifies it. */
export function dlqKey(topic: string, partition: number, offset: string): string {
  return `${part(topic, TOPIC_PATTERN, 'topic')}:${nonNegativeInt(partition, 'partition')}:${part(offset, OFFSET_PATTERN, 'offset')}`;
}

/**
 * ES external version and the consumers' staleness guard: `(count + rejectedCount) * 2 + closed`.
 * Strictly increases on every fold, every counted rejection, and on closing, so the final closed
 * record always wins and a replayed or stale write always loses.
 */
export function revision(count: number, rejectedCount: number, closed: boolean): number {
  nonNegativeInt(count, 'count');
  nonNegativeInt(rejectedCount, 'rejectedCount');
  const value = (count + rejectedCount) * 2 + (closed ? 1 : 0);
  if (!Number.isSafeInteger(value)) throw new RangeError('revision overflows a safe integer');
  return value;
}
