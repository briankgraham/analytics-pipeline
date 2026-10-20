import { z } from 'zod';
import { EpochMs, Identifier, Ulid, utf8ByteLength } from './primitives.js';

export const LEVELS = ['debug', 'info', 'warn', 'error', 'fatal'] as const;
export const Level = z.enum(LEVELS);
export type Level = z.infer<typeof Level>;

export const MAX_MESSAGE_BYTES = 8 * 1024;
export const MAX_HOST_LENGTH = 255;
export const MAX_TRACE_ID_LENGTH = 128;
export const MAX_ATTR_KEYS = 32;
export const MAX_ATTR_KEY_LENGTH = 128;
export const MAX_ATTR_VALUE_LENGTH = 256;
export const MAX_BATCH_EVENTS = 500;

const AttrKey = z.string().min(1).max(MAX_ATTR_KEY_LENGTH);
// JSON.parse makes `__proto__` an own key; zod's record parser drops it silently, so check the
// raw object first. Cost: `attrs` is `unknown` on the schema's *input* side (z.input, input JSON
// Schema); the output type LogEvent is fully typed. Request docs need an explicit attrs schema.
const hasNoProtoKey = (value: unknown): boolean =>
  typeof value !== 'object' || value === null || !Object.hasOwn(value, '__proto__');
const AttrValue = z.union([z.string().max(MAX_ATTR_VALUE_LENGTH), z.number(), z.boolean()]);
const TraceField = z.string().min(1).max(MAX_TRACE_ID_LENGTH).optional();

/**
 * What a client sends (docs/design.md, Deep dive 1). Named LogEvent so it never shadows the DOM's
 * `Event` in the UI. Unknown fields are stripped, not rejected, so newer clients keep working, and
 * so a client can never supply `tenantId` or `receivedAt` (server-assigned; see RawEventSchema).
 * Checks that depend on arrival time (`ts` too far ahead or older than retention) belong to
 * ingest, which knows `receivedAt`.
 */
export const LogEventSchema = z.object({
  schemaVersion: z.literal(1),
  id: Ulid,
  ts: EpochMs,
  service: Identifier,
  env: Identifier,
  host: z.string().min(1).max(MAX_HOST_LENGTH).optional(),
  level: Level,
  message: z
    .string()
    .refine((m) => utf8ByteLength(m) <= MAX_MESSAGE_BYTES, { error: 'message_too_large' }),
  traceId: TraceField,
  spanId: TraceField,
  // zod 4 numbers reject Infinity and NaN by default.
  durationMs: z.number().nonnegative().optional(),
  attrs: z
    .unknown()
    .refine(hasNoProtoKey, { error: 'reserved_attr_key' })
    .pipe(
      z
        .record(AttrKey, AttrValue)
        .refine((a) => Object.keys(a).length <= MAX_ATTR_KEYS, { error: 'too_many_attrs' }),
    )
    .optional(),
});
export type LogEvent = z.infer<typeof LogEventSchema>;

/**
 * An event as stored in Kafka and ES: `tenantId` comes from the API key, `receivedAt` from ingest.
 * Never parse client input with this schema.
 */
export const RawEventSchema = LogEventSchema.extend({
  tenantId: Identifier,
  receivedAt: EpochMs,
});
export type RawEvent = z.infer<typeof RawEventSchema>;

/**
 * `POST /v1/events` body. Events stay `unknown` here: partial acceptance rejects them one by one
 * (by index, with a code), so one bad event must not fail the batch. The 1 MB limit is HTTP's.
 */
export const IngestBatchSchema = z.object({
  events: z.array(z.unknown()).min(1).max(MAX_BATCH_EVENTS),
});
export type IngestBatch = z.infer<typeof IngestBatchSchema>;
