import { z } from 'zod';

/** tenantId, service, env, and rule ids. No `:`, so `:`-joined composite IDs are unambiguous. */
export const IDENTIFIER_PATTERN = /^[a-z0-9-]{1,64}$/;
export const Identifier = z.string().regex(IDENTIFIER_PATTERN);

/** Crockford base32, 26 chars; the first char is at most 7 so the 48-bit time does not overflow. */
export const ULID_PATTERN = /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/;
export const Ulid = z.string().regex(ULID_PATTERN);

/** The largest timestamp a JS Date can hold; beyond it, day-index routing would throw. */
export const MAX_EPOCH_MS = 8.64e15;

/** Milliseconds since the epoch: event time or processing time, never mixed (see field names). */
export const EpochMs = z.int().min(0).max(MAX_EPOCH_MS);

/** Tumbling one-minute windows `[start, start + WINDOW_MS)`. */
export const WINDOW_MS = 60_000;

/** A window start: an epoch-ms instant on the one-minute grid. */
export const WindowStart = EpochMs.refine((ms) => ms % WINDOW_MS === 0, {
  error: 'window start must be a multiple of 60000 ms',
});

/**
 * Event counts. Bounded at 2^50 so `revision = (count + rejectedCount) * 2 + 1` always stays a
 * safe integer and validating a hostile record returns a failure instead of throwing.
 */
export const MAX_COUNT = 2 ** 50;
export const Count = z.int().min(0).max(MAX_COUNT);

/** Kafka offsets are int64; JSON numbers lose precision past 2^53, so offsets travel as strings. */
export const OFFSET_PATTERN = /^(0|[1-9][0-9]*)$/;
export const Offset = z.string().regex(OFFSET_PATTERN);

/** Kafka's legal topic names (letters, digits, `.`, `_`, `-`; up to 249 chars), except `.` and `..`. */
export const TOPIC_PATTERN = /^(?!\.{1,2}$)[a-zA-Z0-9._-]{1,249}$/;
export const Topic = z.string().regex(TOPIC_PATTERN);

export const Partition = z.int().nonnegative();

/** Where a side-topic record came from in its input topic. */
export const SourcePositionSchema = z.object({
  topic: Topic,
  partition: Partition,
  offset: Offset,
});
export type SourcePosition = z.infer<typeof SourcePositionSchema>;

/**
 * UTF-8 byte length without TextEncoder, so this package needs neither DOM nor Node typings. A
 * lone surrogate counts 3 bytes, as it does once encoded (it becomes U+FFFD).
 */
export function utf8ByteLength(text: string): number {
  let bytes = 0;
  // for...of yields whole code points: a surrogate pair (2 UTF-16 units) is an astral 4-byte char.
  for (const char of text) {
    const unit = char.charCodeAt(0);
    bytes += char.length === 2 ? 4 : unit < 0x80 ? 1 : unit < 0x800 ? 2 : 3;
  }
  return bytes;
}
