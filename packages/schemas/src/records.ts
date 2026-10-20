import { z } from 'zod';
import { Level, RawEventSchema } from './event.js';
import { alertId, revision } from './ids.js';
import {
  Count,
  EpochMs,
  Identifier,
  SourcePositionSchema,
  WINDOW_MS,
  WindowStart,
} from './primitives.js';

const Percentile = z.number().nonnegative().nullable();

/**
 * Cross-field rules run only when every field already parsed; zod runs refinements after
 * continuable field issues too, and these rules assume well-formed fields.
 */
const fieldsValid = {
  when: (payload: { issues: readonly unknown[] }) => payload.issues.length === 0,
};

/**
 * One record per (tenant, service, env, window) on the `aggregates` topic and in `aggregates-v1`,
 * holding counts for every level so an error-rate rule always sees a complete window. The
 * percentile sketch joins this record with the aggregator (Phase 3).
 */
export const WindowRecordSchema = z
  .object({
    tenantId: Identifier,
    service: Identifier,
    env: Identifier,
    windowStart: WindowStart,
    windowEnd: EpochMs,
    count: Count,
    // An enum-keyed record is exhaustive in zod 4: every level must be present.
    countsByLevel: z.record(Level, Count),
    durationCount: Count,
    p50: Percentile,
    p95: Percentile,
    p99: Percentile,
    rejectedCount: Count,
    closed: z.boolean(),
    revision: z.int().nonnegative(),
  })
  .refine((r) => r.windowEnd - r.windowStart === WINDOW_MS, {
    error: 'windowEnd must be windowStart + 60000',
    ...fieldsValid,
  })
  .refine((r) => Object.values(r.countsByLevel).reduce((sum, n) => sum + n, 0) === r.count, {
    error: 'countsByLevel must sum to count',
    ...fieldsValid,
  })
  .refine((r) => r.durationCount <= r.count, {
    error: 'durationCount cannot exceed count',
    ...fieldsValid,
  })
  .refine((r) => [r.p50, r.p95, r.p99].every((p) => (p !== null) === r.durationCount > 0), {
    error: 'percentiles are present exactly when durationCount > 0',
    ...fieldsValid,
  })
  .refine(
    (r) => r.p50 === null || r.p95 === null || r.p99 === null || (r.p50 <= r.p95 && r.p95 <= r.p99),
    { error: 'percentiles must satisfy p50 <= p95 <= p99', ...fieldsValid },
  )
  // Count is bounded (MAX_COUNT), so revision() cannot overflow and throw here.
  .refine((r) => r.revision === revision(r.count, r.rejectedCount, r.closed), {
    error: 'revision must equal (count + rejectedCount) * 2 + closed',
    ...fieldsValid,
  });
export type WindowRecord = z.infer<typeof WindowRecordSchema>;

/** v1 rule: error rate over one series' closed windows. Its id is derived, so restarts dedupe. */
export const AlertSchema = z
  .object({
    id: z.string(),
    ruleId: Identifier,
    tenantId: Identifier,
    service: Identifier,
    env: Identifier,
    breachStartWindow: WindowStart,
    lastWindowStart: WindowStart,
    errorRate: z.number().min(0).max(1),
    threshold: z.number().min(0).max(1),
  })
  .refine((a) => a.id === alertId(a.ruleId, a.tenantId, a.service, a.env, a.breachStartWindow), {
    error: 'id must be derived from the alert fields',
    ...fieldsValid,
  })
  .refine((a) => a.lastWindowStart >= a.breachStartWindow, {
    error: 'lastWindowStart precedes breachStartWindow',
    ...fieldsValid,
  });
export type Alert = z.infer<typeof AlertSchema>;

export const MAX_DLQ_ERROR_LENGTH = 4096;

/** `events.dlq` value: the undecodable or poison input, kept byte-for-byte. */
export const DlqEnvelopeSchema = z.object({
  originalBase64: z.base64(),
  stage: Identifier,
  error: z.string().max(MAX_DLQ_ERROR_LENGTH),
  attempts: z.int().positive(),
  source: SourcePositionSchema,
  failedAt: EpochMs,
});
export type DlqEnvelope = z.infer<typeof DlqEnvelopeSchema>;

export const REJECTION_REASONS = ['window_cap', 'dedup_cap'] as const;

/** `events.rejected` value: cap overflow in the aggregator, never silently dropped. */
export const RejectedEventSchema = z.object({
  event: RawEventSchema,
  reason: z.enum(REJECTION_REASONS),
  source: SourcePositionSchema,
});
export type RejectedEvent = z.infer<typeof RejectedEventSchema>;

/** `events.late` value: the event's window was already finalized by this watermark. */
export const LateEventSchema = z
  .object({
    event: RawEventSchema,
    windowStart: WindowStart,
    watermark: EpochMs,
    source: SourcePositionSchema,
  })
  .refine((l) => l.windowStart <= l.event.ts && l.event.ts < l.windowStart + WINDOW_MS, {
    error: "windowStart must be the event's own window",
    ...fieldsValid,
  })
  .refine((l) => l.watermark >= l.windowStart + WINDOW_MS, {
    error: 'a late event needs a watermark at or past its window end',
    ...fieldsValid,
  });
export type LateEvent = z.infer<typeof LateEventSchema>;
