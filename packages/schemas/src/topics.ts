/**
 * Kafka topic names. infra/kafka/topics.conf declares the same topics with their partitions and
 * configs; tools/arch-rules fails the build if the two drift apart.
 */
export const TOPICS = {
  raw: 'events.raw',
  aggregates: 'aggregates',
  rejected: 'events.rejected',
  late: 'events.late',
  dlq: 'events.dlq',
  alerts: 'alerts',
} as const;

export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];

/**
 * Topics whose partition N carries the output for input partition N of `events.raw` (the
 * aggregator produces with an explicit partition), so their partition counts must be equal.
 */
export const PARTITION_ALIGNED_TOPICS: readonly TopicName[] = [
  TOPICS.raw,
  TOPICS.aggregates,
  TOPICS.rejected,
];
