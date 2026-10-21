import { describe, expect, it } from 'vitest';
import { PARTITION_ALIGNED_TOPICS, TOPIC_PATTERN, TOPICS } from './index.js';

describe('TOPICS', () => {
  it('are valid, distinct Kafka topic names', () => {
    const names = Object.values(TOPICS);
    expect(new Set(names).size).toBe(names.length);
    for (const name of names) expect(name).toMatch(TOPIC_PATTERN);
  });

  it('aligns the aggregator input with its partition-matched outputs', () => {
    expect(PARTITION_ALIGNED_TOPICS).toEqual(['events.raw', 'aggregates', 'events.rejected']);
  });
});
