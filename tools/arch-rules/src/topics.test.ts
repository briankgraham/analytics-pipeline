import { readFileSync } from 'node:fs';
import path from 'node:path';
import { PARTITION_ALIGNED_TOPICS, TOPICS } from '@obs/schemas';
import { describe, expect, it } from 'vitest';

const confPath = path.resolve(import.meta.dirname, '../../../infra/kafka/topics.conf');

interface DeclaredTopic {
  partitions: number;
  configs: Map<string, string>;
}

/** The same format create-topics.sh reads: `<name> <partitions> <key>=<value> ...`. */
function declaredTopics(): Map<string, DeclaredTopic> {
  const topics = new Map<string, DeclaredTopic>();
  for (const line of readFileSync(confPath, 'utf8').split('\n')) {
    if (/^\s*(#|$)/.test(line)) continue;
    const [name = '', partitions = '', ...configs] = line.trim().split(/\s+/);
    topics.set(name, {
      partitions: Number(partitions),
      configs: new Map(
        configs.map((kv) => [kv.slice(0, kv.indexOf('=')), kv.slice(kv.indexOf('=') + 1)]),
      ),
    });
  }
  return topics;
}

describe('infra/kafka/topics.conf', () => {
  const topics = declaredTopics();

  it('declares exactly the topics the code uses', () => {
    expect([...topics.keys()].sort()).toEqual(Object.values(TOPICS).sort());
  });

  it('gives every topic a positive partition count and well-formed configs', () => {
    for (const [name, topic] of topics) {
      expect(Number.isSafeInteger(topic.partitions) && topic.partitions > 0, name).toBe(true);
      expect(topic.configs.get('cleanup.policy'), name).toMatch(
        /^(delete|compact|compact,delete)$/,
      );
      expect(Number(topic.configs.get('retention.ms')), name).toBeGreaterThan(0);
    }
  });

  it('keeps the partition-aligned topics at the same partition count', () => {
    const counts = PARTITION_ALIGNED_TOPICS.map((name) => topics.get(name)?.partitions);
    expect(new Set(counts).size).toBe(1);
  });

  it('matches the design table exactly (docs/design.md, Topic settings)', () => {
    const minute = 60_000;
    const hour = 60 * minute;
    const day = 24 * hour;
    const sideTopic = {
      partitions: 1,
      configs: { 'cleanup.policy': 'delete', 'retention.ms': 7 * day, 'segment.ms': day },
    };
    const design = {
      [TOPICS.raw]: {
        partitions: 12,
        configs: { 'cleanup.policy': 'delete', 'retention.ms': hour, 'segment.ms': 10 * minute },
      },
      [TOPICS.aggregates]: {
        partitions: 12,
        configs: {
          'cleanup.policy': 'compact,delete',
          'retention.ms': 6 * hour,
          'segment.ms': hour,
          'max.compaction.lag.ms': hour,
        },
      },
      [TOPICS.rejected]: { ...sideTopic, partitions: 12 },
      [TOPICS.late]: sideTopic,
      [TOPICS.dlq]: sideTopic,
      [TOPICS.alerts]: sideTopic,
    };
    const actual = Object.fromEntries(
      [...topics].map(([name, topic]) => [
        name,
        {
          partitions: topic.partitions,
          configs: Object.fromEntries(
            [...topic.configs].map(([key, value]) => [
              key,
              /^\d+$/.test(value) ? Number(value) : value,
            ]),
          ),
        },
      ]),
    );
    expect(actual).toEqual(design);
  });

  it('closes segments well before retention, or retention never takes effect', () => {
    for (const [name, topic] of topics) {
      expect(Number(topic.configs.get('segment.ms')), name).toBeLessThan(
        Number(topic.configs.get('retention.ms')),
      );
    }
  });
});
