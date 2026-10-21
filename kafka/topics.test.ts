import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { partitionCountFrom, readTopics } from './topics.ts';

describe('readTopics', () => {
  // Every environment's config must be valid and list the same topics, even the ones not used yet.
  const environments = ['local', 'sandbox', 'stage', 'prod'];
  const topicNames = (environment: string) => {
    const text = readFileSync(new URL(`../config/${environment}.yml`, import.meta.url), 'utf8');
    return readTopics(text).map((topic) => topic.name);
  };

  it.each(environments)('reads config/%s.yml', (environment) => {
    expect(topicNames(environment)).toContain('logs');
  });

  it.each(environments)('config/%s.yml lists the same topics as local', (environment) => {
    expect(topicNames(environment)).toEqual(topicNames('local'));
  });

  it('reads each topic', () => {
    const text = `
topics:
  - name: logs
    partitions: 3
    purpose: incoming logs
  - name: alerts
    partitions: 1
    purpose: alerts to send
`;
    expect(readTopics(text)).toEqual([
      { name: 'logs', partitions: 3, purpose: 'incoming logs' },
      { name: 'alerts', partitions: 1, purpose: 'alerts to send' },
    ]);
  });

  it('allows an empty list', () => {
    expect(readTopics('topics: []')).toEqual([]);
  });

  it.each([
    ['an empty file', '', '"topics:" list'],
    ['no topics list', 'other: 1', '"topics:" list'],
    ['topics that is not a list', 'topics: logs', '"topics:" list'],
    ['an entry that is not a topic', 'topics: [logs]', 'topic #1 must have'],
    ['a missing name', 'topics: [{partitions: 3, purpose: x}]', 'topic #1: name'],
    ['a name with a space', 'topics: [{name: "my logs", partitions: 3, purpose: x}]', 'name'],
    ['partitions as a word', 'topics: [{name: logs, partitions: three, purpose: x}]', 'partitions'],
    ['partitions as text', 'topics: [{name: logs, partitions: "3", purpose: x}]', 'partitions'],
    ['zero partitions', 'topics: [{name: logs, partitions: 0, purpose: x}]', 'partitions'],
    ['fractional partitions', 'topics: [{name: logs, partitions: 1.5, purpose: x}]', 'partitions'],
    ['a missing purpose', 'topics: [{name: logs, partitions: 3}]', 'purpose'],
    ['a blank purpose', 'topics: [{name: logs, partitions: 3, purpose: " "}]', 'purpose'],
    ['the name "."', 'topics: [{name: ".", partitions: 3, purpose: x}]', 'name'],
    ['the name ".."', 'topics: [{name: "..", partitions: 3, purpose: x}]', 'name'],
    [
      'names that differ only by "." and "_"',
      'topics: [{name: a.b, partitions: 3, purpose: x}, {name: a_b, partitions: 1, purpose: y}]',
      'a_b clashes with a.b',
    ],
    [
      'a name listed twice',
      'topics: [{name: logs, partitions: 3, purpose: x}, {name: logs, partitions: 1, purpose: y}]',
      'listed twice',
    ],
  ])('rejects %s', (_, text, message) => {
    expect(() => readTopics(text)).toThrow(message);
  });

  it('rejects text that is not valid YAML', () => {
    expect(() => readTopics('topics: [')).toThrow();
  });
});

describe('partitionCountFrom', () => {
  // Copied from real kafka-topics.sh --describe output.
  const logs =
    'Topic: logs\tTopicId: TttgTjhqS-WNNuV_yKrHIg\tPartitionCount: 3\tReplicationFactor: 1\tConfigs: min.insync.replicas=1\n' +
    '\tTopic: logs\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\n';

  it('reads the count from real kafka-topics.sh output', () => {
    expect(partitionCountFrom(logs, 'logs')).toBe(3);
  });

  it('reads only the line for the exact name', () => {
    // Describing "a.b" also matches "axb", because Kafka treats the name as a pattern.
    const output =
      'Topic: axb\tTopicId: x\tPartitionCount: 7\tReplicationFactor: 1\n' +
      'Topic: a.b\tTopicId: y\tPartitionCount: 2\tReplicationFactor: 1\n';
    expect(partitionCountFrom(output, 'a.b')).toBe(2);
  });

  it('fails when the topic is missing', () => {
    expect(() => partitionCountFrom(logs, 'alerts')).toThrow('PartitionCount of alerts');
  });
});
