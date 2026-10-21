import { parse } from 'yaml';

export type Topic = {
  name: string;
  partitions: number;
  purpose: string;
};

// Reads the topics list from the text of a config file (config/local.yml and the others) and checks
// it. Throws an error naming the first problem found, so a mistake in the file stops `pnpm topics`
// before anything is sent to Kafka.
export function readTopics(yamlText: string): Topic[] {
  const file: unknown = parse(yamlText);
  if (!isObject(file) || !Array.isArray(file.topics)) {
    throw new Error('the config must have a "topics:" list');
  }

  const topics: Topic[] = [];
  for (const [index, entry] of file.topics.entries()) {
    const where = `topic #${index + 1}`;
    if (!isObject(entry)) {
      throw new Error(`${where} must have name, partitions, and purpose`);
    }
    const { name, partitions, purpose } = entry;
    // Kafka's rules for topic names: letters, digits, '.', '_', '-', at most 249 characters, and
    // not '.' or '..'.
    if (
      typeof name !== 'string' ||
      !/^[a-zA-Z0-9._-]{1,249}$/.test(name) ||
      name === '.' ||
      name === '..'
    ) {
      throw new Error(`${where}: name must be letters, digits, '.', '_' or '-'`);
    }
    if (typeof partitions !== 'number' || !Number.isInteger(partitions) || partitions < 1) {
      throw new Error(`${name}: partitions must be a whole number of at least 1`);
    }
    if (typeof purpose !== 'string' || purpose.trim() === '') {
      throw new Error(`${name}: purpose must say what the topic is for`);
    }
    if (topics.some((topic) => topic.name === name)) {
      throw new Error(`${name} is listed twice`);
    }
    // Kafka treats '.' and '_' as the same in names when checking for clashes, so it refuses to
    // have both "a.b" and "a_b".
    const clash = topics.find((topic) => sameToKafka(topic.name, name));
    if (clash) {
      throw new Error(`${name} clashes with ${clash.name}: Kafka treats '.' and '_' as the same`);
    }
    topics.push({ name, partitions, purpose });
  }
  return topics;
}

// Finds a topic's partition count in the output of `kafka-topics.sh --describe`. Each topic has a
// line like "Topic: logs<tab>TopicId: ...<tab>PartitionCount: 3<tab>...". The output can include
// other topics too, because Kafka treats the name given to --describe as a pattern ("a.b" also
// matches "axb"), so only the line for this exact name counts.
export function partitionCountFrom(describeOutput: string, name: string): number {
  for (const line of describeOutput.split('\n')) {
    if (line.startsWith(`Topic: ${name}\t`)) {
      const match = /PartitionCount:\s*(\d+)/.exec(line);
      if (match?.[1]) {
        return Number(match[1]);
      }
    }
  }
  throw new Error(`could not find the PartitionCount of ${name} in:\n${describeOutput}`);
}

function sameToKafka(a: string, b: string): boolean {
  return a.replaceAll('.', '_') === b.replaceAll('.', '_');
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
