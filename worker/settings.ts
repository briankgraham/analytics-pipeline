import { parse } from 'yaml';
import { readKafkaBrokers, section } from '../config/read-config.ts';
import { readTopics } from '../kafka/topics.ts';
import { longestTotalWaitMs, type RetrySettings } from '../retries/with-retries.ts';

export type WorkerSettings = {
  kafkaBrokers: string[];
  topic: string;
  group: string;
  elasticsearchUrl: string;
  index: string;
  replicas: number;
  retries: RetrySettings;
};

// While the worker waits to retry a log, it can't tell Kafka it's still alive. After 30 seconds of
// silence (kafkajs's default) Kafka drops the worker from the group. The worker speaks up after each
// failed round of retries (see kafka/read-logs.ts), but kafkajs then pauses for up to 12 seconds
// before the next round. One round's waits must fit in what's left, with time for the tries themselves.
const longestAllowedWaitMs = 16_000;

function readRetrySettings(value: unknown): RetrySettings {
  const { maxRetries, delayMs, exponential, jitter } = (
    typeof value === 'object' && value !== null ? value : {}
  ) as Record<string, unknown>;

  if (typeof maxRetries !== 'number' || !Number.isInteger(maxRetries) || maxRetries < 0) {
    throw new Error('elasticsearch.retries.maxRetries must be a whole number of at least 0');
  }
  if (typeof delayMs !== 'number' || !Number.isInteger(delayMs) || delayMs < 1) {
    throw new Error('elasticsearch.retries.delayMs must be a whole number of at least 1');
  }
  if (typeof exponential !== 'boolean') {
    throw new Error('elasticsearch.retries.exponential must be true or false');
  }
  if (typeof jitter !== 'boolean') {
    throw new Error('elasticsearch.retries.jitter must be true or false');
  }

  const settings = { maxRetries, delayMs, exponential, jitter };
  if (longestTotalWaitMs(settings) > longestAllowedWaitMs) {
    throw new Error(
      `elasticsearch.retries must not wait more than ${longestAllowedWaitMs} ms in one round of retries, or Kafka may drop the worker from its group`,
    );
  }
  return settings;
}

// Reads the worker's settings from the text of a config file (config/local.yml). Throws an error
// naming the first problem, so a mistake in the file stops the worker before it starts.
export function readWorkerSettings(yamlText: string): WorkerSettings {
  const file: unknown = parse(yamlText);
  const kafkaBrokers = readKafkaBrokers(file);
  const { topic, group } = section(file, 'worker');
  const { url, index, replicas, retries } = section(file, 'elasticsearch');

  // A topic that isn't listed under "topics" doesn't exist in Kafka, so catch the typo here.
  if (typeof topic !== 'string' || !readTopics(yamlText).some((known) => known.name === topic)) {
    throw new Error('worker.topic must be the name of a topic listed under "topics:"');
  }
  if (typeof group !== 'string' || group.trim() === '') {
    throw new Error('worker.group must be a name, like worker');
  }
  if (typeof url !== 'string' || !/^https?:\/\/./.test(url) || !URL.canParse(url)) {
    throw new Error('elasticsearch.url must be an address like http://127.0.0.1:9200');
  }
  // Elasticsearch's rules for index names, kept simple: lowercase letters, digits, '.', '_' and
  // '-', starting with a letter or digit, at most 255 characters.
  if (typeof index !== 'string' || !/^[a-z0-9][a-z0-9._-]{0,254}$/.test(index)) {
    throw new Error(
      "elasticsearch.index must be lowercase letters, digits, '.', '_' or '-', starting with a letter or digit",
    );
  }
  if (typeof replicas !== 'number' || !Number.isInteger(replicas) || replicas < 0) {
    throw new Error('elasticsearch.replicas must be a whole number of at least 0');
  }

  return {
    kafkaBrokers,
    topic,
    group,
    elasticsearchUrl: url,
    index,
    replicas,
    retries: readRetrySettings(retries),
  };
}
