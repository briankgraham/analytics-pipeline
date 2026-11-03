import { parse } from 'yaml';
import { readKafkaBrokers, section } from '../config/read-config.ts';
import { readTopics } from '../kafka/topics.ts';
import { longestWaitMs, type RetrySettings } from '../retries/with-retries.ts';

export type WorkerSettings = {
  kafkaBrokers: string[];
  topic: string;
  // How many partitions the topic has, so the worker can handle them all at the same time.
  topicPartitions: number;
  failedTopic: string;
  group: string;
  waitBeforeTryingAgainMs: number;
  moveToFailedAfterMs: number;
  alertAfterMs: number;
  elasticsearchUrl: string;
  index: string;
  replicas: number;
  bulkSize: number;
  requestTimeoutMs: number;
  retries: RetrySettings;
};

// Kafka drops a worker from its group after 30 seconds of silence (kafkajs's default). The worker
// speaks up before each wait, so the silence is one wait plus one request. kafkajs can also hold back
// a heartbeat for up to 3 seconds if it sent one recently, which leaves this much.
const longestAllowedSilenceMs = 27_000;

// More retries than this would keep one round going for far too long.
const mostRetries = 20;

const dayMs = 24 * 60 * 60 * 1000;

// Returns a setting that must be a whole number of at least 1, or throws naming the setting.
function readWholeNumber(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a whole number of at least 1`);
  }
  return value;
}

function readRetrySettings(value: unknown, requestTimeoutMs: number): RetrySettings {
  const { maxRetries, delayMs, exponential, jitter } = (
    typeof value === 'object' && value !== null ? value : {}
  ) as Record<string, unknown>;

  if (typeof maxRetries !== 'number' || !Number.isInteger(maxRetries) || maxRetries < 0) {
    throw new Error('elasticsearch.retries.maxRetries must be a whole number of at least 0');
  }
  if (maxRetries > mostRetries) {
    throw new Error(`elasticsearch.retries.maxRetries must be at most ${mostRetries}`);
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
  if (longestWaitMs(settings) + requestTimeoutMs > longestAllowedSilenceMs) {
    throw new Error(
      `the longest wait in elasticsearch.retries plus elasticsearch.requestTimeoutMs must not be more than ${longestAllowedSilenceMs} ms, or Kafka may drop the worker from its group`,
    );
  }
  return settings;
}

// Reads the worker's settings from the text of a config file (config/local.yml). Throws an error
// naming the first problem, so a mistake in the file stops the worker before it starts.
export function readWorkerSettings(yamlText: string): WorkerSettings {
  const file: unknown = parse(yamlText);
  const kafkaBrokers = readKafkaBrokers(file);
  const worker = section(file, 'worker');
  const { topic, failedTopic, group } = worker;
  const { url, index, replicas, bulkSize, requestTimeoutMs, retries } = section(
    file,
    'elasticsearch',
  );

  // A topic that isn't listed under "topics" doesn't exist in Kafka, so catch the typo here.
  const topics = readTopics(yamlText);
  const logsTopic = topics.find((known) => known.name === topic);
  if (typeof topic !== 'string' || logsTopic === undefined) {
    throw new Error('worker.topic must be the name of a topic listed under "topics:"');
  }
  const failedLogsTopic = topics.find((known) => known.name === failedTopic);
  if (typeof failedTopic !== 'string' || failedLogsTopic === undefined || failedTopic === topic) {
    throw new Error('worker.failedTopic must be the name of another topic listed under "topics:"');
  }
  // Moving a log only helps if it happens before Kafka deletes it, and if the new home keeps it longer.
  const moveAfterDays = readWholeNumber(
    worker.moveToFailedAfterDays,
    'worker.moveToFailedAfterDays',
  );
  if (moveAfterDays >= logsTopic.retentionDays) {
    throw new Error(
      `worker.moveToFailedAfterDays must be less than the ${logsTopic.retentionDays} days the ${topic} topic keeps logs`,
    );
  }
  if (failedLogsTopic.retentionDays <= logsTopic.retentionDays) {
    throw new Error(`the ${failedTopic} topic must keep logs for longer than the ${topic} topic`);
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

  // With one log per request, no other log ever saves beside a stuck one, so it would never move.
  const groupSize = readWholeNumber(bulkSize, 'elasticsearch.bulkSize');
  if (groupSize < 2) {
    throw new Error('elasticsearch.bulkSize must be at least 2');
  }
  const timeoutMs = readWholeNumber(requestTimeoutMs, 'elasticsearch.requestTimeoutMs');

  return {
    kafkaBrokers,
    topic,
    topicPartitions: logsTopic.partitions,
    failedTopic,
    group,
    waitBeforeTryingAgainMs: readWholeNumber(
      worker.waitBeforeTryingAgainMs,
      'worker.waitBeforeTryingAgainMs',
    ),
    moveToFailedAfterMs: moveAfterDays * dayMs,
    alertAfterMs: readWholeNumber(worker.alertAfterMs, 'worker.alertAfterMs'),
    elasticsearchUrl: url,
    index,
    replicas,
    bulkSize: groupSize,
    requestTimeoutMs: timeoutMs,
    retries: readRetrySettings(retries, timeoutMs),
  };
}
