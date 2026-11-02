import { parse } from 'yaml';
import { readKafkaBrokers, section } from '../config/read-config.ts';
import { readTopics } from '../kafka/topics.ts';

export type WorkerSettings = {
  kafkaBrokers: string[];
  topic: string;
  group: string;
  elasticsearchUrl: string;
  index: string;
  replicas: number;
};

// Reads the worker's settings from the text of a config file (config/local.yml). Throws an error
// naming the first problem, so a mistake in the file stops the worker before it starts.
export function readWorkerSettings(yamlText: string): WorkerSettings {
  const file: unknown = parse(yamlText);
  const kafkaBrokers = readKafkaBrokers(file);
  const { topic, group } = section(file, 'worker');
  const { url, index, replicas } = section(file, 'elasticsearch');

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

  return { kafkaBrokers, topic, group, elasticsearchUrl: url, index, replicas };
}
