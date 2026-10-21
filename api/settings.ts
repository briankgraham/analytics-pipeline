import { parse } from 'yaml';
import { readTopics } from '../kafka/topics.ts';

export type ApiSettings = {
  port: number;
  kafkaBrokers: string[];
  topic: string;
};

// Reads the API's settings from the text of a config file (config/local.yml). Throws an error naming
// the first problem, so a mistake in the file stops the API before it starts.
export function readApiSettings(yamlText: string): ApiSettings {
  const file: unknown = parse(yamlText);
  const kafka = section(file, 'kafka');
  const api = section(file, 'api');

  const { brokers } = kafka;
  if (
    !Array.isArray(brokers) ||
    brokers.length === 0 ||
    !brokers.every((broker) => typeof broker === 'string' && broker !== '')
  ) {
    throw new Error('kafka.brokers must be a list of addresses like 127.0.0.1:9092');
  }

  const { port, topic } = api;
  if (typeof port !== 'number' || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('api.port must be a whole number from 1 to 65535');
  }
  // A topic that isn't listed under "topics" doesn't exist in Kafka, so catch the typo here.
  if (typeof topic !== 'string' || !readTopics(yamlText).some((known) => known.name === topic)) {
    throw new Error('api.topic must be the name of a topic listed under "topics:"');
  }

  return { port, kafkaBrokers: brokers as string[], topic };
}

function section(file: unknown, name: string): Record<string, unknown> {
  const value =
    typeof file === 'object' && file !== null ? (file as Record<string, unknown>)[name] : undefined;
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`the config must have a "${name}:" section`);
  }
  return value as Record<string, unknown>;
}
