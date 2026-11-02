import { parse } from 'yaml';
import { readKafkaBrokers, section } from '../config/read-config.ts';
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
  const kafkaBrokers = readKafkaBrokers(file);
  const { port, topic } = section(file, 'api');

  if (typeof port !== 'number' || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('api.port must be a whole number from 1 to 65535');
  }
  // A topic that isn't listed under "topics" doesn't exist in Kafka, so catch the typo here.
  if (typeof topic !== 'string' || !readTopics(yamlText).some((known) => known.name === topic)) {
    throw new Error('api.topic must be the name of a topic listed under "topics:"');
  }

  return { port, kafkaBrokers, topic };
}
