import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { readApiSettings } from '../../api/settings.ts';

const good = `
kafka:
  brokers: ['127.0.0.1:9092']
api:
  port: 3000
  topic: logs
topics:
  - name: logs
    partitions: 3
    purpose: Accepted logs.
`;

describe('readApiSettings', () => {
  it('reads config/local.yml', () => {
    const text = readFileSync(new URL('../../config/local.yml', import.meta.url), 'utf8');
    expect(readApiSettings(text)).toEqual({
      port: 3000,
      kafkaBrokers: ['127.0.0.1:9092'],
      topic: 'logs',
    });
  });

  it('rejects a missing section', () => {
    expect(() => readApiSettings(good.replace('kafka:', 'kafkaa:'))).toThrow('"kafka:" section');
    expect(() => readApiSettings(good.replace('api:', 'apii:'))).toThrow('"api:" section');
  });

  it('rejects brokers that are not a list of addresses', () => {
    expect(() => readApiSettings(good.replace("['127.0.0.1:9092']", '[]'))).toThrow(
      'kafka.brokers',
    );
    expect(() => readApiSettings(good.replace("['127.0.0.1:9092']", '127.0.0.1:9092'))).toThrow(
      'kafka.brokers',
    );
  });

  it.each(['0', '70000', '3000.5', 'abc'])('rejects the port %s', (port) => {
    expect(() => readApiSettings(good.replace('port: 3000', `port: ${port}`))).toThrow('api.port');
  });

  it('rejects a topic that is not listed under topics', () => {
    expect(() => readApiSettings(good.replace('topic: logs', 'topic: logz'))).toThrow('api.topic');
  });
});
