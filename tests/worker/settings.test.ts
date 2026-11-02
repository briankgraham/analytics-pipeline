import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { readWorkerSettings } from '../../worker/settings.ts';

const good = `
kafka:
  brokers: ['127.0.0.1:9092']
elasticsearch:
  url: http://127.0.0.1:9200
  index: logs
  replicas: 0
worker:
  topic: logs
  group: worker
topics:
  - name: logs
    partitions: 3
    purpose: Accepted logs.
`;

describe('readWorkerSettings', () => {
  it('reads config/local.yml', () => {
    const text = readFileSync(new URL('../../config/local.yml', import.meta.url), 'utf8');
    expect(readWorkerSettings(text)).toEqual({
      kafkaBrokers: ['127.0.0.1:9092'],
      topic: 'logs',
      group: 'worker',
      elasticsearchUrl: 'http://127.0.0.1:9200',
      index: 'logs',
      replicas: 0,
    });
  });

  it('rejects a missing section', () => {
    expect(() => readWorkerSettings(good.replace('kafka:', 'kafkaa:'))).toThrow('"kafka:" section');
    expect(() => readWorkerSettings(good.replace('worker:', 'workerr:'))).toThrow(
      '"worker:" section',
    );
    expect(() => readWorkerSettings(good.replace('elasticsearch:', 'elastic:'))).toThrow(
      '"elasticsearch:" section',
    );
  });

  it('rejects brokers that are not a list of addresses', () => {
    expect(() => readWorkerSettings(good.replace("['127.0.0.1:9092']", '[]'))).toThrow(
      'kafka.brokers',
    );
  });

  it('rejects a topic that is not listed under topics', () => {
    expect(() => readWorkerSettings(good.replace('topic: logs', 'topic: logz'))).toThrow(
      'worker.topic',
    );
  });

  it.each(["''", "'  '", '42'])('rejects the group %s', (group) => {
    expect(() => readWorkerSettings(good.replace('group: worker', `group: ${group}`))).toThrow(
      'worker.group',
    );
  });

  it.each(['127.0.0.1:9200', 'ftp://127.0.0.1', 'http://'])('rejects the url %s', (url) => {
    expect(() =>
      readWorkerSettings(good.replace('url: http://127.0.0.1:9200', `url: ${url}`)),
    ).toThrow('elasticsearch.url');
  });

  it.each(['Logs', '_logs', '-logs', '.', 'my logs', 'a/b', 'x'.repeat(256)])(
    'rejects the index name %s',
    (index) => {
      expect(() => readWorkerSettings(good.replace('index: logs', `index: '${index}'`))).toThrow(
        'elasticsearch.index',
      );
    },
  );

  it.each(['-1', '0.5', 'none'])('rejects replicas %s', (replicas) => {
    expect(() => readWorkerSettings(good.replace('replicas: 0', `replicas: ${replicas}`))).toThrow(
      'elasticsearch.replicas',
    );
  });
});
