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
  retries:
    maxRetries: 5
    delayMs: 500
    exponential: true
    jitter: true
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
      retries: { maxRetries: 5, delayMs: 500, exponential: true, jitter: true },
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

  it.each([
    ['maxRetries: 5', 'maxRetries: -1', 'maxRetries'],
    ['maxRetries: 5', 'maxRetries: 1.5', 'maxRetries'],
    ['delayMs: 500', 'delayMs: 0', 'delayMs'],
    ['delayMs: 500', 'delayMs: soon', 'delayMs'],
    ['exponential: true', 'exponential: yes', 'exponential'],
    ['jitter: true', 'jitter: maybe', 'jitter'],
  ])('rejects the retry setting %s changed to %s', (from, to, name) => {
    expect(() => readWorkerSettings(good.replace(from, to))).toThrow(
      `elasticsearch.retries.${name}`,
    );
  });

  it('rejects retries that are missing', () => {
    const noRetries = good.replace(/  retries:\n(    .*\n)+/, '');
    expect(noRetries).not.toContain('retries');
    expect(() => readWorkerSettings(noRetries)).toThrow('elasticsearch.retries.maxRetries');
  });

  it('rejects retries that would wait so long that Kafka thinks the worker is gone', () => {
    expect(() => readWorkerSettings(good.replace('maxRetries: 5', 'maxRetries: 6'))).toThrow(
      'must not wait more than',
    );
  });

  it('rejects retries that wait just over the longest allowed time', () => {
    const text = good
      .replace('maxRetries: 5', 'maxRetries: 1')
      .replace('delayMs: 500', 'delayMs: 16001')
      .replace('exponential: true', 'exponential: false');
    expect(() => readWorkerSettings(text)).toThrow('must not wait more than');
  });

  it('accepts retries that wait exactly the longest allowed time', () => {
    const text = good
      .replace('maxRetries: 5', 'maxRetries: 2')
      .replace('delayMs: 500', 'delayMs: 8000')
      .replace('exponential: true', 'exponential: false');
    expect(readWorkerSettings(text).retries.maxRetries).toBe(2);
  });
});
