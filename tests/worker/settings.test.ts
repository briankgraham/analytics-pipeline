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
  bulkSize: 500
  requestTimeoutMs: 10000
  retries:
    maxRetries: 5
    delayMs: 500
    exponential: true
    jitter: true
worker:
  topic: logs
  failedTopic: failed
  group: worker
  pauseMs: 120000
  moveStuckLogAfterRounds: 10
  moveToFailedAfterDays: 6
  alertAfterMs: 600000
topics:
  - name: logs
    partitions: 3
    purpose: Accepted logs.
    retentionDays: 7
  - name: failed
    partitions: 3
    purpose: Logs that could not be saved.
    retentionDays: 30
`;

describe('readWorkerSettings', () => {
  it('reads config/local.yml', () => {
    const text = readFileSync(new URL('../../config/local.yml', import.meta.url), 'utf8');
    expect(readWorkerSettings(text)).toEqual({
      kafkaBrokers: ['127.0.0.1:9092'],
      topic: 'logs',
      failedTopic: 'failed',
      group: 'worker',
      pauseMs: 120_000,
      moveStuckLogAfterRounds: 10,
      moveToFailedAfterMs: 6 * 24 * 60 * 60 * 1000,
      alertAfterMs: 600_000,
      elasticsearchUrl: 'http://127.0.0.1:9200',
      index: 'logs',
      replicas: 0,
      bulkSize: 500,
      requestTimeoutMs: 10_000,
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

  it('rejects a wait and a request that would together leave Kafka thinking the worker is gone', () => {
    // The sixth retry waits 16 seconds, which with a 10-second request is within the 27 allowed.
    const six = good.replace('maxRetries: 5', 'maxRetries: 6');
    expect(readWorkerSettings(six).retries.maxRetries).toBe(6);
    expect(() =>
      readWorkerSettings(six.replace('requestTimeoutMs: 10000', 'requestTimeoutMs: 11001')),
    ).toThrow('must not be more than');
    expect(() => readWorkerSettings(good.replace('maxRetries: 5', 'maxRetries: 7'))).toThrow(
      'must not be more than',
    );
  });

  it('rejects more retries than would ever make sense, even with short waits', () => {
    const text = good
      .replace('maxRetries: 5', 'maxRetries: 21')
      .replace('exponential: true', 'exponential: false');
    expect(() => readWorkerSettings(text)).toThrow('maxRetries must be at most 20');
  });

  it('rejects a bulk size of 1, because a stuck log could then never be noticed', () => {
    expect(() => readWorkerSettings(good.replace('bulkSize: 500', 'bulkSize: 1'))).toThrow(
      'elasticsearch.bulkSize must be at least 2',
    );
  });

  it('rejects a pause too long for a timer', () => {
    expect(() =>
      readWorkerSettings(good.replace('pauseMs: 120000', 'pauseMs: 2147483648')),
    ).toThrow('worker.pauseMs must be at most');
  });

  it('accepts a bigger bulk size', () => {
    const text = good.replace('bulkSize: 500', 'bulkSize: 1000');
    expect(readWorkerSettings(text).bulkSize).toBe(1000);
  });

  it.each([
    ['bulkSize: 500', 'elasticsearch.bulkSize'],
    ['requestTimeoutMs: 10000', 'elasticsearch.requestTimeoutMs'],
    ['pauseMs: 120000', 'worker.pauseMs'],
    ['moveStuckLogAfterRounds: 10', 'worker.moveStuckLogAfterRounds'],
    ['moveToFailedAfterDays: 6', 'worker.moveToFailedAfterDays'],
    ['alertAfterMs: 600000', 'worker.alertAfterMs'],
  ])('rejects bad values for %s', (setting, name) => {
    const key = setting.split(':')[0];
    for (const value of ['0', '-1', '1.5', 'soon']) {
      expect(() => readWorkerSettings(good.replace(setting, `${key}: ${value}`))).toThrow(name);
    }
  });

  it('rejects a failed topic that is not listed, or is the same as the logs topic', () => {
    for (const failedTopic of ['nope', 'logs']) {
      const text = good.replace('failedTopic: failed', `failedTopic: ${failedTopic}`);
      expect(() => readWorkerSettings(text)).toThrow('worker.failedTopic');
    }
  });

  it('rejects moving logs only after Kafka would already have deleted them', () => {
    const text = good.replace('moveToFailedAfterDays: 6', 'moveToFailedAfterDays: 7');
    expect(() => readWorkerSettings(text)).toThrow('moveToFailedAfterDays must be less than');
  });

  it('rejects a failed topic that keeps logs no longer than the logs topic', () => {
    const text = good.replace('retentionDays: 30', 'retentionDays: 7');
    expect(() => readWorkerSettings(text)).toThrow('must keep logs for longer');
  });
});
