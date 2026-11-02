import { afterEach, describe, expect, it, vi } from 'vitest';
import type { ElasticsearchCalls } from '../../elasticsearch/client.ts';
import { LogStorage } from '../../elasticsearch/log-storage.ts';

const retries = { maxRetries: 0, delayMs: 1, exponential: false, jitter: false };
const log = { service: 'checkout', level: 'error', message: 'card declined' } as const;

function standInClient() {
  return {
    createIndex: vi.fn<ElasticsearchCalls['createIndex']>(async () => 'created'),
    readIndexFields: vi.fn<ElasticsearchCalls['readIndexFields']>(async () => []),
    saveDocuments: vi.fn<ElasticsearchCalls['saveDocuments']>(async (_index, documents) =>
      documents.map(({ id }) => ({ id, saved: true })),
    ),
  };
}

let storage: LogStorage | undefined;

function getStorage(client: ElasticsearchCalls, replicas = 0) {
  storage = LogStorage.getInstance(client, 'logs', replicas, retries);
  return storage;
}

afterEach(async () => {
  await storage?.close();
  storage = undefined;
});

describe('LogStorage', () => {
  it('touches nothing until init, which sets up the given index', async () => {
    const client = standInClient();
    const logStorage = getStorage(client, 2);
    expect(client.createIndex).not.toHaveBeenCalled();

    await logStorage.init();
    expect(client.createIndex).toHaveBeenCalledWith('logs', expect.anything(), 2);
  });

  it('stops boot-up by throwing when the index cannot be set up', async () => {
    const client = standInClient();
    client.createIndex.mockRejectedValue(new Error('not allowed'));
    await expect(getStorage(client).init()).rejects.toThrow('not allowed');
  });

  it('saves logs into the given index', async () => {
    const client = standInClient();
    const logStorage = getStorage(client);
    await logStorage.init();
    await logStorage.saveLogs([{ id: 'a', log }], async () => {});
    expect(client.saveDocuments).toHaveBeenCalledWith('logs', [{ id: 'a', document: log }]);
  });

  it('has only one instance', () => {
    const client = standInClient();
    expect(getStorage(client)).toBe(getStorage(client));
  });

  it('treats the same retries written in another order as the same settings', () => {
    const client = standInClient();
    const { maxRetries, delayMs, exponential, jitter } = retries;
    const reordered = { jitter, exponential, delayMs, maxRetries };
    expect(LogStorage.getInstance(client, 'logs', 0, reordered)).toBe(getStorage(client));
  });

  it('refuses to save logs before init, without touching Elasticsearch', async () => {
    const client = standInClient();
    await expect(getStorage(client).saveLogs([{ id: 'a', log }], async () => {})).rejects.toThrow(
      'call init() first',
    );
    expect(client.saveDocuments).not.toHaveBeenCalled();
  });

  it('refuses to save logs after a failed init', async () => {
    const client = standInClient();
    client.createIndex.mockRejectedValue(new Error('not allowed'));
    const logStorage = getStorage(client);
    await expect(logStorage.init()).rejects.toThrow('not allowed');
    await expect(logStorage.saveLogs([{ id: 'a', log }], async () => {})).rejects.toThrow(
      'call init() first',
    );
  });

  it('refuses to save logs after close', async () => {
    const client = standInClient();
    const logStorage = getStorage(client);
    await logStorage.init();
    await logStorage.close();
    await expect(logStorage.saveLogs([{ id: 'a', log }], async () => {})).rejects.toThrow(
      'is closed',
    );
    expect(client.saveDocuments).not.toHaveBeenCalled();
  });

  it('waits for an init still working before close finishes', async () => {
    const client = standInClient();
    let finishCreating = (_answer: 'created') => {};
    client.createIndex.mockImplementation(
      () => new Promise((resolve) => (finishCreating = resolve)),
    );
    const logStorage = getStorage(client);
    const starting = logStorage.init();

    let closed = false;
    const closing = logStorage.close().then(() => (closed = true));
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(closed).toBe(false);

    finishCreating('created');
    await Promise.all([starting, closing]);
    expect(closed).toBe(true);
    await expect(logStorage.saveLogs([{ id: 'a', log }], async () => {})).rejects.toThrow(
      'is closed',
    );
  });

  it('closes cleanly even if an init still working then fails', async () => {
    const client = standInClient();
    client.createIndex.mockRejectedValue(new Error('not allowed'));
    const logStorage = getStorage(client);
    const starting = logStorage.init();
    await expect(logStorage.close()).resolves.toBeUndefined();
    await expect(starting).rejects.toThrow('not allowed');
  });

  it('refuses to be asked for with different settings', () => {
    const client = standInClient();
    getStorage(client);
    expect(() => LogStorage.getInstance(client, 'other', 0, retries)).toThrow('different settings');
    expect(() => LogStorage.getInstance(client, 'logs', 1, retries)).toThrow('different settings');
    for (const changed of [
      { maxRetries: 1 },
      { delayMs: 2 },
      { exponential: true },
      { jitter: true },
    ]) {
      expect(() => LogStorage.getInstance(client, 'logs', 0, { ...retries, ...changed })).toThrow(
        'different settings',
      );
    }
    expect(() => LogStorage.getInstance(standInClient(), 'logs', 0, retries)).toThrow(
      'different settings',
    );
  });

  it('sets up the index once however often init is called', async () => {
    const client = standInClient();
    const logStorage = getStorage(client);
    await logStorage.init();
    await logStorage.init();
    expect(client.createIndex).toHaveBeenCalledOnce();
  });

  it('shares one set-up between calls made at the same time', async () => {
    const client = standInClient();
    const logStorage = getStorage(client);
    await Promise.all([logStorage.init(), logStorage.init(), logStorage.init()]);
    expect(client.createIndex).toHaveBeenCalledOnce();
  });

  it('tries again on the next init after a failure', async () => {
    const client = standInClient();
    client.createIndex.mockRejectedValueOnce(new Error('not allowed'));
    const logStorage = getStorage(client);
    await expect(logStorage.init()).rejects.toThrow('not allowed');
    await expect(logStorage.init()).resolves.toBeUndefined();
    expect(client.createIndex).toHaveBeenCalledTimes(2);
  });

  it('refuses init after close, and getInstance then gives a new one', async () => {
    const client = standInClient();
    const closed = getStorage(client);
    await closed.close();
    await expect(closed.init()).rejects.toThrow('is closed');
    expect(getStorage(client)).not.toBe(closed);
  });

  it('can be closed twice without disturbing a newer instance', async () => {
    const client = standInClient();
    const closed = getStorage(client);
    await closed.close();
    const newer = getStorage(client);
    await closed.close();
    expect(getStorage(client)).toBe(newer);
  });
});
