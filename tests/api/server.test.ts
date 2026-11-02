import { createServer } from 'node:net';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiServer } from '../../api/server.ts';

// Stands in for kafkajs, and records the order things happen in. The port is real.
const { producer, order } = vi.hoisted(() => {
  const order: string[] = [];
  return {
    order,
    producer: {
      connect: vi.fn(async () => void order.push('kafka opened')),
      disconnect: vi.fn(async () => void order.push('kafka closed')),
      send: vi.fn(),
    },
  };
});

vi.mock('kafkajs', () => ({
  Partitioners: {},
  Kafka: class {
    producer() {
      return producer;
    }
  },
}));

const good = { service: 'checkout', level: 'error', message: 'card declined' };

// Asks the system for a free port, by opening one for a moment.
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

let port: number;
let apiServer: ApiServer | undefined;

function getServer() {
  apiServer = ApiServer.getInstance({ port, kafkaBrokers: ['b:9092'], topic: 'logs' });
  return apiServer;
}

function postLog() {
  return fetch(`http://127.0.0.1:${port}/v1/message`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(good),
  });
}

beforeEach(async () => {
  vi.clearAllMocks();
  order.length = 0;
  vi.spyOn(console, 'log').mockImplementation(() => {});
  port = await freePort();
});

afterEach(async () => {
  await apiServer?.close();
  apiServer = undefined;
  vi.restoreAllMocks();
});

describe('ApiServer', () => {
  it('touches nothing until init', () => {
    getServer();
    expect(producer.connect).not.toHaveBeenCalled();
  });

  it('has only one instance, and refuses different settings', () => {
    expect(getServer()).toBe(getServer());
    expect(() =>
      ApiServer.getInstance({ port: port + 1, kafkaBrokers: ['b:9092'], topic: 'logs' }),
    ).toThrow('different settings');
  });

  it('connects its clients before it opens the port, then answers requests', async () => {
    producer.connect.mockImplementationOnce(async () => {
      order.push('kafka opened');
      await expect(postLog()).rejects.toThrow();
    });
    await getServer().init();

    const reply = await postLog();
    expect(reply.status).toBe(202);
    expect(producer.send).toHaveBeenCalledWith({
      topic: 'logs',
      messages: [{ value: JSON.stringify(good) }],
    });
  });

  it('starts once however many times init is called, even at the same time', async () => {
    const server = getServer();
    await Promise.all([server.init(), server.init()]);
    await server.init();
    expect(producer.connect).toHaveBeenCalledOnce();
  });

  it('finishes requests in progress and closes the port before it closes Kafka', async () => {
    const server = getServer();
    await server.init();

    let finishSend: () => void = () => {};
    producer.send.mockImplementationOnce(
      () => new Promise<void>((resolve) => (finishSend = resolve)),
    );
    const reply = postLog();
    await vi.waitFor(() => expect(producer.send).toHaveBeenCalled());

    const closing = server.close();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(order).toEqual(['kafka opened']);

    finishSend();
    expect((await reply).status).toBe(202);
    await closing;
    expect(order).toEqual(['kafka opened', 'kafka closed']);
    await expect(postLog()).rejects.toThrow();
  });

  it('reports a port already in use, and can still close what it opened', async () => {
    const other = createServer();
    await new Promise<void>((resolve) => other.listen(port, '127.0.0.1', resolve));
    const server = getServer();

    await expect(server.init()).rejects.toThrow('EADDRINUSE');
    await server.close();
    expect(order).toEqual(['kafka opened', 'kafka closed']);
    await new Promise((resolve) => other.close(resolve));
  });

  it('closes Kafka even if it never finished connecting', async () => {
    producer.connect.mockRejectedValueOnce(new Error('Kafka is down'));
    const server = getServer();
    await expect(server.init()).rejects.toThrow('Kafka is down');
    await server.close();
    expect(producer.disconnect).toHaveBeenCalledOnce();
  });

  it('does not open the port if it closed while connecting', async () => {
    let finishConnecting: () => void = () => {};
    producer.connect.mockImplementationOnce(
      () => new Promise<undefined>((resolve) => (finishConnecting = () => resolve(undefined))),
    );
    const server = getServer();
    const starting = server.init();
    const closing = server.close();
    finishConnecting();

    await expect(starting).rejects.toThrow('is closed');
    await closing;
    await expect(postLog()).rejects.toThrow();
  });

  it('refuses to start after close, and gives a new instance', async () => {
    const server = getServer();
    await server.close();
    await server.close();
    await expect(server.init()).rejects.toThrow('is closed');
    expect(getServer()).not.toBe(server);
  });
});
