import { describe, expect, it } from 'vitest';
import { InMemoryKafka } from '../memory.js';
import { pollUntil, pollUntilError, text } from './contract.js';
import { freePort, portOf, retry } from './kafka-container.js';

describe('contract helpers', () => {
  it('pollUntil fails loudly instead of hanging when records never arrive', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    const consumer = kafka.consumer('g');
    await consumer.connect();
    consumer.assign([{ topic: 't', partition: 0, offset: '0' }]);
    await expect(pollUntil(consumer, 1, 0)).rejects.toThrow(
      'gave up waiting for 1 records (got 0)',
    );
  });

  it('pollUntilError fails loudly when every poll succeeds', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    const consumer = kafka.consumer('g');
    await consumer.connect();
    consumer.assign([{ topic: 't', partition: 0, offset: '0' }]);
    await expect(pollUntilError(consumer, 0)).rejects.toThrow('gave up waiting for a poll to fail');
  });

  it('text refuses a null buffer', () => {
    expect(text(Buffer.from('a'))).toBe('a');
    expect(() => text(null)).toThrow('expected a non-null key or value');
  });
});

describe('container helpers', () => {
  it('finds a free TCP port', async () => {
    const port = await freePort();
    expect(Number.isInteger(port) && port > 0 && port < 65_536).toBe(true);
  });

  it('portOf reads TCP addresses and refuses pipes and unbound servers', () => {
    expect(portOf({ address: '127.0.0.1', family: 'IPv4', port: 4321 })).toBe(4321);
    expect(() => portOf('/tmp/socket')).toThrow('expected a TCP address');
    expect(() => portOf(null)).toThrow('expected a TCP address');
  });
});

describe('retry', () => {
  it('returns the first success, retrying failures', async () => {
    let calls = 0;
    const result = await retry(3, () => {
      calls++;
      return calls < 3
        ? Promise.reject(new Error(`attempt ${String(calls)}`))
        : Promise.resolve('ok');
    });
    expect(result).toBe('ok');
    expect(calls).toBe(3);
  });

  it('throws the last failure once attempts run out, and refuses zero attempts', async () => {
    let calls = 0;
    await expect(
      retry(2, () => {
        calls++;
        return Promise.reject(new Error(`attempt ${String(calls)}`));
      }),
    ).rejects.toThrow('attempt 2');
    await expect(retry(0, () => Promise.resolve(1))).rejects.toThrow('at least one attempt');
  });
});
