import { describe, expect, it } from 'vitest';
import { FakeClock } from './clock.js';
import { InMemoryKafka } from './memory.js';
import { kafkaContract } from './testing/contract.js';

kafkaContract('InMemoryKafka', () => {
  const kafka = new InMemoryKafka();
  return {
    createTopic: (topic, partitions) => {
      kafka.createTopic(topic, partitions);
      return Promise.resolve();
    },
    producer: () => kafka.producer(),
    consumer: (groupId) => kafka.consumer(groupId),
    pollTimeoutMs: 0,
  };
});

describe('InMemoryKafka specifics', () => {
  async function connected(kafka: InMemoryKafka) {
    const producer = kafka.producer();
    await producer.connect();
    const consumer = kafka.consumer('g');
    await consumer.connect();
    return { producer, consumer };
  }

  it('stamps records with the injected clock', async () => {
    const clock = new FakeClock(1_000);
    const kafka = new InMemoryKafka(clock);
    kafka.createTopic('t', 1);
    const { producer, consumer } = await connected(kafka);
    await producer.send([{ topic: 't', key: 'k', value: Buffer.from('a') }]);
    clock.advance(500);
    await producer.send([{ topic: 't', key: 'k', value: 'b' }]);
    consumer.assign([{ topic: 't', partition: 0, offset: '0' }]);
    expect((await consumer.poll(10, 0)).map((r) => r.timestamp)).toEqual([1_000, 1_500]);
    expect(await consumer.offsetsForTimes([{ topic: 't', partition: 0 }], 1_200)).toEqual([
      { topic: 't', partition: 0, offset: '1' },
    ]);
  });

  it('fails the next send without storing anything, then recovers', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    const { producer, consumer } = await connected(kafka);
    kafka.failNextSend(new Error('broker down'));
    await expect(producer.send([{ topic: 't', key: 'k', value: 'a' }])).rejects.toThrow(
      'broker down',
    );
    expect(await consumer.watermarks({ topic: 't', partition: 0 })).toEqual({
      low: '0',
      high: '0',
    });
    await producer.send([{ topic: 't', key: 'k', value: 'a' }]);
    expect(await consumer.watermarks({ topic: 't', partition: 0 })).toEqual({
      low: '0',
      high: '1',
    });
  });

  it('rejects a whole batch atomically when any record is invalid', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 2);
    const { producer, consumer } = await connected(kafka);
    await expect(
      producer.send([
        { topic: 't', key: 'k', value: 'ok', partition: 0 },
        { topic: 't', key: 'k', value: 'bad', partition: 2 },
      ]),
    ).rejects.toThrow('unknown partition t/2');
    await expect(producer.send([{ topic: 'missing', key: 'k', value: 'a' }])).rejects.toThrow(
      'unknown topic missing',
    );
    expect(await consumer.watermarks({ topic: 't', partition: 0 })).toEqual({
      low: '0',
      high: '0',
    });
  });

  it('requires connect before use', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    const producer = kafka.producer();
    await expect(producer.send([{ topic: 't', key: 'k', value: 'a' }])).rejects.toThrow(
      'producer is not connected',
    );
    await producer.connect();
    await producer.disconnect();
    await expect(producer.send([{ topic: 't', key: 'k', value: 'a' }])).rejects.toThrow(
      'producer is not connected',
    );
    const consumer = kafka.consumer('g');
    expect(() => {
      consumer.assign([{ topic: 't', partition: 0, offset: '0' }]);
    }).toThrow('consumer is not connected');
    await expect(consumer.poll(1, 0)).rejects.toThrow('consumer is not connected');
  });

  it('validates topics, partitions, and seeks', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    expect(() => {
      kafka.createTopic('t', 1);
    }).toThrow('already exists');
    expect(() => {
      kafka.createTopic('u', 0);
    }).toThrow(RangeError);
    const { consumer } = await connected(kafka);
    expect(() => {
      consumer.subscribe(['missing'], {
        onAssigned: () => Promise.resolve([]),
        onRevoked: () => undefined,
      });
    }).toThrow('unknown topic missing');
    await expect(consumer.seek({ topic: 't', partition: 0, offset: '0' })).rejects.toThrow(
      't/0 is not assigned',
    );
  });

  it('does not report a revocation for a group member that never got partitions', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    const { consumer } = await connected(kafka);
    const revoked: unknown[] = [];
    consumer.subscribe(['t'], {
      onAssigned: () => Promise.resolve([]),
      onRevoked: (p) => revoked.push(p),
    });
    await consumer.disconnect();
    expect(revoked).toEqual([]);
  });
});

describe('InMemoryKafka groups', () => {
  const noStarts = () => Promise.resolve([]);

  async function member(kafka: InMemoryKafka, revoked: unknown[] = []) {
    const consumer = kafka.consumer('g');
    await consumer.connect();
    consumer.subscribe(['t'], { onAssigned: noStarts, onRevoked: (p) => revoked.push(p) });
    await consumer.poll(10, 0);
    return consumer;
  }

  it('rebalance() revokes every member, which then rejoins on its next poll', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 2);
    const revoked: unknown[] = [];
    const consumer = await member(kafka, revoked);
    expect(consumer.assignment()).toHaveLength(2);
    kafka.rebalance('g');
    expect(consumer.assignment()).toEqual([]);
    expect(revoked).toHaveLength(1);
    await consumer.poll(10, 0);
    expect(consumer.assignment()).toHaveLength(2);
  });

  it('gives a member with no share nothing, and no assignment callback', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    await member(kafka);
    const assigned: unknown[] = [];
    const late = kafka.consumer('g');
    await late.connect();
    late.subscribe(['t'], {
      onAssigned: (p) => (assigned.push(p), noStarts()),
      onRevoked: () => undefined,
    });
    await late.poll(10, 0);
    expect(late.assignment()).toEqual([]);
    expect(assigned).toEqual([]);
  });

  it('drops an assignment that a rebalance superseded while its listener ran', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    const consumer = kafka.consumer('g');
    await consumer.connect();
    let calls = 0;
    consumer.subscribe(['t'], {
      onAssigned: () => {
        calls++;
        if (calls === 1) kafka.rebalance('g');
        return noStarts();
      },
      onRevoked: () => undefined,
    });
    await consumer.poll(10, 0);
    expect(consumer.assignment()).toEqual([]);
    await consumer.poll(10, 0);
    expect(consumer.assignment()).toEqual([{ topic: 't', partition: 0 }]);
  });

  it('makes a throwing revocation listener fail every later poll', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    const consumer = kafka.consumer('g');
    await consumer.connect();
    consumer.subscribe(['t'], {
      onAssigned: noStarts,
      onRevoked: () => {
        throw new Error('flush failed');
      },
    });
    await consumer.poll(10, 0);
    kafka.rebalance('g');
    await expect(consumer.poll(10, 0)).rejects.toThrow('flush failed');
    await expect(consumer.poll(10, 0)).rejects.toThrow('flush failed');
  });

  it('interleaves partitions round-robin so one backlog cannot starve the others', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 2);
    const producer = kafka.producer();
    await producer.connect();
    await producer.send([
      ...['a0', 'a1', 'a2'].map((value) => ({ topic: 't', key: 'k', value, partition: 0 })),
      { topic: 't', key: 'k', value: 'b0', partition: 1 },
    ]);
    const consumer = kafka.consumer('g');
    await consumer.connect();
    consumer.assign([
      { topic: 't', partition: 0, offset: '0' },
      { topic: 't', partition: 1, offset: '0' },
    ]);
    const values = (await consumer.poll(3, 0)).map((r) => r.value?.toString());
    expect(values).toEqual(['a0', 'b0', 'a1']);
  });

  it('validates offsets on commit and seek like the real adapter', async () => {
    const kafka = new InMemoryKafka();
    kafka.createTopic('t', 1);
    const consumer = kafka.consumer('g');
    await consumer.connect();
    consumer.assign([{ topic: 't', partition: 0, offset: '0' }]);
    await expect(consumer.commit([{ topic: 't', partition: 0, offset: '1.5' }])).rejects.toThrow(
      RangeError,
    );
    await expect(consumer.seek({ topic: 't', partition: 0, offset: '-2' })).rejects.toThrow(
      RangeError,
    );
  });
});

describe('FakeClock', () => {
  it('moves only when advanced, and only forwards', () => {
    const clock = new FakeClock();
    expect(clock.now()).toBe(0);
    clock.advance(250);
    expect(clock.now()).toBe(250);
    expect(() => {
      clock.advance(-1);
    }).toThrow(RangeError);
    expect(() => {
      clock.advance(Number.NaN);
    }).toThrow(RangeError);
  });
});
