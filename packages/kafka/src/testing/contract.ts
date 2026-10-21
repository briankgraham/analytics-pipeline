// The Kafka port contract. One suite, run against InMemoryKafka (unit) and against a real broker
// through the confluent adapter (integration), so the fake cannot drift from Kafka's behaviour.
import { describe, expect, it } from 'vitest';
import { partitionFor } from '../murmur2.js';
import type {
  ConsumedRecord,
  Consumer,
  Producer,
  RebalanceListener,
  TopicPartition,
  TopicPartitionOffset,
} from '../ports.js';

export interface ContractTarget {
  createTopic(topic: string, partitions: number): Promise<void>;
  producer(): Producer;
  consumer(groupId: string): Consumer;
  /** How long one poll may wait; real brokers need longer (group joins, fetch latency). */
  pollTimeoutMs: number;
}

let unique = 0;
/** A fresh name per test (each target is a fresh broker or fake, so a counter suffices). */
const fresh = (prefix: string): string => `${prefix}-${String(++unique)}`;

/** Kept below the integration testTimeout (60 s at 1 s polls) so waits fail with their own message. */
export const ATTEMPTS = 30;

/** Repeats `step` until it reports done; fails rather than hanging if it never does. */
export async function eventually(what: string, step: () => Promise<boolean>): Promise<void> {
  for (let i = 0; i < ATTEMPTS; i++) if (await step()) return;
  throw new Error(`gave up waiting for ${what}`);
}

/** Polls until `count` records arrived. */
export async function pollUntil(
  consumer: Consumer,
  count: number,
  pollTimeoutMs: number,
): Promise<ConsumedRecord[]> {
  const records: ConsumedRecord[] = [];
  await eventually(`${String(count)} records (got ${String(records.length)})`, async () => {
    records.push(...(await consumer.poll(count - records.length, pollTimeoutMs)));
    return records.length >= count;
  });
  return records;
}

/** Polls until a poll rejects, and returns that rejection. */
export async function pollUntilError(consumer: Consumer, pollTimeoutMs: number): Promise<unknown> {
  let failure: unknown = undefined;
  await eventually('a poll to fail', async () => {
    try {
      await consumer.poll(10, pollTimeoutMs);
      return false;
    } catch (error) {
      failure = error;
      return true;
    }
  });
  return failure;
}

/** A record key or value as text; every record in this suite has both. */
export function text(buffer: Buffer | null): string {
  if (buffer === null) throw new Error('expected a non-null key or value');
  return buffer.toString('utf8');
}

interface RecordingListener extends RebalanceListener {
  assigned: TopicPartition[][];
  revoked: TopicPartition[][];
}

function recordingListener(
  decide: (
    partitions: readonly TopicPartition[],
  ) => Promise<readonly TopicPartitionOffset[]> = () => Promise.resolve([]),
): RecordingListener {
  const assigned: TopicPartition[][] = [];
  const revoked: TopicPartition[][] = [];
  return {
    assigned,
    revoked,
    onAssigned: (partitions) => {
      assigned.push([...partitions]);
      return decide(partitions);
    },
    onRevoked: (partitions) => {
      revoked.push([...partitions]);
    },
  };
}

const sortTps = (tps: readonly TopicPartition[]): TopicPartition[] =>
  [...tps]
    .map(({ topic, partition }) => ({ topic, partition }))
    .sort((a, b) => a.partition - b.partition);

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

export function kafkaContract(name: string, target: () => ContractTarget): void {
  describe(`Kafka port contract: ${name}`, () => {
    async function setup(partitions: number) {
      const t = target();
      const topic = fresh('contract');
      await t.createTopic(topic, partitions);
      const producer = t.producer();
      await producer.connect();
      return { t, topic, producer };
    }

    async function assigned(t: ContractTarget, topic: string, offsets: string[]) {
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      consumer.assign(offsets.map((offset, partition) => ({ topic, partition, offset })));
      return consumer;
    }

    it('round-trips records sent to an explicit partition, with string offsets', async () => {
      const { t, topic, producer } = await setup(3);
      await producer.send(
        ['a', 'b', 'c'].map((v, i) => ({ topic, key: `k${String(i)}`, value: v, partition: 2 })),
      );
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      consumer.assign([{ topic, partition: 2, offset: '0' }]);
      const records = await pollUntil(consumer, 3, t.pollTimeoutMs);
      expect(records.map((r) => [r.partition, r.offset, text(r.key), text(r.value)])).toEqual([
        [2, '0', 'k0', 'a'],
        [2, '1', 'k1', 'b'],
        [2, '2', 'k2', 'c'],
      ]);
      for (const r of records) {
        expect(Number.isSafeInteger(r.timestamp) && r.timestamp > 0).toBe(true);
      }
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('places keyed records on murmur2(UTF-8 key) % partitions, like the Java client', async () => {
      const { t, topic, producer } = await setup(12);
      const keys = [
        ...Array.from({ length: 20 }, (_, i) => `tenant-${String(i % 5)}:service-${String(i)}`),
        'tenant-é:service',
        '租户:服务',
        'tenant:😀',
        'a',
      ];
      await producer.send(keys.map((key) => ({ topic, key, value: key })));
      const consumer = await assigned(
        t,
        topic,
        Array.from({ length: 12 }, () => '0'),
      );
      const records = await pollUntil(consumer, keys.length, t.pollTimeoutMs);
      expect(records.map((r) => text(r.key)).sort()).toEqual([...keys].sort());
      for (const r of records) expect(r.partition).toBe(partitionFor(text(r.key), 12));
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('starts an assignment at the given offset and re-reads after a seek', async () => {
      const { t, topic, producer } = await setup(1);
      await producer.send(
        ['a', 'b', 'c'].map((value) => ({ topic, key: 'k', value, partition: 0 })),
      );
      const consumer = await assigned(t, topic, ['1']);
      const offsets = (rs: ConsumedRecord[]): string[] => rs.map((r) => r.offset);
      expect(offsets(await pollUntil(consumer, 2, t.pollTimeoutMs))).toEqual(['1', '2']);
      await consumer.seek({ topic, partition: 0, offset: '0' });
      expect(offsets(await pollUntil(consumer, 3, t.pollTimeoutMs))).toEqual(['0', '1', '2']);
      expect(sortTps(consumer.assignment())).toEqual([{ topic, partition: 0 }]);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('resets an offset past the end to the earliest record', async () => {
      const { t, topic, producer } = await setup(1);
      await producer.send(
        ['a', 'b', 'c'].map((value) => ({ topic, key: 'k', value, partition: 0 })),
      );
      const consumer = await assigned(t, topic, ['10']);
      const records = await pollUntil(consumer, 3, t.pollTimeoutMs);
      expect(records.map((r) => r.offset)).toEqual(['0', '1', '2']);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('refuses offsets that are not safe non-negative decimal integers', async () => {
      const { t, topic, producer } = await setup(1);
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      for (const offset of ['-1', '9007199254740992']) {
        expect(() => {
          consumer.assign([{ topic, partition: 0, offset }]);
        }).toThrow(RangeError);
      }
      consumer.assign([{ topic, partition: 0, offset: '0' }]);
      // Promise-returning calls reject (never throw), and a bad batch commits nothing.
      const good = { topic, partition: 0, offset: '1' };
      await expect(consumer.seek({ ...good, offset: '-1' })).rejects.toThrow(RangeError);
      await expect(consumer.commit([good, { ...good, offset: '-1' }])).rejects.toThrow(RangeError);
      expect(await consumer.committed([good])).toEqual([]);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('returns nothing from paused partitions until they are resumed', async () => {
      const { t, topic, producer } = await setup(2);
      await producer.send([
        { topic, key: 'k', value: 'p0', partition: 0 },
        { topic, key: 'k', value: 'p1', partition: 1 },
      ]);
      const consumer = await assigned(t, topic, ['0', '0']);
      consumer.pause([{ topic, partition: 0 }]);
      const first = await pollUntil(consumer, 1, t.pollTimeoutMs);
      expect(first.map((r) => text(r.value))).toEqual(['p1']);
      expect(await consumer.poll(10, t.pollTimeoutMs)).toEqual([]);
      consumer.resume([{ topic, partition: 0 }]);
      const resumed = await pollUntil(consumer, 1, t.pollTimeoutMs);
      expect(resumed.map((r) => text(r.value))).toEqual(['p0']);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('waits only for the first record, not for every record up to max', async () => {
      const { t, topic, producer } = await setup(1);
      await producer.send([{ topic, key: 'k', value: 'first', partition: 0 }]);
      const consumer = await assigned(t, topic, ['0']);
      // Records keep trickling in; a per-record timeout would stretch one poll to ~max * interval.
      const sends: Promise<void>[] = [];
      const trickle = setInterval(() => {
        const value = `more-${String(sends.length + 1)}`;
        sends.push(producer.send([{ topic, key: 'k', value, partition: 0 }]));
      }, 100);
      try {
        await pollUntil(consumer, 1, t.pollTimeoutMs);
        const started = Date.now();
        await consumer.poll(1_000, t.pollTimeoutMs);
        expect(Date.now() - started).toBeLessThan(t.pollTimeoutMs + 1_000);
      } finally {
        clearInterval(trickle);
        // Every send is awaited before disconnect, so none can fail unobserved.
        await Promise.all(sends);
      }
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('rejects a poll for no records or with a negative timeout, and keeps polling after', async () => {
      const { t, topic, producer } = await setup(1);
      await producer.send([{ topic, key: 'k', value: 'v', partition: 0 }]);
      const consumer = await assigned(t, topic, ['0']);
      for (const [max, timeoutMs] of [
        [0, t.pollTimeoutMs],
        [1.5, t.pollTimeoutMs],
        [Number.NaN, t.pollTimeoutMs],
        [1, -1],
        [1, Number.POSITIVE_INFINITY],
      ] as const) {
        await expect(consumer.poll(max, timeoutMs)).rejects.toThrow(RangeError);
      }
      const records = await pollUntil(consumer, 1, t.pollTimeoutMs);
      expect(records.map((r) => r.offset)).toEqual(['0']);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('rejects a second commit while one is in flight', async () => {
      const { t, topic, producer } = await setup(1);
      const consumer = await assigned(t, topic, ['0']);
      const at = [{ topic, partition: 0, offset: '0' }];
      const first = consumer.commit(at);
      await expect(consumer.commit(at)).rejects.toThrow('a commit is already in flight');
      await first;
      await consumer.commit(at);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('rejects a second poll while one is in flight', async () => {
      const { t, topic, producer } = await setup(1);
      const consumer = await assigned(t, topic, ['0']);
      const first = consumer.poll(10, t.pollTimeoutMs);
      await expect(consumer.poll(10, t.pollTimeoutMs)).rejects.toThrow(
        'a poll is already in flight',
      );
      await first;
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('reports watermarks and finds offsets by timestamp', async () => {
      const { t, topic, producer } = await setup(1);
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      expect(await consumer.watermarks({ topic, partition: 0 })).toEqual({ low: '0', high: '0' });
      await producer.send(
        ['a', 'b', 'c'].map((value) => ({ topic, key: 'k', value, partition: 0 })),
      );
      expect(await consumer.watermarks({ topic, partition: 0 })).toEqual({ low: '0', high: '3' });
      const tp = [{ topic, partition: 0 }];
      expect(await consumer.offsetsForTimes(tp, 0)).toEqual([{ topic, partition: 0, offset: '0' }]);
      expect(await consumer.offsetsForTimes(tp, 8.64e15)).toEqual([
        { topic, partition: 0, offset: '3' },
      ]);
      expect(await consumer.offsetsForTimes([], 0)).toEqual([]);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('joins a group from earliest, and a later member resumes at the committed offsets', async () => {
      const { t, topic, producer } = await setup(2);
      await producer.send([
        { topic, key: 'k', value: 'a', partition: 0 },
        { topic, key: 'k', value: 'b', partition: 1 },
      ]);
      const group = fresh('group');
      const first = t.consumer(group);
      const listener = recordingListener();
      await first.connect();
      first.subscribe([topic], listener);
      const records = await pollUntil(first, 2, t.pollTimeoutMs);
      expect(records.map((r) => text(r.value)).sort()).toEqual(['a', 'b']);
      const both = [
        { topic, partition: 0 },
        { topic, partition: 1 },
      ];
      expect(listener.assigned.map(sortTps)).toEqual([both]);
      await first.commit(
        records.map((r) => ({
          topic,
          partition: r.partition,
          offset: String(BigInt(r.offset) + 1n),
        })),
      );
      const committed = [...(await first.committed(both))].sort(
        (a, b) => a.partition - b.partition,
      );
      expect(committed).toEqual([
        { topic, partition: 0, offset: '1' },
        { topic, partition: 1, offset: '1' },
      ]);
      await first.disconnect();
      expect(listener.revoked.map(sortTps)).toEqual([both]);

      await producer.send([{ topic, key: 'k', value: 'c', partition: 0 }]);
      const second = t.consumer(group);
      await second.connect();
      second.subscribe([topic], recordingListener());
      const resumed = await pollUntil(second, 1, t.pollTimeoutMs);
      expect(resumed.map((r) => text(r.value))).toEqual(['c']);
      await second.disconnect();
      await producer.disconnect();
    });

    it('starts each partition where the async assignment listener decides, never earlier', async () => {
      const { t, topic, producer } = await setup(1);
      await producer.send(
        ['a', 'b', 'c', 'd'].map((value) => ({ topic, key: 'k', value, partition: 0 })),
      );
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      // Like the aggregator: load a checkpoint (here: wait), then start at its nextOffset.
      consumer.subscribe(
        [topic],
        recordingListener(async (partitions) => {
          await delay(200);
          return partitions.map((tp) => ({ ...tp, offset: '2' }));
        }),
      );
      const records = await pollUntil(consumer, 2, t.pollTimeoutMs);
      expect(records.map((r) => r.offset)).toEqual(['2', '3']);
      expect(await consumer.poll(10, t.pollTimeoutMs)).toEqual([]);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('reports a failed assignment listener on the next poll', async () => {
      const { t, topic, producer } = await setup(1);
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      consumer.subscribe(
        [topic],
        recordingListener(() => Promise.reject(new Error('checkpoint load failed'))),
      );
      const failure = await pollUntilError(consumer, t.pollTimeoutMs);
      expect(String(failure)).toContain('checkpoint load failed');
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('keeps a partition paused during its assignment listener after the listener resolves', async () => {
      const { t, topic, producer } = await setup(1);
      await producer.send([{ topic, key: 'k', value: 'v', partition: 0 }]);
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      const decided = Promise.withResolvers<readonly TopicPartitionOffset[]>();
      let started = false;
      consumer.subscribe(
        [topic],
        recordingListener(() => {
          started = true;
          return decided.promise;
        }),
      );
      // Only the poll loop drives the consumer (one poll in flight at a time).
      const records: ConsumedRecord[] = [];
      let polls = 0;
      const stop = new AbortController();
      const loop = (async () => {
        while (!stop.signal.aborted) {
          records.push(...(await consumer.poll(10, t.pollTimeoutMs)));
          polls++;
        }
      })();
      await eventually('the listener to start', async () => {
        await delay(250);
        return started;
      });
      // Backpressure: pause everything owned, including partitions still being decided.
      consumer.pause(consumer.assignment());
      decided.resolve([]);
      const after = polls;
      await eventually('a few polls after the listener resolved', async () => {
        await delay(250);
        return polls >= after + 3;
      });
      expect(records).toEqual([]);
      consumer.resume(consumer.assignment());
      await eventually('the record after resume', async () => {
        await delay(250);
        return records.length > 0;
      });
      stop.abort();
      await loop;
      expect(records.map((r) => r.offset)).toEqual(['0']);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('returns nothing before the assignment listener resolves, even if resumed meanwhile', async () => {
      const { t, topic, producer } = await setup(1);
      await producer.send(['a', 'b'].map((value) => ({ topic, key: 'k', value, partition: 0 })));
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      const decided = Promise.withResolvers<readonly TopicPartitionOffset[]>();
      let started = false;
      consumer.subscribe(
        [topic],
        recordingListener(() => {
          started = true;
          return decided.promise;
        }),
      );
      // The fake's poll waits for the listener, so only a background loop can drive both.
      const records: ConsumedRecord[] = [];
      const stop = new AbortController();
      const loop = (async () => {
        while (!stop.signal.aborted) records.push(...(await consumer.poll(10, t.pollTimeoutMs)));
      })();
      await eventually('the listener to start', async () => {
        await delay(250);
        return started;
      });
      // Backpressure that clears while the checkpoint is still loading.
      consumer.pause(consumer.assignment());
      consumer.resume(consumer.assignment());
      // Absence is only observable over a window: three polls' worth (the fake's poll waits for the
      // listener instead, so its window is empty).
      await delay(3 * t.pollTimeoutMs);
      expect(records).toEqual([]);
      decided.resolve([{ topic, partition: 0, offset: '1' }]);
      await eventually('the record at the decided offset', async () => {
        await delay(250);
        return records.length > 0;
      });
      stop.abort();
      await loop;
      expect(records.map((r) => r.offset)).toEqual(['1']);
      await consumer.disconnect();
      await producer.disconnect();
    });

    it('makes an invalid start offset from the assignment listener fail every later poll', async () => {
      const { t, topic, producer } = await setup(1);
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      consumer.subscribe(
        [topic],
        recordingListener((partitions) =>
          Promise.resolve(partitions.map((tp) => ({ ...tp, offset: '-1' }))),
        ),
      );
      const failure = await pollUntilError(consumer, t.pollTimeoutMs);
      expect(String(failure)).toContain('"-1"');
      await expect(consumer.poll(10, t.pollTimeoutMs)).rejects.toThrow('"-1"');
      await consumer.disconnect();
      await producer.disconnect();
    });

    // A superseded listener may resolve late, or honour its signal by rejecting; both are ignored.
    it.each(['resolves', 'rejects'] as const)(
      'revokes partitions whose assignment listener is still running, and ignores what it %s with',
      async (outcome) => {
        const { t, topic, producer } = await setup(2);
        await producer.send(
          [0, 1].map((partition) => ({ topic, key: 'k', value: 'v', partition })),
        );
        const group = fresh('group');
        const [first, second] = [t.consumer(group), t.consumer(group)];
        await first.connect();
        const events: string[] = [];
        const names = (tps: readonly TopicPartition[]): string =>
          sortTps(tps)
            .map((tp) => String(tp.partition))
            .join(',');
        let slowLoad: AbortSignal | undefined;
        first.subscribe([topic], {
          onAssigned: async (partitions, signal) => {
            events.push(`assigned ${names(partitions)}`);
            if (slowLoad !== undefined) return [];
            // A checkpoint load that outlasts the rebalance below; its start offsets come too late.
            slowLoad = signal;
            await new Promise((resolve) => {
              signal.addEventListener('abort', resolve);
            });
            if (outcome === 'rejects') signal.throwIfAborted();
            return partitions.map((tp) => ({ ...tp, offset: '1' }));
          },
          onRevoked: (partitions) => {
            events.push(`revoked ${names(partitions)}`);
          },
        });
        // Only the poll loop drives the first member (one poll in flight at a time).
        const records: ConsumedRecord[] = [];
        const stop = new AbortController();
        const loop = (async () => {
          while (!stop.signal.aborted) records.push(...(await first.poll(10, t.pollTimeoutMs)));
        })();
        await eventually('the first member to start loading', async () => {
          await delay(250);
          return slowLoad !== undefined;
        });
        // Owned (paused) while the listener decides.
        expect(names(first.assignment())).toBe('0,1');
        await second.connect();
        second.subscribe([topic], recordingListener());
        await eventually('the first member to rejoin and read', async () => {
          await second.poll(10, t.pollTimeoutMs);
          return records.length > 0;
        });
        stop.abort();
        await loop;
        expect(slowLoad?.aborted).toBe(true);
        const [owned] = first.assignment();
        expect(events).toEqual([
          'assigned 0,1',
          'revoked 0,1',
          `assigned ${String(owned?.partition)}`,
        ]);
        // The superseded listener's offset '1' was not applied: the partition starts at earliest.
        expect(records.map((r) => r.offset)).toEqual(['0']);
        await first.disconnect();
        await second.disconnect();
        await producer.disconnect();
      },
    );

    it('splits partitions between two members, revoking the first member when the second joins', async () => {
      const { t, topic, producer } = await setup(2);
      const group = fresh('group');
      const [first, second] = [t.consumer(group), t.consumer(group)];
      const firstListener = recordingListener();
      await first.connect();
      first.subscribe([topic], firstListener);
      await eventually('the first member to own both partitions', async () => {
        await first.poll(10, t.pollTimeoutMs);
        return first.assignment().length === 2;
      });
      await second.connect();
      second.subscribe([topic], recordingListener());
      await eventually('each member to own one partition', async () => {
        await first.poll(10, t.pollTimeoutMs);
        await second.poll(10, t.pollTimeoutMs);
        return first.assignment().length === 1 && second.assignment().length === 1;
      });
      expect(firstListener.revoked.length).toBeGreaterThanOrEqual(1);
      const owners = [...first.assignment(), ...second.assignment()].map((tp) => tp.partition);
      expect(owners.sort()).toEqual([0, 1]);

      await producer.send([
        { topic, key: 'k', value: 'p0', partition: 0 },
        { topic, key: 'k', value: 'p1', partition: 1 },
      ]);
      const seen = new Map<string, number>();
      await eventually('both records', async () => {
        for (const member of [first, second]) {
          for (const r of await member.poll(10, t.pollTimeoutMs)) {
            expect(member.assignment().map((tp) => tp.partition)).toContain(r.partition);
            seen.set(text(r.value), r.partition);
          }
        }
        return seen.size === 2;
      });
      expect([...seen].sort()).toEqual([
        ['p0', 0],
        ['p1', 1],
      ]);
      await first.disconnect();
      await second.disconnect();
      await producer.disconnect();
    });

    it('makes a throwing revocation listener fail every later poll', async () => {
      const { t, topic, producer } = await setup(2);
      const group = fresh('group');
      const [first, second] = [t.consumer(group), t.consumer(group)];
      await first.connect();
      first.subscribe([topic], {
        onAssigned: () => Promise.resolve([]),
        onRevoked: () => {
          throw new Error('flush before revoke failed');
        },
      });
      await eventually('the first member to own its partitions', async () => {
        await first.poll(10, t.pollTimeoutMs);
        return first.assignment().length === 2;
      });
      await second.connect();
      second.subscribe([topic], recordingListener());
      let failure: unknown = undefined;
      await eventually('the revocation failure to surface', async () => {
        await second.poll(10, t.pollTimeoutMs);
        try {
          await first.poll(10, t.pollTimeoutMs);
          return false;
        } catch (error) {
          failure = error;
          return true;
        }
      });
      expect(String(failure)).toContain('flush before revoke failed');
      await expect(first.poll(10, t.pollTimeoutMs)).rejects.toThrow('flush before revoke failed');
      await first.disconnect();
      await second.disconnect();
      await producer.disconnect();
    });

    it('settles concurrent disconnects, as racing shutdown paths need', async () => {
      const { t, topic, producer } = await setup(1);
      const consumer = await assigned(t, topic, ['0']);
      await Promise.all([consumer.disconnect(), consumer.disconnect()]);
      await Promise.all([producer.disconnect(), producer.disconnect()]);
    });

    it('fails every call after disconnect', async () => {
      const { t, topic, producer } = await setup(1);
      const consumer = await assigned(t, topic, ['0']);
      await consumer.disconnect();
      await expect(consumer.poll(1, t.pollTimeoutMs)).rejects.toThrow();
      expect(() => {
        consumer.pause([{ topic, partition: 0 }]);
      }).toThrow();
      await producer.disconnect();
    });

    it('reports no committed offset for a partition the group never committed', async () => {
      const { t, topic, producer } = await setup(1);
      const consumer = t.consumer(fresh('group'));
      await consumer.connect();
      expect(await consumer.committed([{ topic, partition: 0 }])).toEqual([]);
      await consumer.disconnect();
      await producer.disconnect();
    });
  });
}
