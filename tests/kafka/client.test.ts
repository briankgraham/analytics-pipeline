import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KafkaClient, type HandleBatch } from '../../kafka/client.ts';

// Stands in for kafkajs, keeping hold of the function the code gives it for each batch. Real Kafka was
// checked by hand: asking about a missing topic throws UNKNOWN_TOPIC_OR_PARTITION and doesn't create it.
const { admin, consumer, producer, ProtocolError, kafkaMade } = vi.hoisted(() => ({
  admin: { connect: vi.fn(), disconnect: vi.fn(), fetchTopicMetadata: vi.fn() },
  consumer: {
    events: { CRASH: 'crash', GROUP_JOIN: 'group-join' },
    on: vi.fn(),
    connect: vi.fn(),
    subscribe: vi.fn(),
    run: vi.fn(),
    disconnect: vi.fn(),
  },
  producer: { connect: vi.fn(), disconnect: vi.fn(), send: vi.fn() },
  ProtocolError: class extends Error {
    type = 'UNKNOWN_TOPIC_OR_PARTITION';
  },
  kafkaMade: vi.fn(),
}));

vi.mock('kafkajs', () => ({
  Partitioners: {},
  Kafka: class {
    constructor() {
      kafkaMade();
    }
    admin() {
      return admin;
    }
    consumer() {
      return consumer;
    }
    producer() {
      return producer;
    }
  },
}));

const resume = vi.fn();
const pause = vi.fn(() => resume);
const resolveOffset = vi.fn();
const heartbeat = vi.fn(async () => {});
const isRunning = vi.fn(() => true);
const isStale = vi.fn(() => false);
const onPartitionsAssigned = vi.fn();
const onGiveUp = vi.fn();

let client: KafkaClient | undefined;

function getClient() {
  client = KafkaClient.getInstance(['127.0.0.1:9092'], 'worker');
  return client;
}

// Starts reading with the given handler, then delivers one batch the way kafkajs would.
async function deliver(handleBatch: HandleBatch) {
  const kafka = getClient();
  await kafka.init();
  await kafka.startReading({
    topic: 'logs',
    group: 'worker',
    pauseMs: 120_000,
    handleBatch,
    onPartitionsAssigned,
    onGiveUp,
  });
  const { eachBatch } = consumer.run.mock.calls[0]?.[0];
  await eachBatch({
    batch: {
      partition: 2,
      messages: [
        { value: Buffer.from('hello'), offset: '7', timestamp: '1000' },
        { value: null, offset: '8', timestamp: '2000' },
      ],
    },
    resolveOffset,
    heartbeat,
    pause,
    isRunning,
    isStale,
  });
  return kafka;
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.useFakeTimers();
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  isRunning.mockReturnValue(true);
  isStale.mockReturnValue(false);
});

afterEach(async () => {
  // A test may have made closing fail on purpose, and close gives the same answer every time.
  await client?.close().catch(() => {});
  client = undefined;
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('reading', () => {
  it('leaves marking messages done to the handler', async () => {
    await deliver(async () => 'finished');
    expect(consumer.run.mock.calls[0]?.[0].eachBatchAutoResolve).toBe(false);
    expect(resolveOffset).not.toHaveBeenCalled();
  });

  it('hands over the messages in plain form', async () => {
    const handleBatch = vi.fn<HandleBatch>(async () => 'finished');
    await deliver(handleBatch);
    const batch = handleBatch.mock.calls[0]?.[0];
    expect(batch?.partition).toBe(2);
    expect(batch?.messages).toEqual([
      { id: 'logs-2-7', offset: '7', text: 'hello', timestampMs: 1000 },
      { id: 'logs-2-8', offset: '8', text: null, timestampMs: 2000 },
    ]);
  });

  it('connects markDone and keepAlive to kafkajs', async () => {
    await deliver(async (batch) => {
      batch.markDone('7');
      await batch.keepAlive();
      return 'finished';
    });
    expect(resolveOffset).toHaveBeenCalledWith('7');
    expect(heartbeat).toHaveBeenCalledOnce();
  });

  it.each([
    [true, false, false],
    [false, false, true],
    [true, true, true],
  ])('with isRunning %s and isStale %s, shouldStop is %s', async (running, stale, stop) => {
    isRunning.mockReturnValue(running);
    isStale.mockReturnValue(stale);
    await deliver(async (batch) => {
      expect(batch.shouldStop()).toBe(stop);
      return 'finished';
    });
  });

  it('does not pause after a batch that finished', async () => {
    await deliver(async () => 'finished');
    expect(pause).not.toHaveBeenCalled();
  });

  it('pauses the partition when asked, and resumes after pauseMs', async () => {
    await deliver(async () => 'pause');
    expect(pause).toHaveBeenCalledOnce();

    vi.advanceTimersByTime(119_999);
    expect(resume).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(resume).toHaveBeenCalledOnce();
  });

  it('does not resume after the client has closed', async () => {
    const kafka = await deliver(async () => 'pause');
    await kafka.close();
    vi.advanceTimersByTime(120_000);
    expect(resume).not.toHaveBeenCalled();
    expect(consumer.disconnect).toHaveBeenCalledOnce();
  });

  it('does not pause when the client closed while the handler was working', async () => {
    await deliver(async () => {
      isRunning.mockReturnValue(false);
      return 'pause';
    });
    expect(pause).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('says which partitions this worker reads after joining the group', async () => {
    await deliver(async () => 'finished');
    const [, onJoin] = consumer.on.mock.calls.find(([event]) => event === 'group-join') ?? [];
    onJoin({ payload: { memberAssignment: { logs: [0, 2] } } });
    expect(onPartitionsAssigned).toHaveBeenLastCalledWith([0, 2]);
    onJoin({ payload: { memberAssignment: {} } });
    expect(onPartitionsAssigned).toHaveBeenLastCalledWith([]);
  });

  it('passes on an unexpected error, because kafkajs uses it to rejoin the group', async () => {
    await expect(
      deliver(async () => {
        throw new Error('the group is rebalancing');
      }),
    ).rejects.toThrow('the group is rebalancing');
  });

  it('tells the caller only when kafkajs stops for good', async () => {
    await deliver(async () => 'finished');
    const [, onCrash] = consumer.on.mock.calls.find(([event]) => event === 'crash') ?? [];
    const error = new Error('gave up');

    onCrash({ payload: { restart: true, error } });
    expect(onGiveUp).not.toHaveBeenCalled();
    onCrash({ payload: { restart: false, error } });
    expect(onGiveUp).toHaveBeenCalledWith(error);
  });

  it('has only one reader', async () => {
    const kafka = await deliver(async () => 'finished');
    const options = {
      topic: 'logs',
      group: 'worker',
      pauseMs: 1,
      handleBatch: async () => 'finished' as const,
      onPartitionsAssigned,
      onGiveUp,
    };
    await expect(kafka.startReading(options)).rejects.toThrow('already reading');
  });
});

describe('one instance, with init and close', () => {
  it('has only one instance, and refuses different settings', () => {
    expect(getClient()).toBe(getClient());
    expect(() => KafkaClient.getInstance(['elsewhere:9092'], 'worker')).toThrow('different');
    expect(() => KafkaClient.getInstance(['127.0.0.1:9092'], 'api')).toThrow('different');
  });

  it('connects once however many times init is called, even at the same time', async () => {
    const kafka = getClient();
    expect(producer.connect).not.toHaveBeenCalled();
    await Promise.all([kafka.init(), kafka.init()]);
    await kafka.init();
    expect(kafkaMade).toHaveBeenCalledOnce();
    expect(producer.connect).toHaveBeenCalledOnce();
  });

  it('tries again on the next init after a failure', async () => {
    producer.connect.mockRejectedValueOnce(new Error('Kafka is down'));
    const kafka = getClient();
    await expect(kafka.init()).rejects.toThrow('Kafka is down');
    await kafka.init();
    expect(kafkaMade).toHaveBeenCalledOnce();
    expect(producer.connect).toHaveBeenCalledTimes(2);
  });

  it('refuses calls before init and after close', async () => {
    const kafka = getClient();
    await expect(kafka.sendMessages('logs', ['a'])).rejects.toThrow('call init() first');
    await kafka.init();
    await kafka.close();
    await expect(kafka.sendMessages('logs', ['a'])).rejects.toThrow('is closed');
    await expect(kafka.init()).rejects.toThrow('is closed');
  });

  it('stops reading before it closes the connection for sending', async () => {
    const kafka = await deliver(async () => 'finished');
    const order: string[] = [];
    consumer.disconnect.mockImplementationOnce(async () => order.push('reading'));
    producer.disconnect.mockImplementationOnce(async () => order.push('sending'));

    await kafka.close();
    await kafka.close();
    expect(order).toEqual(['reading', 'sending']);
  });

  it('can still send while reading stops, for a batch still being handled', async () => {
    const kafka = await deliver(async () => 'finished');
    let finishStopping: () => void = () => {};
    consumer.disconnect.mockImplementationOnce(
      () => new Promise<void>((resolve) => (finishStopping = resolve)),
    );

    const closing = kafka.close();
    await vi.advanceTimersByTimeAsync(0);
    expect(consumer.disconnect).toHaveBeenCalled();
    await kafka.sendMessages('failed', ['a']);
    expect(producer.send).toHaveBeenCalledOnce();
    expect(producer.disconnect).not.toHaveBeenCalled();

    finishStopping();
    await closing;
    expect(producer.disconnect).toHaveBeenCalledOnce();
    await expect(kafka.sendMessages('failed', ['b'])).rejects.toThrow('is closed');
  });

  it('refuses new work as soon as close is called', async () => {
    const kafka = await deliver(async () => 'finished');
    const closing = kafka.close();
    await expect(kafka.init()).rejects.toThrow('is closed');
    await expect(kafka.topicExists('failed')).rejects.toThrow('is closed');
    await closing;
  });

  it('makes a second close wait for the first', async () => {
    const kafka = await deliver(async () => 'finished');
    let finishStopping: () => void = () => {};
    consumer.disconnect.mockImplementationOnce(
      () => new Promise<void>((resolve) => (finishStopping = resolve)),
    );
    const first = kafka.close();
    const second = kafka.close();
    let secondDone = false;
    void second.then(() => (secondDone = true));
    await vi.advanceTimersByTimeAsync(0);
    expect(secondDone).toBe(false);

    finishStopping();
    await first;
    await second;
    expect(consumer.disconnect).toHaveBeenCalledOnce();
  });

  it('can try reading again after a failed start, and cleans up the failed try', async () => {
    const kafka = getClient();
    await kafka.init();
    consumer.subscribe.mockRejectedValueOnce(new Error('no such topic'));
    consumer.disconnect.mockResolvedValueOnce(undefined);
    const options = {
      topic: 'logs',
      group: 'worker',
      pauseMs: 1,
      handleBatch: async () => 'finished' as const,
      onPartitionsAssigned,
      onGiveUp,
    };
    await expect(kafka.startReading(options)).rejects.toThrow('no such topic');
    expect(consumer.disconnect).toHaveBeenCalledOnce();

    await kafka.startReading(options);
    expect(consumer.run).toHaveBeenCalledOnce();
  });

  it('still closes the connection for sending when reading fails to close', async () => {
    const kafka = await deliver(async () => 'finished');
    consumer.disconnect.mockRejectedValueOnce(new Error('stuck'));
    await expect(kafka.close()).rejects.toThrow('stuck');
    expect(producer.disconnect).toHaveBeenCalledOnce();
  });

  it('gives a new instance after close', async () => {
    const first = getClient();
    await first.close();
    expect(getClient()).not.toBe(first);
  });
});

describe('sendMessages', () => {
  it('sends each text as one message', async () => {
    const kafka = getClient();
    await kafka.init();
    await kafka.sendMessages('logs', ['a', 'b']);
    expect(producer.send).toHaveBeenCalledWith({
      topic: 'logs',
      messages: [{ value: 'a' }, { value: 'b' }],
    });
  });
});

describe('topicExists', () => {
  it('answers yes when Kafka knows the topic', async () => {
    const kafka = getClient();
    await kafka.init();
    expect(await kafka.topicExists('failed')).toBe(true);
    expect(admin.disconnect).toHaveBeenCalledOnce();
  });

  it('answers no when Kafka does not', async () => {
    admin.fetchTopicMetadata.mockRejectedValueOnce(new ProtocolError('unknown'));
    const kafka = getClient();
    await kafka.init();
    expect(await kafka.topicExists('failed')).toBe(false);
    expect(admin.disconnect).toHaveBeenCalledOnce();
  });

  it('passes on any other problem as it is', async () => {
    admin.fetchTopicMetadata.mockRejectedValueOnce(new Error('network down'));
    const kafka = getClient();
    await kafka.init();
    await expect(kafka.topicExists('failed')).rejects.toThrow('network down');
    expect(admin.disconnect).toHaveBeenCalledOnce();
  });
});
