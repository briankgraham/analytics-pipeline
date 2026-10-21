import { describe, expect, it, vi } from 'vitest';
import {
  AssignmentGate,
  consumerErrorHandler,
  DELIVERY_TIMEOUT_MS,
  fromNativeOffset,
  fromPartitionResults,
  kafkaJsLogger,
  NO_REBALANCE_LISTENER,
  withStartOffsets,
  KafkaError,
  onRebalance,
  producerSettings,
  settle,
  settleVoid,
  toConsumedRecord,
  toKafkaError,
  toNativeOffset,
  toTopicPartitions,
  withTimeout,
} from './native.js';

describe('toKafkaError', () => {
  it('keeps the native message and code, with the original as cause', () => {
    const native = { message: 'Broker: Unknown topic', code: 3, errno: 3, origin: 'kafka' };
    const error = toKafkaError(native, 'commit');
    expect(error).toBeInstanceOf(KafkaError);
    expect(error.message).toBe('commit: Broker: Unknown topic');
    expect(error.code).toBe(3);
    expect(error.cause).toBe(native);
    expect(error.name).toBe('KafkaError');
  });

  it('copes with values that are not error-shaped', () => {
    expect(toKafkaError('boom', 'poll')).toMatchObject({ message: 'poll: boom', code: undefined });
    expect(toKafkaError(null, 'poll').message).toBe('poll: null');
    expect(toKafkaError({ message: 42, code: 'x' }, 'poll')).toMatchObject({ code: undefined });
  });
});

describe('settle', () => {
  it('resolves with the value when there is no error', () => {
    const resolve = vi.fn();
    const reject = vi.fn();
    settle('x', resolve, reject)(null, 7);
    expect(resolve).toHaveBeenCalledWith(7);
    expect(reject).not.toHaveBeenCalled();
  });

  it('rejects when mapping the value throws, so nothing escapes into native code', () => {
    const reject = vi.fn();
    settle(
      'committed',
      () => {
        throw new RangeError('bad offset');
      },
      reject,
    )(null, 7);
    expect(reject).toHaveBeenCalledWith(
      expect.objectContaining({ message: 'committed: bad offset' }),
    );
  });

  it('rejects with a KafkaError when there is one', () => {
    const resolve = vi.fn();
    const reject = vi.fn();
    settle('seek', resolve, reject)({ message: 'Erroneous state', code: -172 }, undefined);
    expect(resolve).not.toHaveBeenCalled();
    expect(reject).toHaveBeenCalledWith(
      expect.objectContaining({ message: 'seek: Erroneous state', code: -172 }),
    );
  });
});

describe('settleVoid', () => {
  it('resolves without a value, or rejects with a KafkaError', () => {
    const resolve = vi.fn();
    const reject = vi.fn();
    settleVoid('seek', resolve, reject)(undefined);
    expect(resolve).toHaveBeenCalledOnce();
    settleVoid('seek', resolve, reject)({ message: 'Local: Erroneous state', code: -172 });
    expect(reject).toHaveBeenCalledWith(expect.objectContaining({ code: -172 }));
  });
});

describe('toNativeOffset', () => {
  it('converts safe decimal offsets', () => {
    expect(toNativeOffset('0')).toBe(0);
    expect(toNativeOffset('9007199254740991')).toBe(Number.MAX_SAFE_INTEGER);
  });

  it.each(['9007199254740992', '-1', '1.5', '01', '', 'abc'])('refuses %j', (offset) => {
    expect(() => toNativeOffset(offset)).toThrow(RangeError);
  });
});

describe('fromNativeOffset', () => {
  it('converts safe non-negative offsets', () => {
    expect(fromNativeOffset(0)).toBe('0');
    expect(fromNativeOffset(Number.MAX_SAFE_INTEGER)).toBe('9007199254740991');
  });

  it.each([2 ** 53, -1, 1.5, Number.NaN])('refuses %d', (offset) => {
    expect(() => fromNativeOffset(offset)).toThrow(RangeError);
  });
});

describe('fromPartitionResults', () => {
  it('maps offsets, with negative sentinels and missing offsets as none', () => {
    expect(
      fromPartitionResults(
        [
          { topic: 't', partition: 0, offset: 5 },
          { topic: 't', partition: 1, offset: -1001 },
          { topic: 't', partition: 2 },
        ],
        'committed',
      ),
    ).toEqual([
      { topic: 't', partition: 0, offset: '5' },
      { topic: 't', partition: 1, offset: undefined },
      { topic: 't', partition: 2, offset: undefined },
    ]);
  });

  it('fails when any partition failed, instead of reporting it as having no offset', () => {
    expect(() =>
      fromPartitionResults(
        [{ topic: 't', partition: 0, offset: 5 }, new Error('Coordinator not available')],
        'committed',
      ),
    ).toThrow('committed: Coordinator not available');
  });
});

describe('toConsumedRecord', () => {
  const base = { topic: 't', partition: 3, offset: 41, value: Buffer.from('v') };

  it('maps a native message, stringifying the offset', () => {
    expect(toConsumedRecord({ ...base, key: Buffer.from('k'), timestamp: 1_700 })).toEqual({
      topic: 't',
      partition: 3,
      offset: '41',
      key: Buffer.from('k'),
      value: Buffer.from('v'),
      timestamp: 1_700,
    });
  });

  it('normalises string, null, and missing keys and a missing timestamp', () => {
    expect(toConsumedRecord({ ...base, key: 'k' }).key).toEqual(Buffer.from('k'));
    expect(toConsumedRecord({ ...base, key: null }).key).toBeNull();
    expect(toConsumedRecord(base)).toMatchObject({ key: null, timestamp: 0 });
  });
});

describe('toTopicPartitions', () => {
  it('drops everything but topic and partition', () => {
    const assignment = [{ topic: 't', partition: 1, offset: 5 }];
    expect(toTopicPartitions(assignment)).toEqual([{ topic: 't', partition: 1 }]);
  });
});

describe('onRebalance', () => {
  const codes = { assign: -175, revoke: -174 };

  it('assigns, revokes, and refuses anything else', () => {
    const actions = { assign: vi.fn(), revoke: vi.fn() };
    onRebalance(-175, codes, actions);
    expect(actions.assign).toHaveBeenCalledOnce();
    onRebalance(-174, codes, actions);
    expect(actions.revoke).toHaveBeenCalledOnce();
    expect(() => {
      onRebalance(-1, codes, actions);
    }).toThrow('unexpected rebalance code -1');
  });
});

describe('producerSettings', () => {
  it('always sets the Java-compatible partitioner and a delivery bound, linger.ms only when asked', () => {
    expect(producerSettings({})).toEqual({
      partitioner: 'murmur2_random',
      'message.timeout.ms': DELIVERY_TIMEOUT_MS,
    });
    expect(producerSettings({ lingerMs: 0, deliveryTimeoutMs: 5_000 })).toEqual({
      partitioner: 'murmur2_random',
      'message.timeout.ms': 5_000,
      'linger.ms': 0,
    });
  });
});

describe('withTimeout', () => {
  it('passes a settled result through', async () => {
    await expect(withTimeout(Promise.resolve(3), 1_000, 'x')).resolves.toBe(3);
    await expect(withTimeout(Promise.reject(new Error('no')), 1_000, 'x')).rejects.toThrow('no');
  });

  it('rejects with a KafkaError once the deadline passes', async () => {
    vi.useFakeTimers();
    try {
      const pending = withTimeout(new Promise<never>(() => undefined), 50, 'commit');
      vi.advanceTimersByTime(50);
      await expect(pending).rejects.toThrow('commit timed out after 50 ms');
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('AssignmentGate', () => {
  it('applies a decision only for the latest assignment', () => {
    const gate = new AssignmentGate();
    const first = gate.next();
    const applied: string[] = [];
    gate.ifCurrent(first, () => applied.push('first'));
    const second = gate.next();
    gate.ifCurrent(first, () => applied.push('stale'));
    gate.ifCurrent(second, () => applied.push('second'));
    expect(applied).toEqual(['first', 'second']);
    expect([first.aborted, second.aborted]).toEqual([true, false]);
  });
});

describe('withStartOffsets', () => {
  it('sets start offsets where decided and leaves the rest to committed/earliest', () => {
    const assignment = [
      { topic: 't', partition: 0 },
      { topic: 't', partition: 1 },
    ];
    expect(withStartOffsets(assignment, [{ topic: 't', partition: 1, offset: '42' }])).toEqual([
      { topic: 't', partition: 0 },
      { topic: 't', partition: 1, offset: 42 },
    ]);
  });

  it('refuses an unsafe start offset', () => {
    expect(() =>
      withStartOffsets(
        [{ topic: 't', partition: 0 }],
        [{ topic: 't', partition: 0, offset: '-1' }],
      ),
    ).toThrow(RangeError);
  });
});

describe('log bridges', () => {
  function capture() {
    const lines: { level: string; fields: Record<string, unknown>; message: string }[] = [];
    return {
      lines,
      log: {
        warn: (fields: Record<string, unknown>, message: string) =>
          lines.push({ level: 'warn', fields, message }),
        error: (fields: Record<string, unknown>, message: string) =>
          lines.push({ level: 'error', fields, message }),
      },
    };
  }

  it('kafkaJsLogger keeps warnings and errors with their namespace, and drops info and debug', () => {
    const { lines, log } = capture();
    const logger = kafkaJsLogger(log);
    logger.info('connected');
    logger.debug('fetch');
    logger.warn('retrying', { attempt: 2 });
    logger.namespace('producer').error('send failed', { topic: 't' });
    logger.setLogLevel();
    expect(lines).toEqual([
      { level: 'warn', fields: { kafka: { namespace: 'kafka', attempt: 2 } }, message: 'retrying' },
      {
        level: 'error',
        fields: { kafka: { namespace: 'kafka.producer', topic: 't' } },
        message: 'send failed',
      },
    ]);
  });

  it('consumerErrorHandler logs native consumer errors as KafkaErrors', () => {
    const { lines, log } = capture();
    consumerErrorHandler(log)({ message: 'all broker connections are down', code: -187 });
    expect(lines).toHaveLength(1);
    expect(lines[0]?.message).toBe('kafka consumer error');
    expect(lines[0]?.fields['err']).toMatchObject({
      code: -187,
      message: 'consumer: all broker connections are down',
    });
  });
});

describe('NO_REBALANCE_LISTENER', () => {
  it('starts nothing anywhere and ignores revocations', async () => {
    const signal = new AbortController().signal;
    await expect(
      NO_REBALANCE_LISTENER.onAssigned([{ topic: 't', partition: 0 }], signal),
    ).resolves.toEqual([]);
    expect(() => {
      NO_REBALANCE_LISTENER.onRevoked([{ topic: 't', partition: 0 }]);
    }).not.toThrow();
  });
});
