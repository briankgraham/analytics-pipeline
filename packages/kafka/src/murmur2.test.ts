import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { murmur2, murmur2Bytes, partitionFor } from './murmur2.js';

describe('murmur2 (Kafka DefaultPartitioner)', () => {
  // Reference values from Kafka's Java client (UtilsTest.testMurmur2) and kafkajs's test suite.
  it.each([
    ['21', -973932308],
    ['foobar', -790332482],
    ['a-little-bit-long-string', -985981536],
    ['a-little-bit-longer-string', -1486304829],
    ['lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8', -58897971],
    ['abc', 479470107],
  ])('hashes %j like the Java client', (key, expected) => {
    expect(murmur2(key)).toBe(expected);
  });

  it('covers every tail length', () => {
    expect(new Set(['', 'a', 'ab', 'abc', 'abcd'].map((k) => murmur2(k))).size).toBe(5);
  });

  it('hashes UTF-8 bytes, not UTF-16 code units', () => {
    // 'é' is C3 A9 in UTF-8 but E9 00 as a UTF-16LE code unit; '😀' is F0 9F 98 80 in UTF-8.
    expect(murmur2('é')).toBe(murmur2Bytes(Buffer.from([0xc3, 0xa9])));
    expect(murmur2('é')).not.toBe(murmur2Bytes(Buffer.from([0xe9, 0x00])));
    expect(murmur2('😀')).toBe(murmur2Bytes(Buffer.from([0xf0, 0x9f, 0x98, 0x80])));
    // librdkafka's own placement of non-ASCII keys is checked against a real broker in the contract.
  });

  it('maps every key into [0, partitions)', () => {
    fc.assert(
      fc.property(fc.string(), fc.integer({ min: 1, max: 64 }), (key, partitions) => {
        const p = partitionFor(key, partitions);
        expect(Number.isInteger(p) && p >= 0 && p < partitions).toBe(true);
      }),
    );
  });
});
