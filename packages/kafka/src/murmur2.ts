/**
 * Kafka's Java DefaultPartitioner hash (also kafkajs's, and librdkafka's `murmur2_random`):
 * murmur2 with seed 0x9747b28c over the key's UTF-8 bytes. Window locality depends on every
 * producer agreeing on key -> partition, so the fake uses this and the contract suite checks a real
 * broker places keys the same way.
 */
export function murmur2(key: string): number {
  return murmur2Bytes(Buffer.from(key, 'utf8'));
}

/** murmur2 over raw bytes (Java's `Utils.murmur2(byte[])`). */
export function murmur2Bytes(data: Buffer): number {
  const m = 0x5bd1e995;
  const r = 24;
  let h = (0x9747b28c ^ data.length) | 0;
  const blocks = data.length >> 2;
  for (let i = 0; i < blocks; i++) {
    let k = data.readInt32LE(i * 4);
    k = Math.imul(k, m);
    k ^= k >>> r;
    k = Math.imul(k, m);
    h = Math.imul(h, m) ^ k;
  }
  // The 1-3 trailing bytes, highest first (Java's fall-through switch, unrolled).
  const tail = blocks * 4;
  const remaining = data.length & 3;
  if (remaining === 3) h ^= data.readUInt8(tail + 2) << 16;
  if (remaining >= 2) h ^= data.readUInt8(tail + 1) << 8;
  if (remaining >= 1) {
    h ^= data.readUInt8(tail);
    h = Math.imul(h, m);
  }
  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;
  return h | 0;
}

/** The partition Kafka's default partitioner picks for a non-null key. */
export function partitionFor(key: string, partitions: number): number {
  return (murmur2(key) & 0x7fffffff) % partitions;
}
