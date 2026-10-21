// Type-only: runtime methods of the native consumer missing from the package's .d.ts
// (lib/kafka-consumer.js).
declare module '@confluentinc/kafka-javascript' {
  interface KafkaConsumer {
    /** Without it, consume(n) waits the full timeout before *every* record, not just the first. */
    setDefaultIsTimeoutOnlyForFirstMessage(isTimeoutOnlyForFirstMessage: boolean): void;
    /**
     * Commits on a worker thread and reports through `cb`. Unlike commit() + offset_commit_cb, whose
     * result only arrives during a consume() call, and commitSync(), which blocks the event loop.
     */
    commitCb(
      topicPartitions: { topic: string; partition: number; offset: number }[],
      cb: (error: unknown) => void,
    ): this;
  }
}

export {};
