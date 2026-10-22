// Small checks used by both api/settings.ts and worker/settings.ts when they read a config file.

// Returns one top-level section of a parsed config file, such as "kafka:", or throws if it's missing.
export function section(file: unknown, name: string): Record<string, unknown> {
  const value =
    typeof file === 'object' && file !== null ? (file as Record<string, unknown>)[name] : undefined;
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`the config must have a "${name}:" section`);
  }
  return value as Record<string, unknown>;
}

// Returns kafka.brokers from a parsed config file, or throws if it isn't a list of addresses.
export function readKafkaBrokers(file: unknown): string[] {
  const { brokers } = section(file, 'kafka');
  if (
    !Array.isArray(brokers) ||
    brokers.length === 0 ||
    !brokers.every((broker) => typeof broker === 'string' && broker !== '')
  ) {
    throw new Error('kafka.brokers must be a list of addresses like 127.0.0.1:9092');
  }
  return brokers as string[];
}
