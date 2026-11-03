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

// Returns a setting that must be a whole number of at least 1, or throws naming the setting.
export function readWholeNumber(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a whole number of at least 1`);
  }
  return value;
}

// Returns where Elasticsearch is, which index holds the logs, and how long one call may take.
export function readElasticsearchConnection(file: unknown): {
  elasticsearchUrl: string;
  index: string;
  requestTimeoutMs: number;
} {
  const { url, index, requestTimeoutMs } = section(file, 'elasticsearch');
  if (typeof url !== 'string' || !/^https?:\/\/./.test(url) || !URL.canParse(url)) {
    throw new Error('elasticsearch.url must be an address like http://127.0.0.1:9200');
  }
  // Elasticsearch's rules for index names, kept simple: lowercase letters, digits, '.', '_' and
  // '-', starting with a letter or digit, at most 255 characters.
  if (typeof index !== 'string' || !/^[a-z0-9][a-z0-9._-]{0,254}$/.test(index)) {
    throw new Error(
      "elasticsearch.index must be lowercase letters, digits, '.', '_' or '-', starting with a letter or digit",
    );
  }
  return {
    elasticsearchUrl: url,
    index,
    requestTimeoutMs: readWholeNumber(requestTimeoutMs, 'elasticsearch.requestTimeoutMs'),
  };
}
