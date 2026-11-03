// The service for GET /v1/search: how the API finds logs, built on the base layer in
// elasticsearch/client.ts. The rest of the API gets a plain `searchLogs` function, so it can be tested
// without a running Elasticsearch.
import { canPassWithTime, type ElasticsearchClient } from '../../elasticsearch/client.ts';
import type { Log } from '../../logs/log.ts';
import type { LogCriterion } from '../../search/build-criteria.ts';
import { buildQuery } from '../../search/build-query.ts';

// Says whether a failed search could work if tried again, as opposed to needing a person.
export const canTryAgainLater = canPassWithTime;

// Answers with the logs that match. A words search puts the best match first. With only exact criteria
// every log matches equally, so the order means nothing. Throws if Elasticsearch couldn't answer.
export type SearchLogs = (criteria: readonly LogCriterion[]) => Promise<Log[]>;

// Returns the function that searches the given index.
export function createLogSearcher(
  client: Pick<ElasticsearchClient, 'searchDocuments'>,
  index: string,
): SearchLogs {
  return async (criteria) => {
    // Only the worker saves to this index, after checking each log, and the index refuses other fields.
    const documents = await client.searchDocuments(index, buildQuery(criteria));
    return documents as Log[];
  };
}
