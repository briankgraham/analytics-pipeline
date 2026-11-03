// The service for GET /v1/search: how the API finds logs, built on the base layer in
// elasticsearch/client.ts. The rest of the API gets a plain `searchLogs` function, so it can be tested
// without a running Elasticsearch.
import { buildLogsQuery, type LogCriterion } from '../../elasticsearch/build-logs-query.ts';
import { canPassWithTime, type ElasticsearchClient } from '../../elasticsearch/client.ts';
import type { Log } from '../../logs/log.ts';

// Says whether a failed search could work if tried again, as opposed to needing a person.
export const canTryAgainLater = canPassWithTime;

// Answers with the logs that match. A words search puts the best match first. With only exact criteria
// every log matches equally, so the order means nothing. Throws if Elasticsearch couldn't answer.
export type SearchLogs = (criteria: readonly LogCriterion[]) => Promise<Log[]>;

// TODO: also answer with counts for each filter value (facets), such as how many matching logs each
// level and service has, so people can see what's behind their results. Each filter's counts must ignore
// that filter's own value, or choosing level=error would show every other level as 0. Compare two ways:
// one request using aggregations and post_filter, or separate queries at once with Promise.all, which
// lets the logs still return when the counts are slow or fail.
//
// Returns the function that searches the given index.
export function createLogSearcher(
  client: Pick<ElasticsearchClient, 'searchDocuments'>,
  index: string,
): SearchLogs {
  return async (criteria) => {
    // Only the worker saves to this index, after checking each log, and the index refuses other fields.
    const documents = await client.searchDocuments(index, buildLogsQuery(criteria));
    return documents as Log[];
  };
}
