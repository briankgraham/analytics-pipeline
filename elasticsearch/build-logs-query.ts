// How a search for logs asks Elasticsearch, built on the generic search-criteria-builder folder.
import { buildElasticsearchQuery } from './search-criteria-builder/build-elasticsearch-query.ts';
import type { Criterion } from './search-criteria-builder/criteria.ts';

// The fields of a log that can be searched, by how logFields in logs-index.ts stores them.
export type LogCriterion = Criterion<'message', 'service' | 'level'>;

// The most logs one search returns.
const mostResults = 50;

// Turns criteria into the search to run: the generic query, plus the choices that are ours to make.
export function buildLogsQuery(criteria: readonly LogCriterion[]) {
  return { query: buildElasticsearchQuery(criteria), size: mostResults };
}
