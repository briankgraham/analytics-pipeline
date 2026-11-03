import { buildElasticsearchQuery } from '../elasticsearch/search-criteria-builder/build-elasticsearch-query.ts';
import type { LogCriterion } from './build-criteria.ts';

// The most logs one search returns.
const mostResults = 50;

// Turns criteria into the search to run: the generic query, plus the choices that are ours to make.
export function buildQuery(criteria: readonly LogCriterion[]) {
  return { query: buildElasticsearchQuery(criteria), size: mostResults };
}
