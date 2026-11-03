import type { Criterion } from './criteria.ts';

export type ElasticsearchQuery = { bool: { must: object[]; filter: object[] } };

// `must` counts towards how well a document matches, so the best matches come first. `filter` only
// keeps or drops a document, which is right for yes-or-no questions and faster.
function translate(criterion: Criterion): { clause: 'must' | 'filter'; query: object } {
  switch (criterion.type) {
    case 'words':
      // operator 'and' means every word. Elasticsearch's default is any one word.
      return {
        clause: 'must',
        query: { match: { [criterion.field]: { query: criterion.words, operator: 'and' } } },
      };
    case 'exact':
      return { clause: 'filter', query: { term: { [criterion.field]: criterion.value } } };
  }
}

// Combines criteria into one Elasticsearch query that a document must satisfy all of. No criteria
// means every document matches, so a caller that doesn't want that has to refuse an empty list itself.
export function buildElasticsearchQuery(criteria: readonly Criterion[]): ElasticsearchQuery {
  const translated = criteria.map(translate);
  const queriesIn = (clause: 'must' | 'filter') =>
    translated.filter((item) => item.clause === clause).map((item) => item.query);
  return { bool: { must: queriesIn('must'), filter: queriesIn('filter') } };
}
