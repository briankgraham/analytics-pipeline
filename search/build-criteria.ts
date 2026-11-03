import type { Criterion } from '../elasticsearch/search-criteria-builder/criteria.ts';
import { levels } from '../logs/log.ts';

// The fields of a log that can be searched, by how logFields in elasticsearch/logs-index.ts stores them.
export type LogCriterion = Criterion<'message', 'service' | 'level'>;

export type BuildCriteriaResult =
  { ok: true; criteria: LogCriterion[] } | { ok: false; problem: string };

const knownValues = ['q', 'service', 'level'];

// A URL can name a value more than once (?q=a&q=b), which arrives here as a list.
type UrlValues = Record<string, string | string[] | undefined>;

// Checks the values from a search URL and turns them into criteria, or returns a plain sentence saying
// what is wrong. A misspelled name is refused, because ignoring it would quietly find too many logs.
export function buildCriteria(values: UrlValues): BuildCriteriaResult {
  const unknown = Object.keys(values).find((name) => !knownValues.includes(name));
  if (unknown !== undefined) {
    return {
      ok: false,
      problem: `"${unknown}" is not something you can search by: use q, service or level`,
    };
  }

  const criteria: LogCriterion[] = [];
  for (const [name, value] of Object.entries(values)) {
    if (typeof value !== 'string' || value.trim() === '') {
      return { ok: false, problem: `${name} must be given once, with a value` };
    }
    if (name === 'q') {
      criteria.push({ type: 'words', field: 'message', words: value });
    } else if (name === 'service') {
      criteria.push({ type: 'exact', field: 'service', value });
    } else if (levels.some((known) => known === value)) {
      criteria.push({ type: 'exact', field: 'level', value });
    } else {
      return { ok: false, problem: `level must be one of: ${levels.join(', ')}` };
    }
  }

  if (criteria.length === 0) {
    return { ok: false, problem: 'give at least one of: q, service, level' };
  }
  return { ok: true, criteria };
}
