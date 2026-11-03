import { describe, expect, it } from 'vitest';
import { buildElasticsearchQuery } from '../../../elasticsearch/search-criteria-builder/build-elasticsearch-query.ts';
import type { Criterion } from '../../../elasticsearch/search-criteria-builder/criteria.ts';

describe('buildElasticsearchQuery', () => {
  it('needs every word of a words criterion', () => {
    const query = buildElasticsearchQuery([{ type: 'words', field: 'text', words: 'a b' }]);
    expect(query).toEqual({
      bool: { must: [{ match: { text: { query: 'a b', operator: 'and' } } }], filter: [] },
    });
  });

  it('turns an exact criterion into a filter', () => {
    const query = buildElasticsearchQuery([{ type: 'exact', field: 'kind', value: 'A' }]);
    expect(query).toEqual({ bool: { must: [], filter: [{ term: { kind: 'A' } }] } });
  });

  it('combines several criteria, keeping the order within each part', () => {
    const query = buildElasticsearchQuery([
      { type: 'exact', field: 'kind', value: 'A' },
      { type: 'words', field: 'text', words: 'one' },
      { type: 'exact', field: 'colour', value: 'red' },
      { type: 'words', field: 'title', words: 'two' },
    ]);
    expect(query).toEqual({
      bool: {
        must: [
          { match: { text: { query: 'one', operator: 'and' } } },
          { match: { title: { query: 'two', operator: 'and' } } },
        ],
        filter: [{ term: { kind: 'A' } }, { term: { colour: 'red' } }],
      },
    });
  });

  it('matches every document when there are no criteria', () => {
    expect(buildElasticsearchQuery([])).toEqual({ bool: { must: [], filter: [] } });
  });

  it("accepts criteria limited to the caller's own fields, and refuses the wrong pairing", () => {
    const criteria: Criterion<'text', 'kind'>[] = [{ type: 'words', field: 'text', words: 'a' }];
    expect(buildElasticsearchQuery(criteria).bool.must).toHaveLength(1);
    // @ts-expect-error 'text' is split into words, so it can't take an exact criterion.
    const wrong: Criterion<'text', 'kind'> = { type: 'exact', field: 'text', value: 'a' };
    expect(wrong.field).toBe('text');
  });
});
