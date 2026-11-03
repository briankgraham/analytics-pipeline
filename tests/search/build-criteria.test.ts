import { describe, expect, it } from 'vitest';
import { buildCriteria } from '../../search/build-criteria.ts';

describe('buildCriteria', () => {
  it('turns q into a words search on the message', () => {
    expect(buildCriteria({ q: 'card declined' })).toEqual({
      ok: true,
      criteria: [{ type: 'words', field: 'message', words: 'card declined' }],
    });
  });

  it('turns service and level into exact searches', () => {
    expect(buildCriteria({ service: 'checkout', level: 'error' })).toEqual({
      ok: true,
      criteria: [
        { type: 'exact', field: 'service', value: 'checkout' },
        { type: 'exact', field: 'level', value: 'error' },
      ],
    });
  });

  it('refuses a search for nothing, which would return every log', () => {
    expect(buildCriteria({})).toEqual({
      ok: false,
      problem: 'give at least one of: q, service, level',
    });
  });

  it('refuses a name it does not know, so a typo cannot widen the search', () => {
    expect(buildCriteria({ q: 'card', levle: 'error' })).toEqual({
      ok: false,
      problem: '"levle" is not something you can search by: use q, service or level',
    });
  });

  it('refuses a level that does not exist', () => {
    expect(buildCriteria({ level: 'loud' })).toEqual({
      ok: false,
      problem: 'level must be one of: debug, info, warn, error',
    });
  });

  it.each([
    ['empty', { q: '' }],
    ['only spaces', { service: '   ' }],
    ['given twice', { q: ['a', 'b'] }],
  ])('refuses a value that is %s', (_name, values) => {
    const result = buildCriteria(values);
    expect(result.ok).toBe(false);
  });
});
