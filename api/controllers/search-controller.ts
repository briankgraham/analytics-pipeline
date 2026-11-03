import { parse } from 'node:querystring';
import type { Middleware } from 'koa';
import { buildCriteria } from '../../search/build-criteria.ts';
import { canTryAgainLater, type SearchLogs } from '../../search/search-service.ts';

// The last step of GET /v1/search (see routes/v1/search.ts): checks the URL's values and answers with
// the logs that match. `searchLogs` does the searching; server.ts gives the real one (see dependencies.ts).
export function searchController(searchLogs: SearchLogs): Middleware {
  return async function searchController(ctx) {
    // Not ctx.query: Koa builds it on a plain object, so a name like __proto__ never shows up as a value
    // and buildCriteria couldn't refuse it. maxKeys 0 means no limit, so no name goes unchecked.
    const result = buildCriteria(parse(ctx.querystring, undefined, undefined, { maxKeys: 0 }));
    if (!result.ok) {
      ctx.status = 400;
      ctx.body = { error: result.problem };
      return;
    }

    try {
      ctx.body = { logs: await searchLogs(result.criteria) };
    } catch (error) {
      // Anything else needs a person, so saying "try again" would mislead. handleErrors answers 500.
      if (!canTryAgainLater(error)) {
        throw error;
      }
      console.error('could not search Elasticsearch:', error);
      ctx.status = 503;
      ctx.body = { error: 'could not search right now, try again' };
    }
  };
}
