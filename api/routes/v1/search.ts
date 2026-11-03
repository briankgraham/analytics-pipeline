import Router from '@koa/router';
import { searchController } from '../../controllers/search-controller.ts';
import type { Dependencies } from '../../dependencies.ts';

// GET /v1/search: finds logs, for example /v1/search?q=card&level=error.
export function searchRoute(deps: Dependencies): Router {
  const router = new Router();
  router.get('/search', searchController(deps.searchLogs));
  return router;
}
