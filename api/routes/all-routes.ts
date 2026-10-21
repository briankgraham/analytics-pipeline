import Router from '@koa/router';
import type { Dependencies } from '../dependencies.ts';
import { messageRoute } from './v1/message.ts';

// Every route the API has, grouped by version. Each route lives in its own file in the folder of
// its version; a new one is added to that version's list. A request to any other URL gets 404, and
// the wrong method on a listed URL gets 405 (see server.ts).
//
// To change something in a way that would break existing senders, add a v2 folder and list instead
// of editing v1, so old senders keep working.
const v1Routes = [messageRoute];

export function allRoutes(deps: Dependencies): Router {
  const router = new Router();

  const v1 = new Router({ prefix: '/v1' });
  for (const route of v1Routes) {
    v1.use(route(deps).routes());
  }
  router.use(v1.routes());

  return router;
}
