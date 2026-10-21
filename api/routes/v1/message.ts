import Router from '@koa/router';
import type { Dependencies } from '../../dependencies.ts';
import { readJsonBody } from '../../middleware/read-json-body.ts';
import { receiveMessage } from '../../middleware/receive-message.ts';
import { requireJson } from '../../middleware/require-json.ts';

// POST /v1/message: receives a log. The steps run in this order.
export function messageRoute(deps: Dependencies): Router {
  const router = new Router();
  router.post('/message', requireJson, readJsonBody, receiveMessage(deps.sendLog));
  return router;
}
