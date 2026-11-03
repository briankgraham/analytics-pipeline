import Router from '@koa/router';
import { messageController } from '../../controllers/message-controller.ts';
import type { Dependencies } from '../../dependencies.ts';
import { readJsonBody } from '../../middleware/read-json-body.ts';
import { requireJson } from '../../middleware/require-json.ts';

// POST /v1/message: receives a log. The steps run in this order.
export function messageRoute(deps: Dependencies): Router {
  const router = new Router();
  router.post('/message', requireJson, readJsonBody, messageController(deps.sendLog));
  return router;
}
