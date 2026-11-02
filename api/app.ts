import Koa from 'koa';
import type { Dependencies } from './dependencies.ts';
import { handleErrors } from './middleware/handle-errors.ts';
import { allRoutes } from './routes/all-routes.ts';

// Puts the middleware and routes together, in the order every request passes through them. It opens no
// port and talks to nothing outside, so tests can give it fakes. server.ts gives it the real dependencies.
export function createApp(deps: Dependencies): Koa {
  const router = allRoutes(deps);
  const app = new Koa();
  app.use(handleErrors);
  app.use(router.routes());
  // Answers 405, with an Allow header, when a listed URL is used with the wrong method.
  app.use(router.allowedMethods());
  return app;
}
