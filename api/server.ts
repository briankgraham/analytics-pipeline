import Koa from 'koa';
import type { Dependencies } from './dependencies.ts';
import { handleErrors } from './middleware/handle-errors.ts';
import { allRoutes } from './routes/all-routes.ts';

// Builds the web server. main.ts gives it the real dependencies; tests give it fakes.
export function createServer(deps: Dependencies): Koa {
  const router = allRoutes(deps);
  const app = new Koa();
  app.use(handleErrors);
  app.use(router.routes());
  // Answers 405, with an Allow header, when a listed URL is used with the wrong method.
  app.use(router.allowedMethods());
  return app;
}
