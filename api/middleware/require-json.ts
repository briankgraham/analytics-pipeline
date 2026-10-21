import type { Middleware } from 'koa';

// Refuses a request that doesn't say its body is JSON. It comes before readJsonBody, which quietly
// ignores other content types, so a body in the wrong format would look like an empty one.
export const requireJson: Middleware = async function requireJson(ctx, next) {
  if (ctx.request.type !== 'application/json') {
    ctx.status = 415;
    ctx.body = { error: 'send the log as JSON with the header Content-Type: application/json' };
    return;
  }
  await next();
};
