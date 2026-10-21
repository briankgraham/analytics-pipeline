import type { Middleware } from 'koa';

// Turns every failure further down the line into a short JSON reply instead of Koa's default text
// and stack trace. It must come first, so it can catch what the others throw.
export const handleErrors: Middleware = async function handleErrors(ctx, next) {
  try {
    await next();
  } catch (error) {
    const status = (error as { status?: number }).status;
    if (status === 400) {
      ctx.status = 400;
      ctx.body = { error: 'the body is not valid JSON, or is not an object' };
    } else if (status === 413) {
      ctx.status = 413;
      ctx.body = { error: 'the body is too large' };
    } else if (typeof status === 'number' && status >= 400 && status < 500) {
      // Some other mistake in the request that the body parser noticed, such as an encoding it
      // can't read.
      ctx.status = status;
      ctx.body = { error: 'the request could not be read' };
    } else {
      console.error(error);
      ctx.status = 500;
      ctx.body = { error: 'something went wrong on our side' };
    }
  }
};
