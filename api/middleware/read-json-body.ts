import { bodyParser } from '@koa/bodyparser';

// Reads a JSON body into ctx.request.body. Anything over 100 KB is refused with 413, and a body
// that isn't valid JSON, or isn't an object or array, with 400 (handleErrors words the replies).
export const readJsonBody = bodyParser({ enableTypes: ['json'], jsonLimit: '100kb' });
