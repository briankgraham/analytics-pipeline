import type { Middleware } from 'koa';
import type { SendLog } from '../services/message-service.ts';
import { readLog } from '../../logs/log.ts';

// The last step of POST /v1/message (see routes/v1/message.ts): checks the log in the body and puts it into
// Kafka. `sendLog` is what does the putting; server.ts gives the real one (see dependencies.ts).
export function messageController(sendLog: SendLog): Middleware {
  return async function messageController(ctx) {
    const result = readLog(ctx.request.body);
    if (!result.ok) {
      ctx.status = 400;
      ctx.body = { error: result.problem };
      return;
    }

    try {
      await sendLog(result.log);
    } catch (error) {
      // Kafka didn't store it, so we don't say it was accepted. The sender can try again.
      console.error('could not put the log into Kafka:', error);
      ctx.status = 503;
      ctx.body = { error: 'could not store the log right now, try again' };
      return;
    }

    ctx.status = 202;
    ctx.body = { accepted: true };
  };
}
