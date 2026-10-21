import type { Middleware } from 'koa';
import type { SendLog } from '../../kafka/send-log.ts';
import { readLog } from '../../logs/log.ts';

// The last step of POST /message (see routes/message.ts): checks the log in the body and puts it into
// Kafka. `sendLog` is what does the putting; main.ts gives the real one to the server (see dependencies.ts).
export function receiveMessage(sendLog: SendLog): Middleware {
  return async function receiveMessage(ctx) {
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
