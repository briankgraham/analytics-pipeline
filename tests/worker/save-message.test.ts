import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Log } from '../../logs/log.ts';
import { saveMessage } from '../../worker/save-message.ts';

const good = { service: 'checkout', level: 'error', message: 'card declined' };
const where = 'logs partition 0 offset 7';

let saved: Log[];
const saveLog = async (log: Log) => {
  saved.push(log);
};
let warn: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
  saved = [];
  // Keep the expected warnings below out of the test output, and check what they say.
  warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('saveMessage', () => {
  it('saves a good log', async () => {
    await saveMessage(JSON.stringify(good), where, saveLog);
    expect(saved).toEqual([good]);
    expect(warn).not.toHaveBeenCalled();
  });

  it('saves only the fields of a log', async () => {
    await saveMessage(JSON.stringify({ ...good, extra: 'ignored' }), where, saveLog);
    expect(saved).toEqual([good]);
  });

  it.each([
    ['empty', null, 'it is empty'],
    ['not JSON', '{"service":', 'it is not JSON'],
    ['not a log', JSON.stringify({ ...good, level: 'loud' }), 'level must be one of'],
    ['a JSON string', '"hello"', 'a log must be a JSON object'],
  ])('skips a message that is %s, saying where it is and why', async (_name, text, why) => {
    await saveMessage(text, where, saveLog);
    expect(saved).toEqual([]);
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]?.[0]).toContain(where);
    expect(warn.mock.calls[0]?.[0]).toContain(why);
  });

  it('passes on a failed save, so Kafka hands the message over again', async () => {
    const failing = async () => {
      throw new Error('Elasticsearch is down');
    };
    await expect(saveMessage(JSON.stringify(good), where, failing)).rejects.toThrow(
      'Elasticsearch is down',
    );
  });
});
