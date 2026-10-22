import type { SaveLog } from '../elasticsearch/save-log.ts';
import { readLog } from '../logs/log.ts';

// Handles one message from Kafka: checks that it's a log and saves it to Elasticsearch. A message
// that isn't a good log is skipped with a warning, because trying it again would never work and
// would hold up every message behind it. If saving fails, the error is passed on, so Kafka hands
// over the same message again later (see kafka/read-logs.ts).
export async function saveMessage(
  text: string | null,
  where: string,
  saveLog: SaveLog,
): Promise<void> {
  if (text === null) {
    console.warn(`worker: skipped the message at ${where}: it is empty`);
    return;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    console.warn(`worker: skipped the message at ${where}: it is not JSON`);
    return;
  }

  const result = readLog(parsed);
  if (!result.ok) {
    console.warn(`worker: skipped the message at ${where}: ${result.problem}`);
    return;
  }

  await saveLog(result.log);
}
