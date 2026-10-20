// Claude Code PreToolUse (Bash) entry: hook JSON on stdin; exit 2 blocks the call.
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { decideCommitGate } from '../commit-gate.ts';

const gateScript = path.resolve(import.meta.dirname, '../../../../scripts/verify-gate.sh');
let input: unknown = null;
try {
  input = JSON.parse(readFileSync(0, 'utf8'));
} catch {
  // decideCommitGate fails closed on null input.
}
const decision = decideCommitGate(input, {
  runVerifyGate(cwd) {
    const run = spawnSync('sh', [gateScript], {
      cwd,
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024,
    });
    return { status: run.status, output: `${run.stdout}${run.stderr}${run.error?.message ?? ''}` };
  },
  readFile: (file) => readFileSync(file, 'utf8'),
});
if (decision.message !== '') process.stderr.write(`${decision.message}\n`);
process.exit(decision.exitCode);
