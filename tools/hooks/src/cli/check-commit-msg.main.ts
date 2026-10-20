// husky commit-msg entry (and CI): node check-commit-msg.main.ts <message-file> [--no-merge]
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { runCommitMsgCheck } from '../commit-msg-cli.ts';

const result = runCommitMsgCheck(process.argv.slice(2), {
  readFile: (file) => readFileSync(file, 'utf8'),
  gitPath: (name) =>
    execFileSync('git', ['rev-parse', '--git-path', name], { encoding: 'utf8' }).trim(),
  exists: existsSync,
});
if (result.stderr !== '') process.stderr.write(`${result.stderr}\n`);
process.exit(result.exitCode);
