import { checkCommitMessage } from './commit-message.ts';

export interface CommitMsgCliDeps {
  readFile(file: string): string;
  /** `git rev-parse --git-path <name>`: the path of a file inside the current worktree's git dir. */
  gitPath(name: string): string;
  exists(file: string): boolean;
}

export interface CliResult {
  exitCode: 0 | 1 | 2;
  stderr: string;
}

export const USAGE = 'usage: check-commit-msg.main.ts <message-file> [--no-merge]';

/**
 * The commit-msg check as a CLI (husky's commit-msg hook and CI). A message is exempt only during
 * a real merge (git's MERGE_HEAD exists). CI passes --no-merge because it checks commits that
 * already exist, so MERGE_HEAD says nothing about them.
 */
export function runCommitMsgCheck(args: readonly string[], deps: CommitMsgCliDeps): CliResult {
  const [file, flag, ...extra] = args;
  if (file === undefined || file.startsWith('-') || extra.length > 0) {
    return { exitCode: 2, stderr: USAGE };
  }
  if (flag !== undefined && flag !== '--no-merge') return { exitCode: 2, stderr: USAGE };

  const isMerge = flag === undefined && deps.exists(deps.gitPath('MERGE_HEAD'));
  const problems = checkCommitMessage(deps.readFile(file), { isMerge });
  if (problems.length === 0) return { exitCode: 0, stderr: '' };
  return { exitCode: 1, stderr: `commit-msg:${problems.map((p) => `\n  - ${p}`).join('')}` };
}
