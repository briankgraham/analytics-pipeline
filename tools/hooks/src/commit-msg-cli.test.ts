import { describe, expect, it } from 'vitest';
import { runCommitMsgCheck, USAGE, type CommitMsgCliDeps } from './commit-msg-cli.ts';

const good = 'feat: x\n\nReview: 1 pass; found none\n';
const mergeMessage = "Merge branch 'main' into feature\n";

function deps(
  message: string,
  mergeInProgress: boolean,
): CommitMsgCliDeps & { gitPaths: string[] } {
  const gitPaths: string[] = [];
  return {
    gitPaths,
    readFile: (file) => {
      expect(file).toBe('MSG');
      return message;
    },
    gitPath: (name) => {
      gitPaths.push(name);
      return `/repo/.git/worktrees/w/${name}`;
    },
    exists: (file) => mergeInProgress && file === '/repo/.git/worktrees/w/MERGE_HEAD',
  };
}

describe('runCommitMsgCheck', () => {
  it('passes a good message', () => {
    expect(runCommitMsgCheck(['MSG'], deps(good, false))).toEqual({ exitCode: 0, stderr: '' });
  });

  it('fails a bad message with every problem listed', () => {
    const result = runCommitMsgCheck(['MSG'], deps('wip\n', false));
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain('header must be');
    expect(result.stderr).toContain('"Review:" line');
  });

  it('exempts a merge only when MERGE_HEAD exists in this worktree', () => {
    const merging = deps(mergeMessage, true);
    expect(runCommitMsgCheck(['MSG'], merging).exitCode).toBe(0);
    expect(merging.gitPaths).toEqual(['MERGE_HEAD']);
    expect(runCommitMsgCheck(['MSG'], deps(mergeMessage, false)).exitCode).toBe(1);
  });

  it('ignores MERGE_HEAD with --no-merge (CI checks existing commits)', () => {
    const d = deps(mergeMessage, true);
    expect(runCommitMsgCheck(['MSG', '--no-merge'], d).exitCode).toBe(1);
    expect(d.gitPaths).toEqual([]);
  });

  it.each([[[]], [['--no-merge']], [['MSG', '--merge']], [['MSG', '--no-merge', 'x']], [['-n']]])(
    'rejects bad arguments %j with usage',
    (args) => {
      expect(runCommitMsgCheck(args, deps(good, false))).toEqual({ exitCode: 2, stderr: USAGE });
    },
  );
});
