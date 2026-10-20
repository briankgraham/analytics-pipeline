import { describe, expect, it } from 'vitest';
import { analyzeCommand } from './commit-command.ts';

describe('analyzeCommand', () => {
  it.each(['git status', 'git log --oneline', 'pnpm verify', 'echo committed', 'gitcommit'])(
    'ignores %j',
    (command) => {
      expect(analyzeCommand(command)).toEqual({ kind: 'none' });
    },
  );

  it.each([
    ['git commit -F msg.txt', 'msg.txt'],
    ['  git commit -F /tmp/x/msg  ', '/tmp/x/msg'],
    ['git commit --file msg', 'msg'],
    ['git commit --file=msg', 'msg'],
    ['git commit -q -F msg', 'msg'],
    ['git commit --amend -F msg', 'msg'],
    ['git commit -F first -F second', 'second'],
  ])('allows %j', (command, messageFile) => {
    expect(analyzeCommand(command)).toEqual({ kind: 'commit', messageFile });
  });

  it.each([
    // bypasses, including spellings a denylist would miss
    ['git commit --no-verify -F m', '--no-verify'],
    ['git commit --no-veri -F m', '--no-veri'],
    ['git commit -n -F m', '-n'],
    ['git commit -nF m', '-nF'],
    ['HUSKY=0 git commit -F m', 'standalone'],
    ['git -c core.hooksPath=/dev/null commit -F m', 'standalone'],
    ['git "commit" --no-verify -F m', 'standalone'],
    ['git co""mmit --no-verify -F m', 'standalone'],
    ['\\git commit -F m', 'standalone'],
    ['/usr/bin/git commit -F m', 'standalone'],
    ["git -c alias.ci='commit --no-verify' ci", 'standalone'],
    ['git --git-dir .git commit -F m', 'standalone'],
    // the committed tree or directory could differ from the verified one
    ['git -C ../main commit -F m', 'standalone'],
    ['cd ../main && git commit -F m', 'standalone'],
    ['git add -A && git commit -F m', 'standalone'],
    ['git commit -F m && git push', '"&&"'],
    ['sed -i s/a/b/ f; git commit -F m', 'standalone'],
    ['git commit -a -F m', '"-a"'],
    ['git commit --all -F m', '"--all"'],
    ['git commit -F m src/file.ts', '"src/file.ts"'],
    ['git commit -F m; rm -rf x', 'plain file path'],
    // message must come from a plain file
    ['git commit -m "feat: x"', '"-m"'],
    ['git commit', 'message file is required'],
    ['git commit --amend', 'message file is required'],
    ['git commit --amend --no-edit', '"--no-edit"'],
    ['git commit -F -', 'plain file path'],
    ['git commit -F --no-verify', 'plain file path'],
    ['git commit --file=-n', 'plain file path'],
    ['git commit -F', 'plain file path'],
    ['git commit --file=', 'plain file path'],
    ['git commit -F $(mktemp)', 'plain file path'],
    ['git commit -F <(echo x)', 'plain file path'],
    // conservative: mentions that are not commits are refused too
    ['git log --grep commit', 'standalone'],
    ['git commit-tree HEAD^{tree}', 'standalone'],
  ])('denies %j', (command, detail) => {
    const result = analyzeCommand(command);
    expect(result).toEqual({ kind: 'deny', reason: expect.stringContaining(detail) as string });
    expect(result).toEqual({
      kind: 'deny',
      reason: expect.stringContaining('git commit -F') as string,
    });
  });
});
