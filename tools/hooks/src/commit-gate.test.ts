import { describe, expect, it } from 'vitest';
import { decideCommitGate, OUTPUT_TAIL_LINES, type CommitGateDeps } from './commit-gate.ts';

const goodMessage = 'feat: x\n\nReview: 1 pass; found none\n';

function deps(
  status: number | null,
  output = '',
  files: Record<string, string> = { '/repo/msg': goodMessage },
): CommitGateDeps & { gateRuns: string[]; reads: string[] } {
  const gateRuns: string[] = [];
  const reads: string[] = [];
  return {
    gateRuns,
    reads,
    runVerifyGate(cwd) {
      gateRuns.push(cwd);
      return { status, output };
    },
    readFile(file) {
      reads.push(file);
      const content = files[file];
      if (content === undefined) throw new Error(`ENOENT: ${file}`);
      return content;
    },
  };
}

const input = (command: string): unknown => ({ cwd: '/repo', tool_input: { command } });

describe('decideCommitGate', () => {
  it('lets unrelated commands through without running anything', () => {
    const d = deps(1);
    expect(decideCommitGate(input('git status'), d)).toEqual({ exitCode: 0, message: '' });
    expect(d.gateRuns).toEqual([]);
    expect(d.reads).toEqual([]);
  });

  it('denies a disallowed commit form without running the gate', () => {
    const d = deps(0);
    const decision = decideCommitGate(input('git commit --no-verify -F msg'), d);
    expect(decision.exitCode).toBe(2);
    expect(decision.message).toContain('"--no-verify" is not allowed');
    expect(d.gateRuns).toEqual([]);
  });

  it('checks the message file relative to cwd, then runs the gate, and allows on success', () => {
    const d = deps(0, 'all good');
    expect(decideCommitGate(input('git commit -F msg'), d)).toEqual({ exitCode: 0, message: '' });
    expect(d.reads).toEqual(['/repo/msg']);
    expect(d.gateRuns).toEqual(['/repo']);
  });

  it('uses an absolute message path as given', () => {
    const d = deps(0, '', { '/tmp/m': goodMessage });
    expect(decideCommitGate(input('git commit -F /tmp/m'), d).exitCode).toBe(0);
    expect(d.reads).toEqual(['/tmp/m']);
  });

  it('rejects a bad message before spending a verify run', () => {
    const d = deps(0, '', { '/repo/msg': 'wip\n' });
    const decision = decideCommitGate(input('git commit -F msg'), d);
    expect(decision.exitCode).toBe(2);
    expect(decision.message).toContain('commit message rejected');
    expect(decision.message).toContain('"Review:" line');
    expect(d.gateRuns).toEqual([]);
  });

  it('blocks when the message file cannot be read', () => {
    const decision = decideCommitGate(input('git commit -F missing'), deps(0));
    expect(decision.exitCode).toBe(2);
    expect(decision.message).toContain('cannot read message file missing');
  });

  it('checks the message file on an amend too', () => {
    const d = deps(0);
    expect(decideCommitGate(input('git commit --amend -F msg'), d).exitCode).toBe(0);
    expect(d.reads).toEqual(['/repo/msg']);
    expect(d.gateRuns).toEqual(['/repo']);
  });

  it('blocks with the tail of the gate output when verify fails', () => {
    const lines = Array.from({ length: OUTPUT_TAIL_LINES + 5 }, (_, i) => `line ${String(i)}`);
    const decision = decideCommitGate(input('git commit -F msg'), deps(1, lines.join('\n') + '\n'));
    expect(decision.exitCode).toBe(2);
    expect(decision.message).toContain('failed (exit 1)');
    expect(decision.message).toContain(`line ${String(OUTPUT_TAIL_LINES + 4)}`);
    expect(decision.message).toContain('line 5\n');
    expect(decision.message).not.toContain('line 4\n');
  });

  it('blocks when the gate could not run at all', () => {
    expect(decideCommitGate(input('git commit -F msg'), deps(null, 'spawn sh ENOENT'))).toEqual({
      exitCode: 2,
      message: expect.stringContaining('exit null') as string,
    });
  });

  it.each([
    null,
    'text',
    {},
    { cwd: '/repo' },
    { cwd: '/repo', tool_input: null },
    { cwd: 1, tool_input: { command: 'git commit -F msg' } },
    { cwd: '/repo', tool_input: { command: 42 } },
  ])('fails closed on malformed input %j', (bad) => {
    expect(decideCommitGate(bad, deps(0)).exitCode).toBe(2);
  });
});
