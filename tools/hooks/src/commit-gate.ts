import path from 'node:path';
import { analyzeCommand } from './commit-command.ts';
import { checkCommitMessage } from './commit-message.ts';

export interface GateRun {
  status: number | null;
  output: string;
}

export interface CommitGateDeps {
  /** Runs scripts/verify-gate.sh in `cwd`, capturing combined output. */
  runVerifyGate(cwd: string): GateRun;
  /** Reads a file; throws if it cannot. */
  readFile(file: string): string;
}

export interface GateDecision {
  /** 0 lets the Bash call proceed; 2 blocks it and shows `message` to Claude. */
  exitCode: 0 | 2;
  message: string;
}

export const OUTPUT_TAIL_LINES = 80;

const ALLOW: GateDecision = { exitCode: 0, message: '' };
const block = (message: string): GateDecision => ({
  exitCode: 2,
  message: `commit gate: ${message}`,
});

function readHookInput(input: unknown): { command: string; cwd: string } | null {
  if (typeof input !== 'object' || input === null) return null;
  const { tool_input: toolInput, cwd } = input as Record<string, unknown>;
  if (typeof toolInput !== 'object' || toolInput === null || typeof cwd !== 'string') return null;
  const { command } = toolInput as Record<string, unknown>;
  return typeof command === 'string' ? { command, cwd } : null;
}

function tail(text: string): string {
  return text.trimEnd().split('\n').slice(-OUTPUT_TAIL_LINES).join('\n');
}

/**
 * Claude Code PreToolUse decision for a Bash call (CLAUDE.md rules 1, 3, 8). It enforces
 * everything the husky hooks do, so a worktree without husky shims is still gated: the commit
 * form, the commit message, and the verify gate on the tree in the directory being committed.
 */
export function decideCommitGate(input: unknown, deps: CommitGateDeps): GateDecision {
  const hook = readHookInput(input);
  if (hook === null) return block('unreadable hook input; refusing to guess');

  const analysis = analyzeCommand(hook.command);
  if (analysis.kind === 'none') return ALLOW;
  if (analysis.kind === 'deny') return block(analysis.reason);

  let message: string;
  try {
    message = deps.readFile(path.resolve(hook.cwd, analysis.messageFile));
  } catch (error) {
    return block(`cannot read message file ${analysis.messageFile}: ${String(error)}`);
  }
  const problems = checkCommitMessage(message, { isMerge: false });
  if (problems.length > 0) {
    return block(`commit message rejected:${problems.map((p) => `\n  - ${p}`).join('')}`);
  }

  const run = deps.runVerifyGate(hook.cwd);
  if (run.status === 0) return ALLOW;
  return block(
    `scripts/verify-gate.sh failed (exit ${String(run.status)}); last lines:\n${tail(run.output)}`,
  );
}
