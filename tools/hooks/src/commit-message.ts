export const COMMIT_TYPES = [
  'build',
  'chore',
  'ci',
  'docs',
  'feat',
  'fix',
  'perf',
  'refactor',
  'revert',
  'style',
  'test',
] as const;

export const MAX_HEADER_LENGTH = 100;

const HEADER = new RegExp(`^(${COMMIT_TYPES.join('|')})(\\([a-z0-9-]+\\))?!?: \\S`);
const REVIEW_LINE = /^Review: \S/m;

export interface CommitContext {
  /** True only for a real merge in progress (git's MERGE_HEAD exists); a header is not evidence. */
  isMerge: boolean;
}

/** Returns the problems with a commit message; an empty list means it passes (CLAUDE.md rules 3 and 8). */
export function checkCommitMessage(raw: string, context: CommitContext): string[] {
  const lines = raw
    .split('\n')
    .filter((line) => !line.startsWith('#'))
    .map((line) => line.trimEnd());
  while (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();

  const header = lines[0] ?? '';
  if (header === '') return ['commit message is empty'];
  // Merges bring in commits that were each checked; the merge itself has no reviewable diff.
  if (context.isMerge) return [];

  const problems: string[] = [];
  if (!HEADER.test(header)) {
    problems.push(
      `header must be "type(scope)?: subject" with type one of ${COMMIT_TYPES.join(', ')}`,
    );
  }
  if (header.length > MAX_HEADER_LENGTH) {
    problems.push(`header is ${String(header.length)} chars; max is ${String(MAX_HEADER_LENGTH)}`);
  }
  if (lines.length > 1 && lines[1] !== '') {
    problems.push('leave a blank line between the header and the body');
  }
  if (!REVIEW_LINE.test(lines.slice(1).join('\n'))) {
    problems.push(
      'body needs a "Review:" line summarizing the fresh-context review (found / fixed / flagged)',
    );
  }
  return problems;
}
