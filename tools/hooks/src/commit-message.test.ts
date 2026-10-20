import { describe, expect, it } from 'vitest';
import { checkCommitMessage, MAX_HEADER_LENGTH } from './commit-message.ts';

const body = '\n\nWhy this change.\n\nReview: 1 pass; found none; fixed none; flagged none\n';
const check = (raw: string): string[] => checkCommitMessage(raw, { isMerge: false });

describe('checkCommitMessage', () => {
  it.each([
    'feat: add schemas',
    'fix(kafka): retry on broker restart',
    'refactor(query-api)!: drop v0 routes',
    'docs(adr): backfill ADRs',
    'revert: feat: add schemas',
  ])('accepts %j with a Review line', (header) => {
    expect(check(header + body)).toEqual([]);
  });

  it('exempts a real merge (MERGE_HEAD present) but not a merge-looking header', () => {
    const merge = "Merge branch 'main' into phase-1/skeleton\n";
    expect(checkCommitMessage(merge, { isMerge: true })).toEqual([]);
    expect(check(merge)).toHaveLength(2);
  });

  it('checks git-generated revert headers like any other commit', () => {
    expect(check('Revert "feat: add schemas"\n\nThis reverts commit abc.\n')).toHaveLength(2);
  });

  it('ignores comment lines and trailing blank lines', () => {
    expect(check(`# Please enter the message\nfeat: x${body}\n\n# comment\n`)).toEqual([]);
  });

  it('rejects an empty message, even for a merge', () => {
    expect(check('\n# only a comment\n\n')).toEqual(['commit message is empty']);
    expect(checkCommitMessage('', { isMerge: true })).toEqual(['commit message is empty']);
  });

  it.each([
    'feature: add schemas',
    'feat:add schemas',
    'feat(Kafka): upper-case scope',
    'Feat: capitalised type',
    'add schemas',
    'fixup! feat: add schemas',
  ])('rejects malformed header %j', (header) => {
    expect(check(header + body)).toEqual([expect.stringContaining('header must be') as string]);
  });

  it('enforces the header length limit at the boundary', () => {
    const atLimit = 'feat: ' + 'x'.repeat(MAX_HEADER_LENGTH - 6);
    expect(check(atLimit + body)).toEqual([]);
    expect(check(atLimit + 'x' + body)).toEqual([
      `header is ${String(MAX_HEADER_LENGTH + 1)} chars; max is ${String(MAX_HEADER_LENGTH)}`,
    ]);
  });

  it('requires a blank line after the header', () => {
    expect(check('feat: x\nReview: 1 pass\n')).toEqual([
      'leave a blank line between the header and the body',
    ]);
  });

  it('requires a Review line in the body, not the header', () => {
    const missing = [expect.stringContaining('"Review:" line') as string];
    expect(check('feat: x\n\nWhy.\n')).toEqual(missing);
    expect(check('feat: x\n')).toEqual(missing);
    expect(check('feat: x\n\nReview:\n')).toEqual(missing);
    expect(check('feat: x\n\n  Review: indented\n')).toEqual(missing);
  });

  it('reports every problem at once', () => {
    expect(check('nope\nbody\n')).toHaveLength(3);
  });
});
