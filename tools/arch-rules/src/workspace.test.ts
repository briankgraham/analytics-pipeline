import { existsSync, readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';

const repoRoot = path.resolve(import.meta.dirname, '../../..');
const roots = ['packages', 'services', 'apps', 'tools', 'bench'];

function workspaceDirs(): string[] {
  return roots.flatMap((root) => {
    const dir = path.join(repoRoot, root);
    if (!existsSync(dir)) return [];
    return readdirSync(dir, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => `${root}/${entry.name}`);
  });
}

function rootReferences(): string[] {
  const solution = JSON.parse(readFileSync(path.join(repoRoot, 'tsconfig.json'), 'utf8')) as {
    references?: { path: string }[];
  };
  return (solution.references ?? []).map((ref) => path.posix.normalize(ref.path));
}

// `tsc -b` only checks referenced projects, and ESLint does not report type errors, so a package
// missing from the root references would ship type errors with a green `pnpm verify`.
describe('workspace typecheck wiring', () => {
  it('gives every workspace package a package.json and a tsconfig.json', () => {
    const missing = workspaceDirs().filter(
      (dir) =>
        !existsSync(path.join(repoRoot, dir, 'package.json')) ||
        !existsSync(path.join(repoRoot, dir, 'tsconfig.json')),
    );
    expect(missing).toEqual([]);
  });

  it('references exactly the workspace packages from the root tsconfig.json', () => {
    expect(rootReferences().sort()).toEqual(workspaceDirs().sort());
  });
});
