import { execFileSync, spawnSync } from 'node:child_process';
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

const gateScript = path.resolve(import.meta.dirname, '../../../scripts/verify-gate.sh');

let repo: string;
let bin: string;

/**
 * These tests run inside `pnpm verify`, which itself runs inside a git pre-commit hook. Git
 * exports GIT_DIR, GIT_INDEX_FILE, and friends to hooks, and inheriting them would point every git
 * call here at the real repository. Drop every GIT_* variable and ignore global/system config so
 * the temp repo is the only repository these tests can touch.
 */
function isolatedEnv(extra: Record<string, string> = {}): NodeJS.ProcessEnv {
  const env = Object.fromEntries(
    Object.entries(process.env).filter(([key]) => !key.startsWith('GIT_')),
  );
  return {
    ...env,
    GIT_CONFIG_GLOBAL: '/dev/null',
    GIT_CONFIG_NOSYSTEM: '1',
    PATH: `${bin}:${process.env['PATH'] ?? ''}`,
    ...extra,
  };
}

function git(...args: string[]): string {
  return execFileSync('git', args, { cwd: repo, encoding: 'utf8', env: isolatedEnv() }).trim();
}

/** A fake `pnpm` on PATH that records each call and exits with VERIFY_STATUS. */
function installFakePnpm(): void {
  const fake = path.join(bin, 'pnpm');
  writeFileSync(fake, `#!/bin/sh\necho "$@" >> "${bin}/calls"\nexit "\${VERIFY_STATUS:-0}"\n`);
  chmodSync(fake, 0o755);
}

function runGate(
  verifyStatus = 0,
  env: NodeJS.ProcessEnv = isolatedEnv(),
): { status: number | null; output: string; calls: string[] } {
  const run = spawnSync('sh', [gateScript], {
    cwd: repo,
    encoding: 'utf8',
    env: { ...env, VERIFY_STATUS: String(verifyStatus) },
  });
  const callsFile = path.join(bin, 'calls');
  const calls = existsSync(callsFile) ? readFileSync(callsFile, 'utf8').trim().split('\n') : [];
  return { status: run.status, output: run.stdout + run.stderr, calls };
}

const stampPath = (): string => path.join(repo, git('rev-parse', '--git-path', 'verify-stamp'));

beforeEach(() => {
  repo = mkdtempSync(path.join(tmpdir(), 'verify-gate-repo-'));
  bin = mkdtempSync(path.join(tmpdir(), 'verify-gate-bin-'));
  installFakePnpm();
  git('init', '-q', '-b', 'main');
  git('config', 'user.email', 'test@example.com');
  git('config', 'user.name', 'Test');
  git('config', 'commit.gpgsign', 'false');
  writeFileSync(path.join(repo, 'a.txt'), 'a\n');
  git('add', 'a.txt');
  git('commit', '-q', '--no-verify', '-m', 'init');
  git('switch', '-q', '-c', 'feature');
});

afterEach(() => {
  rmSync(repo, { recursive: true, force: true });
  rmSync(bin, { recursive: true, force: true });
});

describe('verify-gate.sh', () => {
  it('refuses to commit on main without running verify', () => {
    git('switch', '-q', 'main');
    const run = runGate();
    expect(run.status).toBe(1);
    expect(run.output).toContain('refusing to commit on main');
    expect(run.calls).toEqual([]);
  });

  it('refuses unstaged changes', () => {
    writeFileSync(path.join(repo, 'a.txt'), 'changed\n');
    const run = runGate();
    expect(run.status).toBe(1);
    expect(run.output).toContain('unstaged changes');
    expect(run.calls).toEqual([]);
  });

  it('refuses untracked files and names them', () => {
    writeFileSync(path.join(repo, 'new.ts'), 'x\n');
    const run = runGate();
    expect(run.status).toBe(1);
    expect(run.output).toContain('new.ts');
    expect(run.calls).toEqual([]);
  });

  it('runs pnpm verify on a clean staged tree and stamps the tree', () => {
    writeFileSync(path.join(repo, 'a.txt'), 'staged\n');
    git('add', 'a.txt');
    const run = runGate();
    expect(run.status).toBe(0);
    expect(run.calls).toEqual(['verify']);
    expect(readFileSync(stampPath(), 'utf8').trim()).toBe(git('write-tree'));
  });

  it('skips verify when the same tree already passed', () => {
    expect(runGate().calls).toEqual(['verify']);
    const second = runGate();
    expect(second.status).toBe(0);
    expect(second.output).toContain('already verified');
    expect(second.calls).toEqual(['verify']);
  });

  it('re-runs verify when the staged tree changes', () => {
    runGate();
    writeFileSync(path.join(repo, 'a.txt'), 'different\n');
    git('add', 'a.txt');
    expect(runGate().calls).toEqual(['verify', 'verify']);
  });

  it('fails without a stamp when verify fails, and clears a stale stamp', () => {
    runGate();
    writeFileSync(path.join(repo, 'a.txt'), 'broken\n');
    git('add', 'a.txt');
    const run = runGate(1);
    expect(run.status).toBe(1);
    expect(existsSync(stampPath())).toBe(false);
  });

  it('isolates the helpers from git hook variables pointing at another repository', () => {
    const sentinel = mkdtempSync(path.join(tmpdir(), 'verify-gate-sentinel-'));
    try {
      execFileSync('git', ['init', '-q', '-b', 'main'], { cwd: sentinel, env: isolatedEnv() });
      const before = readFileSync(path.join(sentinel, '.git', 'config'), 'utf8');
      const saved = { dir: process.env['GIT_DIR'], index: process.env['GIT_INDEX_FILE'] };
      process.env['GIT_DIR'] = path.join(sentinel, '.git');
      process.env['GIT_INDEX_FILE'] = path.join(sentinel, '.git', 'index');
      try {
        git('config', 'user.name', 'Changed');
        expect(runGate().status).toBe(0);
      } finally {
        if (saved.dir === undefined) delete process.env['GIT_DIR'];
        else process.env['GIT_DIR'] = saved.dir;
        if (saved.index === undefined) delete process.env['GIT_INDEX_FILE'];
        else process.env['GIT_INDEX_FILE'] = saved.index;
      }
      expect(readFileSync(path.join(sentinel, '.git', 'config'), 'utf8')).toBe(before);
      expect(existsSync(path.join(sentinel, '.git', 'index'))).toBe(false);
      expect(git('config', 'user.name')).toBe('Changed');
    } finally {
      rmSync(sentinel, { recursive: true, force: true });
    }
  });
});
