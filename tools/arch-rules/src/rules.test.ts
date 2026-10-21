import { createRequire } from 'node:module';
import path from 'node:path';
import { cruise, type IConfiguration, type ICruiseResult } from 'dependency-cruiser';
import { describe, expect, it } from 'vitest';

const repoRoot = path.resolve(import.meta.dirname, '../../..');
const fixtures = path.resolve(import.meta.dirname, '../fixtures');
const config = createRequire(import.meta.url)(
  path.join(repoRoot, '.dependency-cruiser.cjs'),
) as IConfiguration;

async function violatedRules(fixture: string): Promise<string[]> {
  const baseDir = path.join(fixtures, fixture);
  const result = await cruise(['.'], {
    ...config.options,
    baseDir,
    ruleSet: { forbidden: config.forbidden ?? [] },
    // The real config excludes the fixtures; here they are the subject.
    exclude: { path: ['^(packages|services|apps|tools|bench)/[^/]+/dist/'] },
    validate: true,
  });
  const output = result.output as ICruiseResult;
  return [...new Set(output.summary.violations.map((v) => v.rule.name))].sort();
}

describe('dependency-cruiser rules', () => {
  it.each([
    ['service-to-service', ['no-service-to-service']],
    ['package-to-service', ['packages-not-to-services-or-apps']],
    ['runtime-to-tools', ['nothing-to-tools']],
    ['package-to-bench', ['nothing-to-bench']],
    ['app-to-service', ['no-app-to-service']],
    ['service-to-app', ['no-service-to-app']],
    ['domain-kafka-client', ['domain-is-io-free', 'not-to-unresolvable']],
    ['domain-kafkajs', ['domain-is-io-free', 'not-to-unresolvable']],
    ['domain-es-client', ['domain-is-io-free', 'not-to-unresolvable']],
    ['domain-obs-kafka', ['domain-is-io-free']],
    ['circular', ['no-circular']],
    ['dev-dep', ['not-to-dev-dep']],
    ['testing-dev-dep', []],
    ['runtime-to-testing', ['testing-only-from-tests']],
    ['unresolvable', ['not-to-unresolvable']],
    ['undeclared-dep', ['not-to-undeclared-dep']],
    ['clean', []],
  ])('%s violates exactly %j', async (fixture, expected) => {
    expect(await violatedRules(fixture)).toEqual(expected);
  });

  it('covers every configured rule with a firing fixture', () => {
    const names = (config.forbidden ?? []).map((r) => r.name);
    expect(names.sort()).toEqual(
      [
        'domain-is-io-free',
        'no-app-to-service',
        'no-circular',
        'no-service-to-app',
        'nothing-to-bench',
        'no-service-to-service',
        'not-to-dev-dep',
        'not-to-undeclared-dep',
        'not-to-unresolvable',
        'nothing-to-tools',
        'packages-not-to-services-or-apps',
        'testing-only-from-tests',
      ].sort(),
    );
  });
});
