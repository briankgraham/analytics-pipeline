import { defineConfig } from 'vitest/config';

// Workspace packages export `@obs/source` -> ./src/index.ts so tests run against source, not dist.
const conditions = ['@obs/source'];

const workspace = '{packages,services,apps,tools}/*';
const notTests = ['**/node_modules/**', '**/dist/**', 'tools/arch-rules/fixtures/**'];

export default defineConfig({
  resolve: { conditions },
  ssr: { resolve: { conditions, externalConditions: conditions } },
  test: {
    passWithNoTests: true,
    restoreMocks: true,
    projects: [
      {
        extends: true,
        test: {
          name: 'unit',
          // Any test inside a workspace package runs, not only those under src/.
          include: [`${workspace}/**/*.test.{ts,tsx}`],
          exclude: ['**/*.int.test.{ts,tsx}', ...notTests],
        },
      },
      {
        extends: true,
        test: {
          name: 'integration',
          include: [`${workspace}/**/*.int.test.{ts,tsx}`],
          exclude: notTests,
          // Testcontainers start real Kafka/ES; tests still wait on conditions, never sleeps.
          testTimeout: 60_000,
          hookTimeout: 180_000,
        },
      },
    ],
    coverage: {
      provider: 'v8',
      include: [`${workspace}/src/**/*.{ts,tsx}`],
      // The complete approved exclusion list (docs/design.md, Testing standard). Additions need user approval.
      // Type-only files need no entry: v8 skips modules that emit no code.
      exclude: [
        '**/*.test.{ts,tsx}', // tests themselves
        '**/main.ts', // composition roots: covered by compose smoke/e2e tests
        '**/*.config.ts', // tool config files
        'apps/web/src/main.tsx', // Vite entry
        '**/src/cli/*.main.ts', // CLI bootstrap entry files only (e.g. keys:create); CLI logic is covered
        '**/src/generated/**', // generated code
      ],
      thresholds: { lines: 100, branches: 100, functions: 100, statements: 100 },
      reporter: ['text', 'html', 'json-summary'],
    },
  },
});
