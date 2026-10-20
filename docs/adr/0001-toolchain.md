# ADR-0001: Toolchain (Node 24 LTS, pnpm via corepack, TypeScript 6.0, ESM)

- Status: accepted
- Date: 2026-10-20
- Decision log: 2026-10-20, "Toolchain pinned"

## Context
Every service, package, and the UI share one toolchain, so this affects all of them (rule 6). The machine started on Node v25 (non-LTS) without pnpm. The quality bar needs type-aware linting (`typescript-eslint` strict-type-checked), a 100% coverage gate, and one `pnpm verify` entry point.

Facts on 2026-10-20:
- Node 24 (Krypton) is the current LTS (24.22.0). Node 26 is still "Current" and is expected to be promoted to LTS around 2026-10-28.
- Node 25 and later no longer bundle corepack; Node 24 still does.
- TypeScript 7.0.2 (the native Go port) is `latest`, but `typescript-eslint` 8.72 supports only `typescript >=4.8.4 <6.1.0`.

## Decision
- **Node 24.22.0**, pinned in `.nvmrc`, `engines.node` (`>=24.22.0 <25`, enforced by `engineStrict`), and Docker base images (`node:24.22.0-*`).
- **pnpm 12.14.0** through corepack (`packageManager` with its sha512). pnpm settings live in `pnpm-workspace.yaml` (`saveExact`, `engineStrict`); pnpm 12 does not read them from `.npmrc`. Every dependency is pinned exactly.
- **TypeScript 6.0.3** for both `tsc -b` and ESLint. One compiler means typecheck and lint can never disagree.
- **ESM only** (`"type": "module"`, `module: nodenext`). Workspace packages are `@obs/<name>` and export a custom `@obs/source` condition pointing at `src/index.ts`, ahead of `dist`. TypeScript (`customConditions`), Vitest, and dependency-cruiser resolve source, so there is no prebuild step during development. Runtime images use `dist`.
- **Lint suppressions are inert:** ESLint runs with `linterOptions.noInlineConfig`, and `scripts/check-suppressions.sh` rejects coverage, type-check, and Stryker suppression comments. Approved exceptions (rule 4) go into config with a reason.
- **Typecheck wiring is tested:** `tsc -b` only checks projects listed in the root `tsconfig.json` references (and ESLint does not report type errors), so `tools/arch-rules` asserts every workspace package has a `tsconfig.json` and is referenced.
- **Coverage exclusions match the approved list literally:** no filename-based "type-only" glob (v8 already skips modules that emit no code, and a `*.types.ts` glob would hide runtime zod schemas); CLI exclusion covers only `src/cli/*.main.ts` entry files. Tests may live anywhere in a package (not only `src/`, but inside the package's tsconfig `include`, or type-aware lint fails), so none is silently skipped.
- **dependency-cruiser rules are tested** against fixtures in `tools/arch-rules`, so a rule that silently matches nothing fails the build. Writing those tests found three such gaps in the first config: `includeOnly` filtered target modules, excluding `node_modules` removed npm dependencies from the graph, and an unanchored `/dist/` exclude matched npm packages' entry points.

## Alternatives considered
- **Node 26:** longer support (end of life Apr 2029 vs Apr 2028), but not yet LTS on the decision date. It also has no bundled corepack, and the native Kafka client's prebuilt binaries for it are unverified. Revisit after its LTS promotion; the bump is a one-line change in three places plus a CI run.
- **TypeScript 7 (tsgo) for typecheck, TS 6 for lint:** faster typecheck, but two compilers can disagree on the same code. Revisit when `typescript-eslint` supports 7.
- **npm or Yarn workspaces:** pnpm's strict `node_modules` catches undeclared dependencies. Our `not-to-undeclared-dep` rule covers the remaining gap, where resolution walks up to the root `node_modules`.
- **Prebuilding packages (`tsc -b --watch`) instead of a source condition:** adds a step that can go stale, and stale `dist` produces confusing test results.

## Consequences
- Prettier skips `*.md` (the hand-wrapped design tables would churn). Docs stay consistent through review instead.
- Contributors need nvm (or any Node manager that honours `.nvmrc`) plus `corepack enable`.
- Upgrade triggers: Node 26 LTS + verified Kafka client binaries; `typescript-eslint` supporting TS 7.
