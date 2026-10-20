import fc from 'fast-check';

// Determinism (docs/design.md, Testing standard): property tests run from a fixed seed, so a CI
// failure always reproduces. Explore other seeds locally with FC_SEED=<n> pnpm test; a failing
// property prints the seed and path to replay it.
const seed = Number(process.env['FC_SEED'] ?? '20261020');
if (!Number.isSafeInteger(seed)) throw new Error(`FC_SEED must be an integer, got ${String(seed)}`);
fc.configureGlobal({ seed });
