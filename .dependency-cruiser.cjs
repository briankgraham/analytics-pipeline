// Architecture rules (CLAUDE.md rule 5). Each rule has a fixture test in tools/arch-rules.
/** @type {import('dependency-cruiser').IConfiguration} */
module.exports = {
  forbidden: [
    {
      name: 'no-circular',
      severity: 'error',
      comment: 'Cycles make lifecycles and test seams impossible to reason about.',
      from: {},
      to: { circular: true },
    },
    {
      name: 'no-service-to-service',
      severity: 'error',
      comment: 'Services never import other services; shared code lives in packages/*.',
      from: { path: '^services/([^/]+)/' },
      to: { path: '^services/', pathNot: '^services/$1/' },
    },
    {
      name: 'packages-not-to-services-or-apps',
      severity: 'error',
      comment: 'Shared packages sit below services and apps.',
      from: { path: '^packages/' },
      to: { path: '^(services|apps)/' },
    },
    {
      name: 'no-app-to-service',
      severity: 'error',
      comment: 'Apps talk to services over HTTP; shared contracts live in packages/schemas.',
      from: { path: '^apps/' },
      to: { path: '^services/' },
    },
    {
      name: 'no-service-to-app',
      severity: 'error',
      comment: 'Services never depend on UI code.',
      from: { path: '^services/' },
      to: { path: '^apps/' },
    },
    {
      name: 'nothing-to-tools',
      severity: 'error',
      comment: 'tools/* is dev tooling; runtime code never depends on it.',
      from: { path: '^(packages|services|apps)/' },
      to: { path: '^tools/' },
    },
    {
      name: 'nothing-to-bench',
      severity: 'error',
      comment:
        'bench/* holds benchmark harnesses (not coverage-gated); nothing may depend on them.',
      from: { path: '^(packages|services|apps|tools)/' },
      to: { path: '^bench/' },
    },
    {
      name: 'domain-is-io-free',
      severity: 'error',
      comment: 'Domain classes never import Kafka or Elasticsearch clients.',
      from: { path: '/src/domain/' },
      to: {
        path: [
          '@confluentinc/kafka-javascript',
          '(^|/)kafkajs(/|$)',
          '@elastic/elasticsearch',
          '^packages/kafka/',
        ],
      },
    },
    {
      name: 'not-to-dev-dep',
      severity: 'error',
      comment: 'Production code must not depend on devDependencies.',
      from: {
        path: '^(packages|services|apps)/[^/]+/src/',
        pathNot: '\\.(int\\.)?test\\.tsx?$',
      },
      to: { dependencyTypes: ['npm-dev'] },
    },
    {
      name: 'not-to-undeclared-dep',
      severity: 'error',
      comment:
        "Packages declare what they import; resolution walking up to the root's node_modules hides gaps.",
      from: { path: '^(packages|services|apps)/[^/]+/src/' },
      to: { dependencyTypes: ['npm-no-pkg', 'npm-unknown'] },
    },
    {
      name: 'not-to-unresolvable',
      severity: 'error',
      comment: 'Every import must resolve.',
      from: {},
      to: { couldNotResolve: true },
    },
  ],
  options: {
    // No includeOnly: it also filters the target side, which silently hides npm and unresolvable
    // imports from the rules above (caught by the tools/arch-rules fixtures).
    doNotFollow: { path: '/node_modules/' },
    // Never exclude node_modules: excluded modules vanish as dependencies too, so npm-target rules
    // (domain-is-io-free, not-to-dev-dep) would never fire. doNotFollow is enough.
    // Anchored: an unanchored '/dist/' also matches npm packages' dist entry points.
    exclude: {
      path: ['^(packages|services|apps|tools|bench)/[^/]+/dist/', '^tools/arch-rules/fixtures/'],
    },
    tsPreCompilationDeps: true,
    combinedDependencies: false,
    enhancedResolveOptions: {
      conditionNames: ['@obs/source', 'import', 'require', 'node', 'default'],
      exportsFields: ['exports'],
      mainFields: ['module', 'main', 'types'],
      extensions: ['.ts', '.tsx', '.js', '.cjs', '.mjs', '.json'],
    },
  },
};
