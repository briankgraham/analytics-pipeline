# Analytics pipeline

A small version of Datadog or Sentry, built to learn how streaming systems work. Apps send logs to an
API, Kafka holds them, a worker saves them to Elasticsearch, and you can search them. What we're
building and in what order is in `docs/design.md`.

## Rules for every session

1. **Never commit to `main`.** Work on a branch, open a PR, and the user decides when to merge.
2. **One PR per session.** Plan it, build it, review it, commit, open the PR, then stop. Aim for one
   commit, but that's a soft rule. It exists to stop changes sprawling, so extra commits are fine when
   they belong to the same piece of work.
3. **Keep each PR small:** about 500 lines at most.
4. **Write tests alongside the code** for any logic that matters. Plain wiring needs no test. All
   tests live in the top-level `tests/` folder, never next to the code.
5. **Get a fresh review before each commit.** A reviewer that didn't write the code (a subagent or
   `/code-review`) checks the diff. Fix what it finds. If a finding seems wrong, tell the user why
   instead of skipping it.
6. **Walk the user through every file** in the PR, in plain words, before opening it.
7. **Use plain names** for files, folders, and functions. A newcomer should be able to guess what's
   inside from the name.
8. **Write middleware as named functions in their own files** (in `api/middleware/`), never as inline
   arrow functions passed to `app.use`. Middleware is for the steps a request passes through on its way;
   a route's last step is its controller.
9. **Build only what the current PR uses.** Folders and helpers appear when something needs them.
10. **Commit titles follow Conventional Commits** (`feat: ...`, `fix: ...`, `docs: ...`). CI checks this.
11. **`pnpm verify` must pass** before committing: formatting, type checking, and tests.
12. **Keep `README.md` accurate** whenever how-to-run steps change.
13. **Every service is a class** that follows `Service` in `services/service.ts`, with `init` for boot-up
    and `close` for shutdown. There is exactly one instance, from a static `getInstance` with a private
    constructor, and calling `init` again never makes a new connection or repeats its work. There are
    two kinds:
    - A **client** owns the connection to one outside system (`ElasticsearchClient`, `KafkaClient`) and
      knows nothing about our data. Our own logic is plain functions built on top of it.
    - A **program** (`ApiServer`, `Worker`) owns everything it needs and starts it in a fixed order:
      clients first, then what uses them, and taking in work last. It remembers the services it started
      and, on close, loops through them newest first and calls each one's own `close`.

    `main.ts` only reads the settings and hands the program to `runProgram`
    (`services/run-program.ts`), the one path for starting and for closing on a signal or an error.

14. **Every route goes route -> controller -> service -> client.** The route file (`api/routes/`) only
    connects a URL to its steps. The controller (`api/controllers/`) handles HTTP and nothing else: it
    reads the request, calls the service, and chooses the reply. The service (`api/services/`) holds
    what the route actually does, and the client talks to the outside system. A route's service is a
    plain function that `server.ts` passes in through `dependencies.ts`, not a `Service` from rule 13,
    which owns a connection or a program.

These rules are kept light on purpose. Stricter checks (coverage targets, linting, git hooks) get
added back one at a time, and only when the user agrees.

## Tools

TypeScript (strict) on Node 24 (`.nvmrc`), pnpm through `corepack`, Prettier, Vitest. CI is
`.github/workflows/ci.yml`. `main` only accepts PRs whose checks named `verify` and `commit-messages`
pass, so keep those job names.

## Folder plan

Folders are added by the PR that first needs them. Folders are named for what they are about
(`logs`, `kafka`, `api`), not for how they are used (no `shared` or `utils`).

```
api/              HTTP server: receives logs, answers searches; server.ts starts and closes it all, app.ts puts middleware and routes in order
api/routes/       one file per URL the API answers, in a folder per version (v1/); all-routes.ts lists them
api/controllers/  the last step of each route: reads the request, calls the service, chooses the reply; one file per route
api/services/     what each controller calls to do the route's work, as plain functions; not the same as the top-level services/ folder
api/build-search-criteria.ts  checks a search URL's values and turns them into criteria for the search service
api/dependencies.ts  what the routes need from outside (like sending a log); server.ts fills it in
api/middleware/   the steps a request passes through before its controller, one function per file, named for what it does
worker/           reads logs from Kafka and saves them to Elasticsearch; worker.ts starts and closes it all
scripts/          small helper scripts, such as sending a test log
logs/             what a log is (its shape, and checking one), used by both the API and the worker
tests/            every test, in folders that match the code folders (tests/api tests the api folder)
config/           settings for each environment: local (in use), sandbox, stage, prod; read-config.ts has checks the readers share
kafka/            client.ts is the base layer that talks to Kafka, sending and reading; send-failed-logs.ts builds on it; create-topics.ts creates topics
services/         what every Service class follows: one instance of a class, with init and close; run-program.ts is the one path for starting and closing
elasticsearch/    client.ts is the base layer that talks to Elasticsearch; logs-index.ts, save-logs.ts and build-logs-query.ts build on it
elasticsearch/search-criteria-builder/  turns a list of search criteria into a query; knows nothing about logs and imports nothing from outside this folder, so it could become its own library
retries/          trying a failed call again, with waits between tries
docker-compose.yml  runs Kafka and Elasticsearch locally
docs/design.md    what we're building, short
```

## Earlier work

A first, much larger attempt is saved at git tag `archive/phase-1-v1`. Treat it as a parts bin to
copy from when a piece is needed, not as a plan to follow.
