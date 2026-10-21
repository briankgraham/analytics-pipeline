# Analytics pipeline

A small log platform, like a tiny Datadog: apps send logs, and you search them. See
[`docs/design.md`](docs/design.md) for how it works and what gets built next.

Nothing runs yet; so far only the tooling is set up.

## Setup

You need Node 24 (see `.nvmrc`). Then:

```sh
corepack enable   # once per machine; makes the pinned pnpm version available
pnpm install
```

## Checks

```sh
pnpm verify   # formatting, type checking, and tests: the same checks CI runs on the code
pnpm format   # fix formatting
```
