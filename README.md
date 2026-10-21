# Analytics pipeline

A small log platform, like a tiny Datadog: apps send logs, and you search them. See
[`docs/design.md`](docs/design.md) for how it works and what gets built next.

Kafka and Elasticsearch run locally; the API and worker come next.

## Setup

You need Node 24 (see `.nvmrc`). Then:

```sh
corepack enable   # once per machine; makes the pinned pnpm version available
pnpm install
```

## Start the services

You need Docker. This starts Kafka on `127.0.0.1:9092` and Elasticsearch on `127.0.0.1:9200`, and
waits until both are ready:

```sh
docker compose up -d --wait
```

Then create the Kafka topics listed in [`config/local.yml`](config/local.yml). Kafka won't create
topics on its own, so the API and worker need this step:

```sh
pnpm topics
```

Check that they're up:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
curl http://127.0.0.1:9200/_cluster/health
```

Stop them with `docker compose down`. Your data is kept. To delete it too, use
`docker compose down -v`.

To add a topic, add it to every file in `config/` (one per environment; the tests check that they
list the same topics), then run `pnpm topics` again. Topics that already exist are left alone.

## Checks

```sh
pnpm verify   # formatting, type checking, and tests: the same checks CI runs on the code
pnpm format   # fix formatting
```
