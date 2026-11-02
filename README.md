# Analytics pipeline

A small log platform, like a tiny Datadog: apps send logs, and you search them. See
[`docs/design.md`](docs/design.md) for how it works and what gets built next.

Kafka and Elasticsearch run locally. The API accepts logs and puts them in Kafka, and the worker
saves them from Kafka into Elasticsearch. Search comes next.

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

## Run the API

With the services running and the topics created (above):

```sh
pnpm api
```

It listens on `http://127.0.0.1:3000` (the port is in [`config/local.yml`](config/local.yml)). Send it a
log:

```sh
curl -X POST http://127.0.0.1:3000/v1/message \
  -H 'content-type: application/json' \
  -d '{"service":"checkout","level":"error","message":"card declined"}'
```

The URL starts with `/v1` so the API can change later without breaking senders that already use it.

`202` means Kafka has stored the log. `400` means the log is wrong (the reply says why), and `503`
means Kafka couldn't store it right now, so try again. If Kafka is down the `503` takes about 10
seconds, because the API retries first. To see the log in Kafka:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:9092 --topic logs --from-beginning --timeout-ms 4000
```

On Node 24 the API and the worker each print a one-time `TimeoutNegativeWarning` at start-up. It's a
harmless bug inside kafkajs.

## Run the worker

With the services running and the topics created, in a second terminal:

```sh
pnpm worker
```

It reads logs from Kafka and saves them into the `logs` index in Elasticsearch, creating the index
the first time. The first time it runs it starts with the oldest log in Kafka, and after that it
carries on where it stopped, even after Ctrl+C or a crash (after a crash, the last few logs may be
saved twice). A message in Kafka that isn't a good log
is skipped with a warning saying where it is. If Elasticsearch is down, the worker keeps retrying
the same log until it can save it.

To see the logs it saved (a word search on `message`, straight to Elasticsearch):

```sh
curl 'http://127.0.0.1:9200/logs/_search?q=message:card&pretty'
```

## Checks

```sh
pnpm verify   # formatting, type checking, and tests: the same checks CI runs on the code
pnpm format   # fix formatting
```
