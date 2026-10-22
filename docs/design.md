# Design

## What this is

A small log platform, like a tiny Datadog. Apps send their logs here, and you can search them. Later
it also counts logs per minute and shows the counts on a live dashboard.

The point is to learn how streaming systems behave, so we build the moving parts ourselves instead of
using a framework that hides them.

## How a log moves through the system

```
app or script  --HTTP-->  api  -->  Kafka  -->  worker  -->  Elasticsearch
                                                                  ^
you  --------------------------- search request --> api ----------+
```

1. **An app sends a log** to the API over HTTP.
2. **The API checks the log** and puts it into Kafka. It replies "accepted" only after Kafka has
   stored it, so an accepted log is safely stored.
3. **Kafka holds the logs** until the worker reads them. If the worker is slow or down, logs
   wait in Kafka instead of being dropped.
4. **The worker reads logs from Kafka** and saves them to Elasticsearch.
5. **You search** by asking the API, which asks Elasticsearch.

Kafka keeps logs in named streams called topics. Every topic is listed, with what it's for, in the
settings for each environment in `config/` (`local.yml` is the only one in use). Kafka won't create
a topic that isn't listed.

Why Kafka sits in the middle: the API stays fast even when Elasticsearch is slow, and a crashed worker
can pick up where it left off.

## A log

The first version keeps a log as small as possible:

| Field     | Example         | Meaning                             |
| --------- | --------------- | ----------------------------------- |
| `service` | `checkout`      | which app sent it                   |
| `level`   | `error`         | `debug`, `info`, `warn`, or `error` |
| `message` | `card declined` | the text you search                 |

The shape lives in `logs/log.ts` so the API and the worker agree on it. We'll add fields (time,
id, extra attributes) when a feature needs them.

## First goal

Send one log with a script, search for a word in it, and get it back. Nothing else.

## Build order

One small PR at a time:

1. **Basics:** these docs, the tooling (TypeScript, Prettier, Vitest), and CI.
2. **Local services:** `docker-compose.yml` that runs Kafka and Elasticsearch, and creates the Kafka
   topics listed in `config/local.yml`.
3. **Receive logs:** `POST /v1/message` in the API (the `/v1` is the API version) checks a log and puts it into Kafka.
4. **Save logs:** the worker reads from Kafka and saves to Elasticsearch. A log counts as done in
   Kafka only after it's saved, so a crash means it's read again, never lost (rarely, saved twice).
   A message that isn't a good log is skipped with a warning.
5. **Search:** `GET /search?q=word` in the API, plus a script that sends a test log. This is the first
   full demo.

## Later, planned once the first goal works

- Counting logs per minute, and error rates.
- A live dashboard.
- Hard cases: logs that arrive late, logs sent twice, and bad input.
- Alerts when error rates are high.
