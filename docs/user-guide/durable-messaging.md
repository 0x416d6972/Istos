---
title: Brokerless Durable Messaging
---

# Brokerless Durable Messaging

Istos exists to give you **durable** messaging **without a broker** — so
microservices and SLM/AI agents can talk to each other reliably without running
and babysitting a Kafka/RabbitMQ/NATS cluster. Durability comes from the *peers*,
not from a central log.

## The idea: the producer is its own log

A broker is a central durable log. Istos deletes it and distributes durability to
the peers instead:

- The **producer** keeps a bounded cache of what it published — its own replay log —
  and heartbeats a sequence number.
- A **subscriber** that joins late (or reconnects after a blip) **replays history**
  and **recovers missed samples** by querying the producer **peer-to-peer**.

```
   PRODUCER peer                       CONSUMER peer
   ┌────────────────┐   live sample   ┌────────────────────┐
   │ AdvancedPublisher ─────────────► │ AdvancedSubscriber  │
   │  · replay cache │                │  · replays history  │
   │  · seq+heartbeat│  ◄─ query ──── │  · detects a gap    │
   │ (its own log)   │ ── replies ──► │  · recovers missed  │
   └────────────────┘                 └────────────────────┘
```

This is built on Zenoh's advanced publisher/subscriber (`zenoh.ext`) — no extra
infrastructure, no broker.

## Usage

Add `durable=True` to `@publish` and `@subscribe`. That's it.

```python
from istos import Istos

app = Istos()

# Producer: retains the last 1000 messages as a replay log.
@app.publish("orders/created", durable=True, cache=1000)
async def created(order: dict):
    return order

# Consumer: replays history on join, recovers missed messages after a blip.
# on_miss fires only when a gap could NOT be recovered.
@app.subscribe("orders/created", durable=True, replay=1000,
               on_miss=lambda source, nb: alert(f"lost {nb} from {source}"))
async def on_created(event: dict):
    await process(event)
```

A subscriber that starts **after** messages were published still receives them —
they are replayed from the producer's cache, peer-to-peer.

### Options

| Decorator | Option | Meaning |
|---|---|---|
| `@publish` | `durable=True` | publish through an AdvancedPublisher with a replay cache |
| | `cache=1000` | how many recent samples the producer retains for replay |
| | `heartbeat=1.0` | seconds between sequence-number heartbeats (gap detection) |
| | `reliability=None` | override link reliability (default `RELIABLE`) |
| | `congestion_control=None` | override backpressure policy (default `BLOCK`) |
| | `persist=None` | persist the stream to object storage (`"s3://…"`) so it survives a producer restart — see [below](#surviving-a-producer-restart-persist-to-object-storage) |
| `@subscribe` | `durable=True` | subscribe via an AdvancedSubscriber with history + recovery |
| | `replay=1000` | max historical samples to replay on join |
| | `recover=True` | re-fetch samples missed during transient disconnects |
| | `on_miss=None` | `on_miss(source, nb)` called on an **unrecoverable** gap |
| | `replay_persisted=False` | on join, replay the stream from a `persist=` object store (survives producer crash) |

`durable=True` cannot be combined with `use_shm=True` (durable publishing manages
its own buffers).

### No silent drops, and a loud signal when delivery fails

Two things make `durable=True` more than a replay cache:

- **The producer won't silently drop.** Durable publishers default to
  `reliability=RELIABLE` and `congestion_control=BLOCK`. Zenoh's normal default is
  to *drop* under backpressure; `BLOCK` instead slows the producer so samples still
  reach the replay cache and the wire. (A blocked `publish()` applies backpressure
  to your producer — the correct trade for durability. Override per-publisher if you
  truly prefer dropping.)
- **You hear about losses you can't recover.** If history/recovery can't fill a gap,
  the subscriber logs a warning and calls your `on_miss(source, nb)` — `source` is the
  producer and `nb` the number of samples irrecoverably missed. This is the edge
  of at-least-once: recovered when possible, surfaced when not. An async
  `on_miss` is awaited; a throwing one is logged and swallowed (it never breaks delivery).

## Guarantees — and honest limits

What you get: **at-least-once within the retained window**, and **effectively-once**
when combined with idempotent processing (see [Storage](storage.md)).

Be clear-eyed about the trade-offs versus a broker:

- **Replay is bounded by the retained window** — `cache`/`replay` size (and, if you
  add a persistent Zenoh storage, its retention). Older messages fall off.
- **Someone must be holding the data when a consumer recovers.** With the in-memory
  cache that's the producer; if the producer restarts, its cache is gone. For replay
  that survives a producer restart you have two options: run a native
  **persistent Zenoh storage** (RocksDB/InfluxDB/S3 backends) — but those are
  compiled plugins that only load inside a `zenohd` **router** process — or use
  Istos's own brokerless persistence (`persist="s3://…"`, below), which needs no
  router and no native plugin. See
  [Surviving a producer restart](#surviving-a-producer-restart-persist-to-object-storage).
- **No broker-committed consumer-group offsets.** Progress is tracked subscriber-side
  via sequence numbers; this is closer to "durable streams" than to Kafka consumer
  groups with server-side ack/redelivery.

## Surviving a producer restart: persist to object storage

The replay cache lives in the **producer's RAM**, so it dies with the producer.
To make a stream survive the producer itself — brokerless, with no `zenohd`
router and no native Zenoh storage plugin — Istos can play the storage role in
Python. Pass `persist="s3://bucket/prefix"` to a durable publisher:

```python
# Producer: also persist every sample to object storage.
@app.publish("orders/created", durable=True, persist="s3://orders-log")
async def created(order: dict):
    return order

# Consumer: on join, replay the persisted stream from the queryable — works
# even if the original producer has since crashed.
@app.subscribe("orders/created", durable=True, replay_persisted=True)
async def on_created(event: dict):
    await process(event)   # make me idempotent
```

That co-locates a **persistence role** (`Istos.persist`) alongside the publisher:

- a **writer** subscribes to the key expression and writes *every* sample to the
  object store (each sample under its own minted key, so the whole **stream** is
  kept — not a last-value-wins snapshot), and
- a **history queryable** answers `session.get("orders/created/**")` by replaying
  the stored samples — so a consumer can fetch history even after the original
  producer has crashed, as long as *some* Istos process hosts the role.

On the consumer side, `@subscribe(replay_persisted=True)` issues that wildcard
history query on join and delivers the recovered samples through the normal
callback pipeline. Replayed samples come from the trusted store, so they skip the
authorizer gate and carry no token. Recovery is best-effort at-least-once — it may
interleave with live samples and overlap a durable subscriber's producer-cache
replay, so **keep the callback idempotent**.

```
   PRODUCER (or standalone node)              CONSUMER
   ┌──────────────────────────┐   get(**)   ┌──────────────┐
   │ publish ─► writer ─► S3   │ ◄────────── │ recover /    │
   │           queryable ◄─────┼── replies ─►│ late join    │
   └──────────────────────────┘  (from S3)  └──────────────┘
```

### Standalone persistence node

Because the role is just a writer + queryable, you can run it in a **dedicated
Istos process** that has no publishers of its own — the brokerless equivalent of a
storage node. It keeps serving history even while producers come and go:

```python
app = Istos()
app.persist("orders/created", "s3://orders-log")   # URL or an ObjectStore instance
app.run()
```

`persist=` accepts an `s3://…` URL (S3/MinIO, via the `istos[s3]` extra),
`memory://` for tests, or a ready
[`ObjectStore`](../api/communication/persist.md) instance if you want a custom
backend. Persistence never crashes the producer: a failing store write is logged
and swallowed.

### Configuring S3 / MinIO

Install the extra: `pip install "istos[s3]"`.

**Credentials** use the standard AWS chain — set them out-of-band, never in the
URL:

```bash
export AWS_ACCESS_KEY_ID=…
export AWS_SECRET_ACCESS_KEY=…
# or ~/.aws/credentials, or an instance/IAM role in production
```

**Bucket, prefix, endpoint, and region** come from the URL. A custom `endpoint`
(for MinIO or an S3-compatible store) and `region` go in the query string:

```python
# AWS, default endpoint:
@app.publish("orders/created", durable=True,
             persist="s3://orders-log/prod?region=us-east-1")
async def created(order): ...

# MinIO:
app.persist("orders/created", "s3://orders-log?endpoint=http://localhost:9000&region=us-east-1")
```

For full control (or dev credentials for MinIO) construct the store directly and
pass it in:

```python
from istos import S3ObjectStore

store = S3ObjectStore(
    "orders-log", prefix="prod",
    endpoint_url="http://localhost:9000",
    access_key_id="minioadmin", secret_access_key="minioadmin",
)
app.persist("orders/created", store)
```

!!! note "Consumers query the wildcard"
    Persisted history is served under per-sample keys, so fetch it with a wildcard
    selector (`orders/created/**`), not the bare key — a plain `get("orders/created")`
    would let Zenoh's reply consolidation collapse the stream to a single value.

## Two layers of durability

Durable pub/sub handles **delivery/replay**. Idempotency handles **processing**.
Use both for end-to-end effectively-once:

| Concern | Mechanism |
|---|---|
| durable delivery / replay | `durable=True` (Zenoh advanced pub/sub) + optional `persist="s3://…"` for producer-crash durability |
| effectively-once processing | idempotency / inbox in the `StoragePlugin` ledger |

The transport makes sure the message *arrives* (even late); the ledger makes sure
you don't *act on it twice*. Neither replaces the other — exactly as in a Kafka
deployment, where apps still keep their own idempotency tables.

## Next Steps

- [Security & TLS](security.md)
- [Storage](storage.md) — Redis / SQLAlchemy idempotency ledger
- [Recipe: Durable orders](../recipes/durable-orders.md)
