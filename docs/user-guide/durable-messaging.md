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
| `@subscribe` | `durable=True` | subscribe via an AdvancedSubscriber with history + recovery |
| | `replay=1000` | max historical samples to replay on join |
| | `recover=True` | re-fetch samples missed during transient disconnects |
| | `on_miss=None` | `on_miss(source, nb)` called on an **unrecoverable** gap |

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
  producer and `nb` the number of samples irrecoverably missed. This is the honest edge
  of at-least-once: recovered silently when possible, surfaced loudly when not. An async
  `on_miss` is awaited; a throwing one is logged and swallowed (it never breaks delivery).

## Guarantees — and honest limits

What you get: **at-least-once within the retained window**, and **effectively-once**
when combined with idempotent processing (see [Storage](storage.md)).

Be clear-eyed about the trade-offs versus a broker:

- **Replay is bounded by the retained window** — `cache`/`replay` size (and, if you
  add a persistent Zenoh storage, its retention). Older messages fall off.
- **Someone must be holding the data when a consumer recovers.** With the in-memory
  cache that's the producer; if the producer restarts, its cache is gone. For replay
  that survives a producer restart, run one or more **persistent Zenoh storages** on
  some peers (RocksDB/InfluxDB/S3 backends). These are ordinary peers that persist —
  **not** a central broker, and you can replicate them for redundancy.
- **No broker-committed consumer-group offsets.** Progress is tracked subscriber-side
  via sequence numbers; this is closer to "durable streams" than to Kafka consumer
  groups with server-side ack/redelivery.

## Two layers of durability

Durable pub/sub handles **delivery/replay**. Idempotency handles **processing**.
Use both for end-to-end effectively-once:

| Concern | Mechanism |
|---|---|
| durable delivery / replay | `durable=True` (Zenoh advanced pub/sub + optional storage peer) |
| effectively-once processing | idempotency / inbox in the `StoragePlugin` ledger |

The transport makes sure the message *arrives* (even late); the ledger makes sure
you don't *act on it twice*. Neither replaces the other — exactly as in a Kafka
deployment, where apps still keep their own idempotency tables.

## Next Steps

- [Security & TLS](security.md)
- [Storage](storage.md) — Redis / SQLAlchemy idempotency ledger
- [Recipe: Durable orders](../recipes/durable-orders.md)
