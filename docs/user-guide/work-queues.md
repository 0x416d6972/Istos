---
title: Work Queues
---

# Work Queues

Pub/sub and work queues look similar until something fails. Publishing an event
fans it out to every subscriber; a job is different — you want *one* worker to do
it, exactly once if you can manage it, and you want it retried if the worker dies
halfway through. "Send this email" is a job. "The order was created" is an event.
They are not the same thing, and treating a job like an event loses work the first
time a process crashes.

A work queue gives a job things a topic doesn't:

- **Single delivery.** A job goes to one worker, not all of them.
- **Acknowledgement.** The job isn't finished until the worker says so. If it
  never says so, the job comes back.
- **Backed-off retries.** A failing job is retried with exponential backoff, so a
  broken dependency isn't hammered a thousand times a second.
- **A dead end for poison jobs.** A job that keeps failing eventually stops
  retrying and lands somewhere you can look at it, instead of looping forever.
- **Delays, priorities, results and schedules** — run this in five minutes, run
  that one first, tell me what it returned, run this every night.

## Where the state lives

A broker-based queue keeps all of this on the broker. Istos has no broker, so one
node plays the **owner**: it holds the jobs, hands them out one at a time, tracks
which ones are leased, and reclaims the ones whose worker went quiet. Workers live
anywhere on the mesh and claim from the owner over Zenoh. Because every claim goes
through that single owner, two workers never get the same job.

```
        enqueue                 claim / ack / nack
  producer ─────► ┌───────────────┐ ◄───────── worker A
                  │  QUEUE OWNER   │ ◄───────── worker B
  producer ─────► │  · jobs        │ ◄───────── worker C
                  │  · leases      │
                  │  · dead-letter │  (sweeper reclaims dead leases)
                  └───────────────┘
```

The owner is still just an Istos process — not a broker you operate separately.
Run it on its own for a dedicated queue node, or alongside the producer.

## The three roles

```python
from istos import Istos

app = Istos()

# 1. Own the queue (this node holds the jobs).
app.queue("jobs/email", lease_s=30, max_attempts=5)

# 2. Work the queue (any node). Return to ack, raise to retry.
@app.worker("jobs/email", concurrency=4)
async def send(job):
    await smtp.send(job["to"], job["body"])

# 3. Put jobs on it (any node).
await app.enqueue("jobs/email", {"to": "a@b.com", "body": "hi"})
```

That's the whole surface. A worker that **returns** acks the job — it's done and
gone. A worker that **raises** nacks it — the job goes back on the queue and is
handed out again, until it has been tried `max_attempts` times, at which point it
is dead-lettered instead.

`Depends(...)` parameters on a worker are injected exactly as on a handler, so a
worker can pull in a database session or config the same way.

## Leases: what happens when a worker dies

When a worker claims a job it gets a **lease** for `lease_s` seconds. The lease is
a promise: "I'll have this acked or nacked within this window." If the worker
finishes, it acks and the job is deleted. If the worker *crashes* — no ack, no
nack, nothing — the lease simply expires. The owner's sweeper notices the expired
lease and puts the job back, so another worker can pick it up.

This is why processing should be **idempotent** where it matters. A crash after
the side effect but before the ack means the job runs again. That's the honest
shape of at-least-once delivery: you never silently lose a job, but you can
occasionally see one twice, so make "send the email" safe to run twice (or key it
against the [idempotency ledger](storage.md)).

Pick `lease_s` a little longer than your slowest reasonable job. Too short and a
slow-but-healthy worker gets its job yanked away and double-processed; too long
and a crashed worker's job sits idle until the lease finally expires.

## Retries, backoff, and the dead-letter list

Each delivery increments the job's attempt count. A worker that raises sends the
job back to `ready` — but not instantly. Istos waits an **exponential backoff**
before the job is eligible again: `retry_backoff_s`, then double, then double
again, capped at `retry_backoff_max_s`, with a little jitter so a batch of
simultaneous failures doesn't retry in lockstep. A failing dependency gets a
breather instead of a tight retry loop.

```python
app.queue("jobs/email",
          max_attempts=5,
          retry_backoff_s=1.0,       # 1s, 2s, 4s, 8s, … between attempts
          retry_backoff_max_s=600.0) # never wait more than 10 minutes
```

Once a job has used all `max_attempts` it is marked **dead** and set aside. Dead
jobs are never handed out again; they wait for you to look at them:

```python
for job in await app.dead_letters("jobs/email"):
    log.error("gave up on %s after %d tries: %s",
              job["job_id"], job["attempts"], job["last_error"])
    # inspect job["data"], fix the cause, re-enqueue if you want to
    await app.enqueue("jobs/email", job["data"])
```

`dead_letters()` gives you the decoded job body, its id, how many times it was
tried, and the last error string. Re-enqueueing is a plain `enqueue` — the queue
has no magic "requeue the dead" button on purpose; moving a poison job back onto
the queue is a decision you should make deliberately.

## Knowing which attempt you are on

By default a worker sees the job body and nothing else, which is usually right: the
job says what to do, and the queue's business is its own. But a retry is not the
same event as a first delivery, and sometimes the work needs to know. Name a `ctx`
parameter and the queue hands you the delivery's context, the same way naming `db`
hands a handler the app's storage:

```python
from istos import JobContext

@app.worker("jobs/email")
async def send(job, ctx: JobContext):
    if ctx.is_last_attempt:
        log.warning("final try for %s; it failed last time with: %s",
                    ctx.job_id, ctx.last_error)
    await smtp.send(job["to"], job["body"])
```

`ctx` carries `job_id`, `queue`, `attempt` (1 on the first delivery), `max_attempts`
and `last_error` — the error string from the attempt before, or `None` on the first
one. Two predicates cover what you normally want to ask: `is_retry` (something
already failed at this) and `is_last_attempt` (raising now dead-letters it rather
than trying again).

That last one is the useful one. It is the difference between a job that vanishes
into the dead-letter list and one that files a ticket on its way out:

```python
@app.worker("jobs/charge")
async def charge(job, ctx: JobContext):
    try:
        await payments.charge(job["order_id"], job["amount"])
    except PaymentError:
        if ctx.is_last_attempt:
            await alert_ops(job["order_id"], ctx.last_error)
        raise
```

`last_error` is what makes a retry smarter than a repeat: the job comes back with
the reason it bounced, so attempt 2 can do something different instead of the same
thing again. It travels with the job, so it works across competing consumers — a
redelivery that lands on a different process still knows what happened.

The parameter is opt-in and additive. A worker that never mentions `ctx` behaves
exactly as before, and a `ctx` that is a `Depends(...)` still resolves as your
dependency — the resolver checks for one first.

## Competing consumers: scaling out

Two ways to add throughput, and you can combine them:

- **`concurrency=N`** runs N claim loops in one process — good for I/O-bound jobs
  that spend most of their time waiting.
- **More processes** running the same `@app.worker` are independent competing
  consumers. Start ten copies of your worker service and the one owner spreads the
  jobs across all of them.

Either way the owner is the arbiter, so no coordination between workers is needed
and no job is claimed twice at the same time.

```python
@app.worker("jobs/resize", concurrency=8)   # 8 in-process loops...
async def resize(job):
    await make_thumbnail(job["path"])
```

Workers don't busy-poll. The owner sends a lightweight nudge when a job arrives,
so an idle worker wakes and claims immediately instead of waiting for its next
tick. The `poll_interval_s` is a safety net — it catches redelivered jobs and jobs
whose delay has just elapsed, not the normal hot path.

## Delaying and prioritizing jobs

`enqueue` takes two knobs. `delay_s` holds a job back until that many seconds have
passed — a job with an ETA. `priority` (higher first) lets urgent work jump the
line ahead of lower-priority jobs already waiting; within the same priority the
queue stays FIFO.

```python
await app.enqueue("jobs/email", welcome, priority=10)          # send this first
await app.enqueue("jobs/email", digest, delay_s=3600)          # …and this in an hour
```

## Getting a result back

By default a queue is fire-and-forget — enqueue, and the job runs somewhere. Turn
on `keep_results` and the worker's return value is retained so the producer (or
anyone) can fetch it by job id:

```python
app.queue("jobs/render", keep_results=True, result_ttl_s=3600)

@app.worker("jobs/render")
async def render(job):
    return {"url": await rasterize(job["doc"])}   # returned value is the result

job_id = await app.enqueue("jobs/render", {"doc": "…"})
# …later, from anywhere on the mesh:
outcome = await app.result("jobs/render", job_id)
# {"state": "done", "result": {"url": "…"}}
```

`result()` reports `state` — `ready`/`leased` while in flight, `done` when
finished, `dead` if it was dead-lettered, `unknown` once the record ages out after
`result_ttl_s`. Results cost memory (and storage), so they're off unless you ask.

## Periodic jobs — intervals and cron

`schedule` is the "beat" side of the system. Give it a fixed interval or a cron
expression:

```python
app.queue("jobs/report")
app.schedule("jobs/report", {"kind": "hourly"}, every_s=3600)
app.schedule("jobs/report", {"kind": "nightly"}, cron="0 2 * * *")   # 02:00 daily
```

Cron uses the five standard fields (minute, hour, day-of-month, month,
day-of-week) with `*`, ranges (`1-5`), steps (`*/15`) and lists (`1,15,30`); when
both day-of-month and day-of-week are set the match is the union, as in Vixie
cron. It ticks on the node that declares it — run a given schedule on **one** node
so you don't get duplicate ticks from every replica.

## Workflows: chains, groups and chords

For multi-step work, three composition helpers sit on top of the queue.

**Chain** runs queues in sequence, piping each step's return into the next — a
pipeline:

```python
# fetch(url) → parse(<fetch result>) → store(<parse result>)
await app.chain(["jobs/fetch", "jobs/parse", "jobs/store"], url)
```

**Group** fans a batch onto one queue in parallel and hands back the job ids:

```python
ids = await app.group("jobs/thumbnail", [img1, img2, img3, img4])
```

**Chord** is a group with a finish line: when **all** the members succeed, a
callback fires once with their results collected in order:

```python
# run every shard, then reduce the results
await app.chord("jobs/shard", shards, callback=("jobs/reduce", {"job": "nightly"}))
# jobs/reduce receives {"results": [...], "input": {"job": "nightly"}}
```

The group's queue owner is the barrier, so a chord's members share one queue.
Continuations are enqueued before the current step is acked, so a crash redelivers
the whole step — keep steps idempotent, as everywhere else in the queue. A member
that exhausts its retries and dead-letters will stall its chord; fix and re-enqueue
it to let the chord finish.

## Durability

The owner keeps the authoritative queue in memory and **writes every change
through to the app's storage**. With the default in-memory storage that means the
queue is fast but volatile — restart the owner and the jobs are gone. Point the
app at a durable [`StoragePlugin`](storage.md) (Redis or SQLAlchemy) and the queue
survives an owner restart: on startup the owner reloads its jobs, leases and all,
and the sweeper reclaims anything that was mid-flight.

```python
from istos import Istos, RedisStoragePlugin

app = Istos(storage=RedisStoragePlugin(url="redis://localhost:6379/0"))
app.queue("jobs/email")   # now durable across owner restarts
```

Nothing else changes — the same `queue` / `worker` / `enqueue` code becomes
durable purely by giving the app durable storage.

The queue is built to scale: claiming, enqueueing, acking and nacking are all
`O(log n)` in the number of jobs (a set of internal heaps), with no scans of the
whole queue on the hot path — a deep backlog doesn't slow down the next claim.

## High availability: owner failover

One owner is a single point for its queue. With `ha=True` you run several owner
replicas that elect a single leader over Zenoh liveliness; the leader binds the
queue and the others stand by. If the leader dies, its liveliness token drops, a
standby is elected in its place, and it recovers the jobs and keeps serving:

```python
# on every replica — same queue, same shared storage
app = Istos(storage=RedisStoragePlugin(url="redis://…"))
app.queue("jobs/email", ha=True)
```

HA needs **shared** storage (Redis/SQLAlchemy) so the new leader can recover the
in-flight jobs — with the in-memory default each replica is an island. Election is
leaderless-recovery, not consensus: during the brief handover window two owners
can momentarily overlap, which (like the rest of the queue) is at-least-once, not
exactly-once. Producers and workers don't change — they reach whichever replica is
currently the leader.

## Guarantees, plainly

- **At-least-once**, not exactly-once. A job is delivered until it's acked;
  a crash between side effect and ack redelivers it. Keep workers idempotent.
- **One at a time.** A single owner serializes claims, so competing workers never
  hold the same job concurrently.
- **Bounded retries.** A failing job is retried up to `max_attempts`, then
  dead-lettered — it can't spin forever.
- **The owner tracks ack state**, because someone has to. Give it durable storage
  so a restart doesn't lose the queue, and run `ha=True` replicas so a crash fails
  over to a standby instead of taking the queue down.

## When to reach for pub/sub instead

If you want *every* interested party to see a message, that's an event — use
[`@publish` / `@subscribe`](pubsub.md), optionally
[durable](durable-messaging.md) so late joiners catch up. Reach for a work queue
when a message is a unit of work that exactly one worker should do, and doing it
twice (or losing it) actually matters.

## Next Steps

- [Storage](storage.md) — the durable backend that makes a queue survive restarts
- [Brokerless Durable Messaging](durable-messaging.md) — durable pub/sub, the other half
- [Dependency Injection](dependency-injection.md) — inject a db/session into a worker
