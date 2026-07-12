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

A work queue gives a job three things a topic doesn't:

- **Single delivery.** A job goes to one worker, not all of them.
- **Acknowledgement.** The job isn't finished until the worker says so. If it
  never says so, the job comes back.
- **A dead end for poison jobs.** A job that keeps failing eventually stops
  retrying and lands somewhere you can look at it, instead of looping forever.

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

## Retries and the dead-letter list

Each delivery increments the job's attempt count. A worker that raises sends the
job back to `ready` — unless it has already used all `max_attempts`, in which case
it is marked **dead** and set aside. Dead jobs are never handed out again; they
wait for you to look at them:

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

## Guarantees, plainly

- **At-least-once**, not exactly-once. A job is delivered until it's acked;
  a crash between side effect and ack redelivers it. Keep workers idempotent.
- **One at a time.** A single owner serializes claims, so competing workers never
  hold the same job concurrently.
- **Bounded retries.** A failing job is retried up to `max_attempts`, then
  dead-lettered — it can't spin forever.
- **The owner is a single point for its queue.** That's inherent to work-queue
  semantics — someone has to track ack state. Give it durable storage so a restart
  doesn't lose the queue, and run it where you'd run any stateful service.

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
