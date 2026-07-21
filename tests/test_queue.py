"""Work queues: ack, lease-based redelivery, dead-letter, competing consumers.

The QueueStore tests are pure (no network); the @app.worker / enqueue tests run
over real loopback Zenoh (owner queryables + worker claim loops)."""

import asyncio

import pytest

from istos import Depends, Istos, JobContext, JobState
from istos.consistency.storage import InMemoryStoragePlugin
from istos.queue import QueueRole, QueueStore


def _app() -> Istos:
    return Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


# ---------------------------------------------------------------------------
# QueueStore — the ack/lease/dead-letter state machine, no transport
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_store_fifo_claim_and_ack():
    s = QueueStore("q")
    id1 = await s.add(b"a", max_attempts=3)
    id2 = await s.add(b"b", max_attempts=3)

    r1 = await s.claim(lease_s=30)
    assert r1 is not None and r1.id == id1 and r1.data == b"a" and r1.attempts == 1
    r2 = await s.claim(lease_s=30)
    assert r2 is not None and r2.id == id2
    assert await s.claim(lease_s=30) is None  # both leased, none left

    assert await s.ack(id1) is True
    stats = await s.stats()
    assert stats[JobState.READY.value] == 0
    assert stats[JobState.LEASED.value] == 1


@pytest.mark.asyncio
async def test_store_nack_redelivers_then_dead_letters():
    s = QueueStore("q")
    jid = await s.add(b"x", max_attempts=2)

    r = await s.claim(lease_s=30)
    assert r is not None and r.attempts == 1
    assert await s.nack(jid, error="boom") == "requeued"

    r = await s.claim(lease_s=30)
    assert r is not None and r.attempts == 2
    assert await s.nack(jid, error="boom") == "dead"

    dead = await s.dead_letters()
    assert len(dead) == 1 and dead[0].id == jid and dead[0].last_error == "boom"
    assert await s.claim(lease_s=30) is None  # dead jobs are not claimable


@pytest.mark.asyncio
async def test_store_expired_lease_reclaimed_by_sweep():
    s = QueueStore("q")
    await s.add(b"x", max_attempts=5)

    r = await s.claim(lease_s=0.0)  # lease expires immediately
    assert r is not None and r.attempts == 1
    await asyncio.sleep(0.01)

    assert await s.sweep() == 1  # crashed worker's job is reclaimed
    again = await s.claim(lease_s=30)
    assert again is not None and again.attempts == 2  # redelivered


@pytest.mark.asyncio
async def test_store_writes_through_and_recovers():
    storage = InMemoryStoragePlugin()
    s = QueueStore("q", storage)
    await s.add(b"a", max_attempts=3)
    jid = await s.add(b"b", max_attempts=3)
    await s.ack(jid)  # acked jobs must not come back

    recovered = QueueStore("q", storage)
    await recovered.load()
    stats = await recovered.stats()
    assert stats[JobState.READY.value] == 1
    r = await recovered.claim(lease_s=30)
    assert r is not None and r.data == b"a"


@pytest.mark.asyncio
async def test_store_priority_then_fifo():
    s = QueueStore("q")
    a = await s.add(b"a", max_attempts=3, priority=0)
    await s.add(b"b", max_attempts=3, priority=10)   # higher priority
    await s.add(b"c", max_attempts=3, priority=10)   # same priority, later

    r = await s.claim(lease_s=30)
    assert r is not None and r.data == b"b"   # highest priority first
    r = await s.claim(lease_s=30)
    assert r is not None and r.data == b"c"   # FIFO within a priority
    r = await s.claim(lease_s=30)
    assert r is not None and r.id == a        # then the low-priority one


@pytest.mark.asyncio
async def test_store_delayed_job_is_not_claimable_yet():
    s = QueueStore("q")
    await s.add(b"soon", max_attempts=3, delay_s=100)
    assert await s.claim(lease_s=30) is None   # held back by not_before

    now_id = await s.add(b"now", max_attempts=3)
    r = await s.claim(lease_s=30)
    assert r is not None and r.id == now_id    # the immediate one is claimable


@pytest.mark.asyncio
async def test_store_keeps_result():
    s = QueueStore("q", keep_results=True)
    jid = await s.add(b"x", max_attempts=3)
    await s.claim(lease_s=30)
    assert await s.ack(jid, result=b"42") is True

    state, data = await s.result(jid)
    assert state == JobState.DONE.value and data == b"42"
    stats = await s.stats()
    assert stats[JobState.DONE.value] == 1
    assert await s.claim(lease_s=30) is None   # a done job is not re-claimed


@pytest.mark.asyncio
async def test_store_scales_and_stays_ordered():
    s = QueueStore("q")
    # 600 jobs across three priority bands, enqueued interleaved.
    for i in range(200):
        await s.add(f"lo{i}".encode(), max_attempts=1, priority=0)
        await s.add(f"hi{i}".encode(), max_attempts=1, priority=10)
        await s.add(f"mid{i}".encode(), max_attempts=1, priority=5)

    drained = []
    while True:
        r = await s.claim(lease_s=30)
        if r is None:
            break
        drained.append(r.data)
        await s.ack(r.id)

    assert len(drained) == 600
    # Highest priority first, FIFO within a band.
    assert drained[:200] == [f"hi{i}".encode() for i in range(200)]
    assert drained[200:400] == [f"mid{i}".encode() for i in range(200)]
    assert drained[400:] == [f"lo{i}".encode() for i in range(200)]
    stats = await s.stats()
    assert all(v == 0 for v in stats.values())  # nothing leaked


@pytest.mark.asyncio
async def test_store_chord_barrier_fires_once():
    s = QueueStore("q")
    assert await s.chord_report("c1", 0, 3, "a") is None
    assert await s.chord_report("c1", 1, 3, "b") is None
    assert await s.chord_report("c1", 0, 3, "a") is None   # redelivered member, no double-count
    res = await s.chord_report("c1", 2, 3, "c")
    assert res == ["a", "b", "c"]                          # completes exactly once, in order
    assert await s.chord_report("c1", 2, 3, "c") is None   # nothing after it fires


def test_role_backoff_is_exponential():
    role = QueueRole("q", QueueStore("q"), retry_backoff_s=1.0, retry_backoff_max_s=10.0, retry_jitter=0.0)
    assert role._backoff(1) == 1.0
    assert role._backoff(2) == 2.0
    assert role._backoff(3) == 4.0
    assert role._backoff(9) == 10.0   # capped


# ---------------------------------------------------------------------------
# End-to-end over loopback Zenoh
# ---------------------------------------------------------------------------
async def _wait(cond, timeout=8.0, step=0.1):
    for _ in range(int(timeout / step)):
        if cond():
            return
        await asyncio.sleep(step)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_processes_enqueued_jobs():
    app = _app()
    app.queue("jobs/test", lease_s=5)
    processed = []

    @app.worker("jobs/test", poll_interval_s=0.1)
    async def handle(job):
        processed.append(job)

    async with app.serving():
        await asyncio.sleep(0.6)
        await app.enqueue("jobs/test", {"n": 1})
        await app.enqueue("jobs/test", {"n": 2})
        await _wait(lambda: len(processed) >= 2)

    assert sorted(p["n"] for p in processed) == [1, 2]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_failing_job_redelivers_then_dead_letters():
    app = _app()
    app.queue("jobs/fail", lease_s=2, max_attempts=2, sweep_interval_s=0.5, retry_backoff_s=0.0)
    attempts = []

    @app.worker("jobs/fail", poll_interval_s=0.1)
    async def handle(job):
        attempts.append(job)
        raise ValueError("boom")

    async with app.serving():
        await asyncio.sleep(0.6)
        await app.enqueue("jobs/fail", {"id": "x"})

        dead: list = []

        async def _poll():
            nonlocal dead
            dead = await app.dead_letters("jobs/fail")
            return bool(dead)

        for _ in range(80):
            if await _poll():
                break
            await asyncio.sleep(0.1)

    assert len(attempts) == 2                       # tried max_attempts times
    assert len(dead) == 1
    assert dead[0]["data"] == {"id": "x"}
    assert dead[0]["last_error"] and "boom" in dead[0]["last_error"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_competing_consumers_never_double_process():
    app = _app()
    app.queue("jobs/c", lease_s=5)
    seen = []

    @app.worker("jobs/c", concurrency=3, poll_interval_s=0.05)
    async def handle(job):
        seen.append(job["id"])
        await asyncio.sleep(0.05)

    async with app.serving():
        await asyncio.sleep(0.6)
        for i in range(6):
            await app.enqueue("jobs/c", {"id": i})
        await _wait(lambda: len(seen) >= 6)

    assert sorted(seen) == [0, 1, 2, 3, 4, 5]
    assert len(seen) == len(set(seen))  # each job handled by exactly one worker


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delayed_job_waits_before_running():
    app = _app()
    app.queue("jobs/delay", lease_s=5)
    when = []
    t0 = asyncio.get_event_loop().time()

    @app.worker("jobs/delay", poll_interval_s=0.1)
    async def handle(job):
        when.append(asyncio.get_event_loop().time() - t0)

    async with app.serving():
        await asyncio.sleep(0.6)
        t0 = asyncio.get_event_loop().time()
        await app.enqueue("jobs/delay", {"n": 1}, delay_s=1.0)
        await _wait(lambda: len(when) >= 1, timeout=5.0)

    assert len(when) == 1
    assert when[0] >= 0.9  # did not run before its delay elapsed


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_backend_returns_handler_output():
    app = _app()
    app.queue("jobs/calc", lease_s=5, keep_results=True)

    @app.worker("jobs/calc", poll_interval_s=0.1)
    async def double(job):
        return {"result": job["n"] * 2}

    async with app.serving():
        await asyncio.sleep(0.6)
        job_id = await app.enqueue("jobs/calc", {"n": 21})

        outcome: dict = {}

        async def _poll():
            nonlocal outcome
            outcome = await app.result("jobs/calc", job_id)
            return outcome.get("state") == "done"

        for _ in range(60):
            if await _poll():
                break
            await asyncio.sleep(0.1)

    assert outcome["state"] == "done"
    assert outcome["result"] == {"result": 42}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retry_backoff_delays_redelivery():
    app = _app()
    app.queue("jobs/retry", lease_s=5, max_attempts=3, retry_backoff_s=0.5, retry_jitter=0.0)
    stamps = []

    @app.worker("jobs/retry", poll_interval_s=0.05)
    async def flaky(job):
        stamps.append(asyncio.get_event_loop().time())
        raise ValueError("nope")

    async with app.serving():
        await asyncio.sleep(0.6)
        await app.enqueue("jobs/retry", {"id": 1})
        await _wait(lambda: len(stamps) >= 2, timeout=5.0)

    # Second attempt is held off by the backoff, not retried instantly.
    assert len(stamps) >= 2
    assert stamps[1] - stamps[0] >= 0.4


@pytest.mark.integration
@pytest.mark.asyncio
async def test_schedule_enqueues_periodically():
    app = _app()
    app.queue("jobs/tick", lease_s=5)
    ticks = []

    @app.worker("jobs/tick", poll_interval_s=0.1)
    async def on_tick(job):
        ticks.append(job)

    app.schedule("jobs/tick", {"beat": True}, every_s=0.3, initial_delay_s=0.1)

    async with app.serving():
        await asyncio.sleep(0.6)
        await _wait(lambda: len(ticks) >= 2, timeout=5.0)

    assert len(ticks) >= 2
    assert all(t == {"beat": True} for t in ticks)


def test_local_queue_owner_lookup():
    """The beat finds its co-located owner by prefix, and reports None when the
    schedule targets a queue this node does not own."""
    app = _app()
    role = app.queue("jobs/tick", lease_s=5)
    assert app._local_queue_owner("jobs/tick") is role
    assert app._local_queue_owner("jobs/tick/") is role   # trailing slash normalised
    assert app._local_queue_owner("jobs/other") is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_an_inactive_owner_does_not_beat():
    """A co-located owner that is not the active leader (a standby) must suppress
    its schedule, so a fleet ticks once rather than once per replica."""
    app = _app()
    role = app.queue("jobs/tick", lease_s=5)
    ticks = []

    @app.worker("jobs/tick", poll_interval_s=0.1)
    async def on_tick(job):
        ticks.append(job)

    app.schedule("jobs/tick", {"beat": True}, every_s=0.2, initial_delay_s=0.1)

    async with app.serving():
        # Simulate this replica losing (or never winning) the election.
        role._active = False
        await asyncio.sleep(0.7)
        assert ticks == [], "an inactive owner must not fire the beat"

        # It wins the election → the beat resumes.
        role._active = True
        await _wait(lambda: len(ticks) >= 1, timeout=5.0)

    assert len(ticks) >= 1


# ---------------------------------------------------------------------------
# Workflows: chain / group / chord
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_chain_pipes_each_result_into_the_next():
    app = _app()
    app.queue("wf/a")
    app.queue("wf/b")
    app.queue("wf/c")
    out = []

    @app.worker("wf/a", poll_interval_s=0.05)
    async def step_a(job):
        return job + 1

    @app.worker("wf/b", poll_interval_s=0.05)
    async def step_b(job):
        return job * 2

    @app.worker("wf/c", poll_interval_s=0.05)
    async def step_c(job):
        out.append(job)

    async with app.serving():
        await asyncio.sleep(0.6)
        await app.chain(["wf/a", "wf/b", "wf/c"], 5)   # 5 → 6 → 12 → sink
        await _wait(lambda: out, timeout=5.0)

    assert out == [12]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_group_runs_all_in_parallel():
    app = _app()
    app.queue("wf/g")
    seen = []

    @app.worker("wf/g", concurrency=3, poll_interval_s=0.05)
    async def g(job):
        seen.append(job)

    async with app.serving():
        await asyncio.sleep(0.6)
        ids = await app.group("wf/g", [1, 2, 3, 4])
        await _wait(lambda: len(seen) >= 4, timeout=5.0)

    assert len(ids) == 4
    assert sorted(seen) == [1, 2, 3, 4]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_chord_fires_callback_after_all_members():
    app = _app()
    app.queue("wf/shard")
    app.queue("wf/reduce")
    reduced = []

    @app.worker("wf/shard", concurrency=2, poll_interval_s=0.05)
    async def shard(job):
        return job * 10

    @app.worker("wf/reduce", poll_interval_s=0.05)
    async def reduce(job):
        reduced.append(job)

    async with app.serving():
        await asyncio.sleep(0.6)
        await app.chord("wf/shard", [1, 2, 3], callback=("wf/reduce", {"tag": "x"}))
        await _wait(lambda: reduced, timeout=6.0)

    assert len(reduced) == 1                         # callback fired exactly once
    assert reduced[0]["results"] == [10, 20, 30]     # collected in member order
    assert reduced[0]["input"] == {"tag": "x"}


# ---------------------------------------------------------------------------
# HA: owner failover via leader election
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_ha_owner_failover():
    # Two owner replicas over shared storage; one leads, the other stands by.
    shared = InMemoryStoragePlugin()
    o1 = Istos(enable_health=False, enable_metrics=False, enable_discovery=False, storage=shared)
    o2 = Istos(enable_health=False, enable_metrics=False, enable_discovery=False, storage=shared)
    role1 = o1.queue("jobs/ha", ha=True, lease_s=5, sweep_interval_s=0.5)
    role2 = o2.queue("jobs/ha", ha=True, lease_s=5, sweep_interval_s=0.5)

    consumer = _app()
    processed = []

    @consumer.worker("jobs/ha", poll_interval_s=0.1)
    async def w(job):
        processed.append(job)

    async with o1.serving(), o2.serving(), consumer.serving():
        await _wait(lambda: role1._active or role2._active, timeout=5.0)
        await asyncio.sleep(0.6)  # let the election settle
        active = [r for r in (role1, role2) if r._active]
        assert len(active) == 1                       # exactly one leader
        leader = active[0]
        standby = role2 if leader is role1 else role1

        await consumer.enqueue("jobs/ha", {"n": 1})
        await _wait(lambda: len(processed) >= 1, timeout=5.0)

        # The leader dies; the standby must take over and keep serving.
        await leader.aclose()
        await _wait(lambda: standby._active, timeout=5.0)

        await consumer.enqueue("jobs/ha", {"n": 2})
        await _wait(lambda: len(processed) >= 2, timeout=5.0)

    assert sorted(p["n"] for p in processed) == [1, 2]


# ---------------------------------------------------------------------------
# JobContext — delivery metadata injected into a worker that asks for it
# ---------------------------------------------------------------------------
def test_job_context_attempt_predicates():
    first = JobContext(job_id="j", queue="q", attempt=1, max_attempts=3)
    assert not first.is_retry and not first.is_last_attempt

    retry = JobContext(job_id="j", queue="q", attempt=2, max_attempts=3, last_error="boom")
    assert retry.is_retry and not retry.is_last_attempt
    assert retry.last_error == "boom"

    final = JobContext(job_id="j", queue="q", attempt=3, max_attempts=3)
    assert final.is_retry and final.is_last_attempt


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_without_ctx_param_is_untouched():
    """The context is opt-in: a worker that never names ctx keeps its old shape."""
    app = _app()
    app.queue("jobs/noctx", lease_s=5)
    seen = []

    @app.worker("jobs/noctx", poll_interval_s=0.1)
    async def handle(job):
        seen.append(job)

    async with app.serving():
        await asyncio.sleep(0.6)
        await app.enqueue("jobs/noctx", {"n": 1})
        await _wait(lambda: seen)

    assert seen == [{"n": 1}]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_ctx_carries_attempt_and_last_error():
    """A redelivered job tells the worker which attempt it is, and why the last failed."""
    app = _app()
    app.queue("jobs/ctx", lease_s=2, max_attempts=3, sweep_interval_s=0.5, retry_backoff_s=0.0)
    seen = []

    @app.worker("jobs/ctx", poll_interval_s=0.1)
    async def handle(job, ctx):
        seen.append(ctx)
        if ctx.attempt < 3:
            raise ValueError(f"boom {ctx.attempt}")
        return {"ok": True}

    async with app.serving():
        await asyncio.sleep(0.6)
        await app.enqueue("jobs/ctx", {"n": 1})
        await _wait(lambda: len(seen) >= 3, timeout=15.0)

    assert [c.attempt for c in seen[:3]] == [1, 2, 3]
    assert [c.max_attempts for c in seen[:3]] == [3, 3, 3]
    assert seen[0].queue == "jobs/ctx" and seen[0].job_id

    # First delivery has nothing behind it; each retry carries the last failure.
    assert seen[0].last_error is None
    assert seen[1].last_error and "boom 1" in seen[1].last_error
    assert seen[2].last_error and "boom 2" in seen[2].last_error

    assert [c.is_retry for c in seen[:3]] == [False, True, True]
    assert [c.is_last_attempt for c in seen[:3]] == [False, False, True]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_ctx_alongside_depends():
    """ctx and Depends(...) resolve together, not either/or."""
    app = _app()
    app.queue("jobs/ctxdep", lease_s=5)
    seen = []

    async def get_setting():
        return "configured"

    @app.worker("jobs/ctxdep", poll_interval_s=0.1)
    async def handle(job, ctx, setting=Depends(get_setting)):
        seen.append((job, ctx.attempt, setting))

    async with app.serving():
        await asyncio.sleep(0.6)
        await app.enqueue("jobs/ctxdep", {"n": 7})
        await _wait(lambda: seen)

    assert seen == [({"n": 7}, 1, "configured")]
