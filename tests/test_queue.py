"""Work queues: ack, lease-based redelivery, dead-letter, competing consumers.

The QueueStore tests are pure (no network); the @app.worker / enqueue tests run
over real loopback Zenoh (owner queryables + worker claim loops)."""

import asyncio

import pytest

from istos import Istos, JobState
from istos.consistency.storage import InMemoryStoragePlugin
from istos.core.queue import QueueStore


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
    app.queue("jobs/fail", lease_s=2, max_attempts=2, sweep_interval_s=0.5)
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
