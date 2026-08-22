"""Handler delivery semantics: at_most_once / at_least_once / exactly_once."""

import asyncio

import pytest

from istos.primitives.handler import handler_wrapper
from istos.consistency.storage import ClaimState, Durability, InMemoryStoragePlugin
from istos.errors import ConflictError, is_retryable
from istos.messages.serialization import JsonSerializer


def _wrap(func, storage, durability):
    return handler_wrapper(
        func,
        prefix="math/double",
        storage=storage,
        serializer=JsonSerializer(),
        durability=durability,
    )


@pytest.mark.asyncio
async def test_at_most_once_runs_body_every_time():
    storage = InMemoryStoragePlugin()
    calls = []

    async def double(x: int):
        calls.append(x)
        return {"result": x * 2}

    h = _wrap(double, storage, Durability.AT_MOST_ONCE)
    assert await h(x=5) == {"result": 10}
    assert await h(x=5) == {"result": 10}
    # Fire-and-forget: no dedup, no event log.
    assert calls == [5, 5]
    assert await storage.get_log("math/double") == []


@pytest.mark.asyncio
async def test_at_least_once_logs_every_call():
    storage = InMemoryStoragePlugin()
    calls = []

    async def double(x: int):
        calls.append(x)
        return {"result": x * 2}

    h = _wrap(double, storage, Durability.AT_LEAST_ONCE)
    await h(x=1)
    await h(x=2)
    # Body runs each time (redelivery may duplicate) but every call is logged.
    assert calls == [1, 2]
    log = await storage.get_log("math/double")
    assert len(log) == 2


@pytest.mark.asyncio
async def test_exactly_once_dedups_and_returns_cached():
    storage = InMemoryStoragePlugin()
    calls = []

    async def double(x: int):
        calls.append(x)
        return {"result": x * 2}

    h = _wrap(double, storage, Durability.EXACTLY_ONCE)
    r1 = await h(x=7)
    r2 = await h(x=7)  # same params -> served from ledger, body NOT re-run
    assert r1 == r2 == {"result": 14}
    assert calls == [7]
    # exactly_once also logs the event exactly once (not skipped by the ledger).
    assert len(await storage.get_log("math/double")) == 1


@pytest.mark.asyncio
async def test_exactly_once_distinguishes_params():
    storage = InMemoryStoragePlugin()
    calls = []

    async def double(x: int):
        calls.append(x)
        return {"result": x * 2}

    h = _wrap(double, storage, Durability.EXACTLY_ONCE)
    assert await h(x=1) == {"result": 2}
    assert await h(x=2) == {"result": 4}  # different key -> runs
    assert await h(x=1) == {"result": 2}  # repeat of first -> cached
    assert calls == [1, 2]


@pytest.mark.asyncio
async def test_exactly_once_survives_new_wrapper_same_storage():
    """A crash/restart re-creates the wrapper but reuses the durable ledger,
    so a redelivered request still dedups."""
    storage = InMemoryStoragePlugin()
    calls = []

    async def double(x: int):
        calls.append(x)
        return {"result": x * 2}

    h1 = _wrap(double, storage, Durability.EXACTLY_ONCE)
    assert await h1(x=9) == {"result": 18}

    # Simulate a process restart: fresh wrapper, same backing store.
    h2 = _wrap(double, storage, Durability.EXACTLY_ONCE)
    assert await h2(x=9) == {"result": 18}
    assert calls == [9]


@pytest.mark.asyncio
async def test_exactly_once_concurrent_duplicates_execute_once():
    """The claim is what makes this exactly-once.

    Dedup by check-then-mark around the handler leaves the whole body between
    the two, so every concurrent redelivery passes the check and runs the side
    effects. The loser of the race gets a ConflictError to retry.
    """
    storage = InMemoryStoragePlugin()
    calls = []

    async def charge(order_id: str):
        calls.append(order_id)
        await asyncio.sleep(0.05)
        return {"charged": order_id}

    h = _wrap(charge, storage, Durability.EXACTLY_ONCE)
    results = await asyncio.gather(
        *(h(order_id="o1") for _ in range(8)), return_exceptions=True
    )

    assert calls == ["o1"]
    conflicts = [r for r in results if isinstance(r, ConflictError)]
    ok = [r for r in results if not isinstance(r, BaseException)]
    assert len(ok) + len(conflicts) == 8
    assert all(r == {"charged": "o1"} for r in ok)
    assert all(is_retryable(c) for c in conflicts)

    # Once the winner lands, a retry collects its result without re-running.
    assert await h(order_id="o1") == {"charged": "o1"}
    assert calls == ["o1"]


@pytest.mark.asyncio
async def test_exactly_once_caches_a_none_result():
    """A handler returning None must not re-run forever.

    check_processed() cannot tell "never ran" from "ran and returned None", so
    the claim has to carry that distinction.
    """
    storage = InMemoryStoragePlugin()
    calls = []

    async def notify(user: str):
        calls.append(user)

    h = _wrap(notify, storage, Durability.EXACTLY_ONCE)
    assert await h(user="amir") is None
    assert await h(user="amir") is None
    assert await h(user="amir") is None
    assert calls == ["amir"]


@pytest.mark.asyncio
async def test_exactly_once_releases_the_claim_when_the_handler_fails():
    """Failed work must stay retryable — a claim is not a result."""
    storage = InMemoryStoragePlugin()
    attempts = []

    async def flaky(x: int):
        attempts.append(x)
        if len(attempts) == 1:
            raise RuntimeError("boom")
        return {"result": x}

    h = _wrap(flaky, storage, Durability.EXACTLY_ONCE)
    with pytest.raises(RuntimeError):
        await h(x=1)
    assert await h(x=1) == {"result": 1}
    assert attempts == [1, 1]


@pytest.mark.asyncio
async def test_exactly_once_takes_over_an_expired_lease():
    """A node that dies mid-handler must not hold the key forever."""
    storage = InMemoryStoragePlugin()

    first = await storage.claim_processed("k", lease_s=0.1)
    assert first.state is ClaimState.CLAIMED
    assert (await storage.claim_processed("k")).state is ClaimState.IN_FLIGHT

    await asyncio.sleep(0.15)
    assert (await storage.claim_processed("k", lease_s=5)).state is ClaimState.CLAIMED

    # A finished result is terminal — no lease expires it.
    await storage.mark_processed("k", {"v": 1})
    assert (await storage.claim_processed("k", lease_s=0.0)).state is ClaimState.DONE
    await storage.release_claim("k")
    assert (await storage.claim_processed("k")).result == {"v": 1}


@pytest.mark.asyncio
async def test_exactly_once_warns_when_storage_cannot_claim():
    """A plugin predating claim_processed() degrades — say so, don't pretend."""

    class LegacyStorage(InMemoryStoragePlugin):
        def __getattribute__(self, name):
            if name in ("claim_processed", "release_claim"):
                raise AttributeError(name)
            return object.__getattribute__(self, name)

    calls = []

    async def double(x: int):
        calls.append(x)
        await asyncio.sleep(0.05)
        return {"result": x * 2}

    h = _wrap(double, LegacyStorage(), Durability.EXACTLY_ONCE)
    with pytest.warns(RuntimeWarning, match="does not implement claim_processed"):
        await asyncio.gather(*(h(x=1) for _ in range(4)))
    assert len(calls) == 4  # the old behaviour, now announced


@pytest.mark.asyncio
async def test_exactly_once_still_logs_the_claimed_call():
    """The claim exists before log() runs; only a *finished* key suppresses it."""
    storage = InMemoryStoragePlugin()

    async def double(x: int):
        return {"result": x * 2}

    h = _wrap(double, storage, Durability.EXACTLY_ONCE)
    await h(x=1)
    await h(x=1)
    assert len(await storage.get_log("math/double")) == 1
