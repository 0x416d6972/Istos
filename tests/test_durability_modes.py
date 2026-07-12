"""Handler delivery semantics: at_most_once / at_least_once / exactly_once.

These exercise the idempotency ledger directly through ``handler_wrapper.__call__``
(the same path a real query takes after decoding), so we can assert on how many
times the body actually ran without standing up a network.
"""

import pytest

from istos.core.handler import handler_wrapper
from istos.consistency.storage import Durability, InMemoryStoragePlugin
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
