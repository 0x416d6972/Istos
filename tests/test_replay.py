"""Durable event log: persist a stream and replay it, resuming from a cursor."""

import asyncio

import pytest

from istos import InMemoryObjectStore, Istos
from istos.communication.persist import InMemoryObjectStore as Store


@pytest.mark.asyncio
async def test_store_since_returns_only_later_events():
    s = Store()
    await s.put("t/000000000000000000001", b"a")
    await s.put("t/000000000000000000002", b"b")
    await s.put("t/000000000000000000003", b"c")
    full = await s.history("t")
    tail = await s.history("t", since=full[0][0])
    assert [v for _, v in tail] == [b"b", b"c"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_replay_full_then_resume_from_cursor():
    store = InMemoryObjectStore()
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)
    app.persist("orders/created", store)

    async with app.serving():
        await asyncio.sleep(0.4)
        for i in range(3):
            await app.publish_once("orders/created", {"n": i})
        await asyncio.sleep(0.4)  # let the writer persist

        events = [e async for e in app.replay("orders/created")]
        assert [e.data["n"] for e in events] == [0, 1, 2]
        assert all(e.position for e in events)
        assert events[0].timestamp_ms > 0

        # Resume strictly after the first event.
        cursor = events[0].position
        rest = [e async for e in app.replay("orders/created", since=cursor)]
        assert [e.data["n"] for e in rest] == [1, 2]

        # A cursor at the end yields nothing new.
        tip = events[-1].position
        assert [e async for e in app.replay("orders/created", since=tip)] == []
