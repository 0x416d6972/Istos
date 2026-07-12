"""Opt-in subscriber dedup window."""

import pytest

from istos import Istos
from istos.messages.serialization import JsonSerializer


def _sub(dedup):
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)
    got = []

    @app.subscribe("orders/created", dedup=dedup)
    async def on_created(data):
        got.append(data)

    # subscribe() returns the wrapper; grab the one just registered.
    return app._subscribers[-1], got


def _bytes(obj):
    return JsonSerializer().serialize(obj)


@pytest.mark.asyncio
async def test_dedup_suppresses_repeated_payload():
    sub, got = _sub(dedup=True)
    payload = _bytes({"id": 1})
    await sub._deliver(payload)
    await sub._deliver(payload)  # exact duplicate -> dropped
    assert got == [{"id": 1}]


@pytest.mark.asyncio
async def test_dedup_disabled_by_default_delivers_twice():
    sub, got = _sub(dedup=False)
    payload = _bytes({"id": 1})
    await sub._deliver(payload)
    await sub._deliver(payload)
    assert got == [{"id": 1}, {"id": 1}]


@pytest.mark.asyncio
async def test_dedup_passes_distinct_payloads():
    sub, got = _sub(dedup=True)
    await sub._deliver(_bytes({"id": 1}))
    await sub._deliver(_bytes({"id": 2}))
    await sub._deliver(_bytes({"id": 1}))  # repeat of first -> dropped
    assert got == [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_dedup_window_evicts_oldest():
    # Window of 1: only the most recent fingerprint is remembered, so an
    # older payload is no longer recognised as a duplicate.
    sub, got = _sub(dedup=1)
    await sub._deliver(_bytes({"id": 1}))
    await sub._deliver(_bytes({"id": 2}))  # evicts id=1 from the window
    await sub._deliver(_bytes({"id": 1}))  # no longer seen -> delivered again
    assert got == [{"id": 1}, {"id": 2}, {"id": 1}]
