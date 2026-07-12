"""Brokerless durable pub/sub: a late subscriber replays history from the
producer's cache, with no broker. Uses a real Zenoh session (integration)."""

import asyncio

import pytest
import zenoh

from istos.primitives.publish import publish_wrapper
from istos.primitives.subscribe import subscribe_wrapper
from istos.communication.durable import (
    declare_durable_publisher,
    declare_durable_subscriber,
)
from istos.messages.serialization import JsonSerializer


def test_durable_defaults_off():
    """Non-durable is the default; durable flags are opt-in and stored."""
    pw = publish_wrapper(lambda: 1, "p", JsonSerializer(), get_session=lambda: None)
    sw = subscribe_wrapper(lambda d: d, "p", JsonSerializer())
    assert pw.durable is False and sw.durable is False


def test_durable_and_shm_conflict():
    with pytest.raises(ValueError, match="cannot be combined"):
        publish_wrapper(lambda: 1, "p", JsonSerializer(), get_session=lambda: None,
                        durable=True, use_shm=True)


@pytest.mark.asyncio
async def test_durable_publish_requires_running_service():
    """Calling a durable publisher before it's declared fails clearly."""
    # Session present, but the AdvancedPublisher hasn't been declared (service not started).
    pw = publish_wrapper(lambda v: v, "p", JsonSerializer(),
                         get_session=lambda: object(), durable=True)
    with pytest.raises(RuntimeError, match="Durable publisher not declared"):
        await pw("x")


@pytest.mark.asyncio
async def test_handle_miss_logs_and_invokes_callback():
    """An unrecoverable gap fires on_miss(source, nb); async callbacks are awaited."""
    seen = []

    async def on_miss(source, nb):
        seen.append((source, nb))

    sw = subscribe_wrapper(lambda d: d, "p", JsonSerializer(), durable=True, on_miss=on_miss)
    await sw.handle_miss("peer/abc", 3)
    assert seen == [("peer/abc", 3)]


@pytest.mark.asyncio
async def test_handle_miss_survives_callback_error():
    """A throwing on_miss must not escape handle_miss (it only logs)."""
    def boom(source, nb):
        raise RuntimeError("nope")

    sw = subscribe_wrapper(lambda d: d, "p", JsonSerializer(), durable=True, on_miss=boom)
    await sw.handle_miss("peer/x", 1)  # should not raise


@pytest.mark.integration
def test_durable_publisher_defaults_reliable_block():
    """durable=True hardens the transport: BLOCK congestion (no silent drop)."""
    session = zenoh.open(zenoh.Config())
    try:
        pub = declare_durable_publisher(session, "istos/test/qos", cache=10)
        assert pub.congestion_control == zenoh.CongestionControl.BLOCK

        override = declare_durable_publisher(
            session, "istos/test/qos2", cache=10,
            congestion_control=zenoh.CongestionControl.DROP,
        )
        assert override.congestion_control == zenoh.CongestionControl.DROP

        pub.undeclare()
        override.undeclare()
    finally:
        session.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_late_subscriber_replays_history_brokerless():
    """
    The core promise: publish before any subscriber exists, then a subscriber
    joins late and still receives every message — replayed peer-to-peer from the
    producer's cache, no broker involved.
    """
    session = zenoh.open(zenoh.Config())
    try:
        pub = publish_wrapper(
            lambda v: v, "istos/test/durable", JsonSerializer(),
            get_session=lambda: session, durable=True, cache=100,
        )
        pub.declare(session)

        # Publish BEFORE any subscriber is listening.
        for i in range(5):
            await pub(f"event-{i}")
        await asyncio.sleep(0.4)

        received = []
        sub = declare_durable_subscriber(
            session, "istos/test/durable",
            lambda smp: received.append(bytes(smp.payload).decode()),
            replay=100,
        )
        await asyncio.sleep(1.2)  # allow history replay

        assert received == ['"event-0"', '"event-1"', '"event-2"', '"event-3"', '"event-4"']

        # And live messages continue to flow after joining.
        await pub("event-live")
        await asyncio.sleep(0.4)
        assert received[-1] == '"event-live"'

        pub.undeclare()
        sub.undeclare()
    finally:
        session.close()
