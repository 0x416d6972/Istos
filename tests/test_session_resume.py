"""Durable @channel sessions: conversation history persists and resumes."""

import asyncio

import pytest

from istos import ChannelSession, Istos, SessionStore
from istos.consistency.storage import InMemoryStoragePlugin


# ---------------------------------------------------------------------------
# SessionStore
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_session_store_is_chronological():
    store = SessionStore(InMemoryStoragePlugin())
    await store.append("c", "in", {"n": 1})
    await store.append("c", "out", {"n": 2})
    await store.append("c", "in", {"n": 3})
    hist = await store.history("c")
    assert [(e["dir"], e["data"]["n"]) for e in hist] == [("in", 1), ("out", 2), ("in", 3)]


@pytest.mark.asyncio
async def test_history_empty_without_store():
    async def _sink(_):
        pass
    from istos.messages.serialization import JsonSerializer
    s = ChannelSession(JsonSerializer(), _sink)  # not durable
    assert await s.history() == []
    assert s.conversation_id is None


# ---------------------------------------------------------------------------
# End-to-end resume over the fabric
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_durable_channel_persists_and_resumes():
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)
    seen_history = []

    @app.channel("agent/chat", durable=True)
    async def chat(s: ChannelSession):
        # On (re)connect, report how many prior turns this conversation had.
        seen_history.append(len(await s.history()))
        async for msg in s:
            await s.send({"echo": msg})

    async with app.serving():
        await asyncio.sleep(0.4)

        # First connection: two turns.
        chan = await app.open_channel("agent/chat", timeout_s=5.0)
        cid = chan.conversation_id
        await chan.send("one")
        assert await chan.receive() == {"echo": "one"}
        await chan.send("two")
        assert await chan.receive() == {"echo": "two"}
        await chan.close()
        await asyncio.sleep(0.3)

        # Reconnect with the same conversation_id: history carries over.
        chan2 = await app.open_channel("agent/chat", conversation_id=cid, timeout_s=5.0)
        await chan2.send("three")
        assert await chan2.receive() == {"echo": "three"}
        await chan2.close()
        await asyncio.sleep(0.3)

    # First session saw no prior history; the resumed one saw the 4 earlier
    # messages (2 in + 2 out) logged during the first connection.
    assert seen_history[0] == 0
    assert seen_history[1] == 4


@pytest.mark.integration
@pytest.mark.asyncio
async def test_non_durable_channel_has_no_history():
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)
    hist_lens = []

    @app.channel("agent/plain")
    async def chat(s: ChannelSession):
        hist_lens.append(len(await s.history()))
        async for msg in s:
            await s.send(msg)

    async with app.serving():
        await asyncio.sleep(0.4)
        chan = await app.open_channel("agent/plain", timeout_s=5.0)
        await chan.send("x")
        assert await chan.receive() == "x"
        await chan.close()
        await asyncio.sleep(0.2)

    assert hist_lens == [0]  # nothing persisted for a non-durable channel
