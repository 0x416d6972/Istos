"""Cross-node @channel over Zenoh: open handshake, duplex messages, auth,
and liveliness teardown. Loopback (one process, real Zenoh session)."""

import asyncio

import pytest

from istos import ChannelSession, Istos, TokenAuthorizer


def _mesh() -> Istos:
    return Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fabric_channel_duplex_echo():
    app = _mesh()

    @app.channel("agent/echo")
    async def echo(s: ChannelSession):
        await s.send({"ready": True})
        async for msg in s:
            await s.send({"echo": msg})

    async with app.serving():
        await asyncio.sleep(0.5)
        chan = await app.open_channel("agent/echo", timeout_s=5.0)
        try:
            assert await chan.receive() == {"ready": True}
            await chan.send("ping")
            assert await chan.receive() == {"echo": "ping"}
            await chan.send({"n": 2})
            assert await chan.receive() == {"echo": {"n": 2}}
        finally:
            await chan.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fabric_channel_open_denied():
    app = _mesh()

    @app.channel("agent/secure", authorizer=TokenAuthorizer("k"))
    async def secure(s: ChannelSession):
        await s.send("hi")

    async with app.serving():
        await asyncio.sleep(0.5)
        with pytest.raises(Exception):  # open handshake replies unauthorized
            await app.open_channel("agent/secure", timeout_s=3.0)  # no token
        # with the right token it opens
        chan = await app.open_channel("agent/secure", token="k", timeout_s=5.0)
        try:
            assert await chan.receive() == "hi"
        finally:
            await chan.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fabric_channel_client_close_ends_handler():
    app = _mesh()
    ended = asyncio.Event()

    @app.channel("agent/lifecycle")
    async def handler(s: ChannelSession):
        try:
            async for _ in s:
                pass
        finally:
            ended.set()  # handler's loop ended after the peer went away

    async with app.serving():
        await asyncio.sleep(0.5)
        chan = await app.open_channel("agent/lifecycle", timeout_s=5.0)
        await chan.send("one")
        await asyncio.sleep(0.3)
        await chan.close()  # drops liveliness -> server tears the session down
        await asyncio.wait_for(ended.wait(), timeout=5.0)
