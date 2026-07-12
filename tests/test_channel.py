"""@channel bidirectional sessions: ChannelSession mechanics, the wrapper's
auth/DI, and an end-to-end WebSocket round-trip."""

import asyncio

import pytest

from istos import (
    ChannelClosed,
    ChannelSession,
    Depends,
    Istos,
    TokenAuthorizer,
    UnauthorizedError,
    current_principal,
)
from istos.messages.serialization import JsonSerializer


def _mesh() -> Istos:
    return Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


def _session():
    sent = []

    async def sink(raw: bytes):
        sent.append(JsonSerializer().deserialize(raw))

    return ChannelSession(JsonSerializer(), sink), sent


# ---------------------------------------------------------------------------
# ChannelSession
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_session_send_and_receive():
    s, sent = _session()
    s.feed(JsonSerializer().serialize({"in": 1}))
    assert await s.receive() == {"in": 1}
    await s.send({"out": 2})
    assert sent == [{"out": 2}]


@pytest.mark.asyncio
async def test_session_iterates_until_close():
    s, _ = _session()
    s.feed(JsonSerializer().serialize("a"))
    s.feed(JsonSerializer().serialize("b"))
    s.close()
    got = [m async for m in s]
    assert got == ["a", "b"]  # buffered messages drain before close is seen


@pytest.mark.asyncio
async def test_session_receive_after_close_raises():
    s, _ = _session()
    s.close()
    with pytest.raises(ChannelClosed):
        await s.receive()


@pytest.mark.asyncio
async def test_session_send_after_close_raises():
    s, _ = _session()
    s.close()
    with pytest.raises(ChannelClosed):
        await s.send({"x": 1})


# ---------------------------------------------------------------------------
# channel_wrapper: auth + DI
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_channel_authorizer_denies():
    app = _mesh()

    @app.channel("agent/secure", authorizer=TokenAuthorizer("k"))
    async def secure(s: ChannelSession):
        await s.send("hi")

    wrapper = app._channels[-1]
    s, _ = _session()
    with pytest.raises(UnauthorizedError):
        await wrapper.run(s, attachment=None, params={})


@pytest.mark.asyncio
async def test_channel_injects_principal_and_runs():
    app = _mesh()

    @app.channel("agent/me")
    async def me(s: ChannelSession, principal=Depends(current_principal)):
        await s.send({"principal": principal})

    wrapper = app._channels[-1]
    s, sent = _session()
    token = b"anything"
    await wrapper.run(s, attachment=token, params={})
    assert sent == [{"principal": None}]  # no authorizer -> anonymous
    assert s.principal is None


# ---------------------------------------------------------------------------
# End-to-end WebSocket
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_channel_over_websocket_echo():
    import aiohttp

    port = 18131
    app = Istos(
        http_port=port,
        enable_health=False, enable_metrics=False, enable_discovery=False,
    )

    @app.channel("agent/echo", ws="/echo")
    async def echo(s: ChannelSession):
        await s.send({"ready": True})
        async for msg in s:
            await s.send({"echo": msg})

    task = asyncio.create_task(app.run_async())
    try:
        await asyncio.sleep(1.5)
        async with aiohttp.ClientSession() as http:
            async with http.ws_connect(f"http://localhost:{port}/echo") as ws:
                assert (await ws.receive_json()) == {"ready": True}
                await ws.send_json("ping")
                assert (await ws.receive_json()) == {"echo": "ping"}
                await ws.send_json({"n": 2})
                assert (await ws.receive_json()) == {"echo": {"n": 2}}
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
