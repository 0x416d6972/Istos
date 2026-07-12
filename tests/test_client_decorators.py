"""Client-side @stream_client / @channel_client decorators — the declarative
counterparts to @query, on the app and on a router."""

import asyncio

import pytest

from istos import ChannelSession, Istos
from istos.routing import IstosRouter


def _mesh() -> Istos:
    return Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


def test_decorators_register_callables():
    app = _mesh()

    @app.stream_client("llm/gen")
    async def gen(chunks):
        return [c async for c in chunks]

    @app.channel_client("agent/chat")
    async def chat(session):
        return session

    assert callable(gen) and callable(chat)


def test_router_client_decorators_wire_up():
    app = _mesh()
    router = IstosRouter(prefix="svc")

    @router.stream_client("gen")
    async def gen(chunks): ...

    @router.channel_client("chat")
    async def chat(session): ...

    app.include_router(router)
    # RouterProxy resolves to the real wrapper after include_router.
    assert gen._real_wrapper is not None
    assert chat._real_wrapper is not None
    assert gen._real_wrapper.prefix == "svc/gen"


# ---------------------------------------------------------------------------
# End-to-end over Zenoh (loopback)
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_client_consumes_stream():
    app = _mesh()

    @app.stream("llm/echo")
    async def echo(prompt: str):
        for word in prompt.split():
            yield word

    @app.stream_client("llm/echo")
    async def collect(chunks):
        return [c async for c in chunks]

    async with app.serving():
        await asyncio.sleep(0.4)
        assert await collect(prompt="one two three") == ["one", "two", "three"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_channel_client_drives_channel():
    app = _mesh()

    @app.channel("agent/echo")
    async def echo(s: ChannelSession):
        async for msg in s:
            await s.send({"echo": msg})

    @app.channel_client("agent/echo")
    async def talk(session):
        await session.send("hi")
        return await session.receive()

    async with app.serving():
        await asyncio.sleep(0.4)
        # body receives only the session; call kwargs would be open params.
        assert await talk() == {"echo": "hi"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_channel_client_closes_session_on_return():
    app = _mesh()
    ended = asyncio.Event()

    @app.channel("agent/life")
    async def life(s: ChannelSession):
        try:
            async for _ in s:
                pass
        finally:
            ended.set()

    @app.channel_client("agent/life")
    async def once(session):
        await session.send("x")
        await asyncio.sleep(0.2)
        # returning here should close the session -> server handler ends

    async with app.serving():
        await asyncio.sleep(0.4)
        await once()
        await asyncio.wait_for(ended.wait(), timeout=5.0)
