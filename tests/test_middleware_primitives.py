"""Middleware runs around @stream and @channel, not just @handle. It wraps the
whole stream / whole session — once at open, once at the end."""

import asyncio

import pytest

from istos import ChannelSession, Istos, IstosTestClient


class _Recorder:
    """Records the operation of every request that passes through, and how many
    times each one entered and left the handler."""

    def __init__(self) -> None:
        self.entered: list = []
        self.left: list = []

    async def __call__(self, scope, call_next):
        self.entered.append(scope.operation)
        try:
            return await call_next(scope)
        finally:
            self.left.append(scope.operation)


def _app() -> Istos:
    return Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


@pytest.mark.asyncio
async def test_middleware_wraps_channel_once():
    app = _app()
    rec = _Recorder()
    app.add_middleware(rec)

    @app.channel("agent/chat")
    async def chat(session: ChannelSession):
        async for msg in session:
            await session.send(msg)

    client = IstosTestClient(app)
    async with client.channel("agent/chat") as chan:
        await chan.send("a")
        assert await chan.receive() == "a"

    # One entry / one exit for the whole session, regardless of message count.
    assert rec.entered == ["channel"]
    assert rec.left == ["channel"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_middleware_wraps_stream_once():
    app = _app()
    rec = _Recorder()
    app.add_middleware(rec)

    @app.stream("llm/gen")
    async def gen(prompt: str):
        for word in prompt.split():
            yield word

    async with app.serving():
        await asyncio.sleep(0.8)
        chunks = [c async for c in app.stream_query("llm/gen", prompt="one two")]

    assert chunks == ["one", "two"]
    # One entry / one exit for the whole stream, not per chunk.
    assert rec.entered == ["stream"]
    assert rec.left == ["stream"]
