"""Streaming RPC — token/chunk streaming via multi-reply queryables.

@stream handlers are async generators; each yield is a reply chunk delivered
incrementally to `async for chunk in app.stream_query(...)`.
"""

import asyncio

import pytest

from istos import Istos, IstosError
from istos.core.stream import stream_wrapper
from istos.messages.serialization import JsonSerializer


# ---------------------------------------------------------------------------
# 1. Wrapper contract (no network)
# ---------------------------------------------------------------------------
def test_stream_requires_async_generator():
    async def not_a_gen(x: int):
        return x

    with pytest.raises(TypeError, match="async generator"):
        stream_wrapper(not_a_gen, "k", JsonSerializer())


def test_stream_registers(istos: Istos):
    @istos.stream("llm/gen")
    async def gen(prompt: str):
        yield prompt

    assert [s.prefix for s in istos._streams] == ["llm/gen"]


async def _drain(aiter):
    return [c async for c in aiter]


def _bg(app: Istos):
    return asyncio.create_task(app.run_async())


async def _stop(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# 2. Integration: streaming over a real Zenoh session
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_delivers_chunks_in_order():
    app = Istos(enable_health=False, enable_metrics=False)

    @app.stream("istos/test/gen")
    async def gen(prompt: str):
        for word in prompt.split():
            yield {"token": word}
            await asyncio.sleep(0.02)

    task = _bg(app)
    try:
        await asyncio.sleep(1.2)
        chunks = await _drain(
            app.stream_query("istos/test/gen", prompt="hello brave new world", timeout_s=5)
        )
        assert [c["token"] for c in chunks] == ["hello", "brave", "new", "world"]
    finally:
        await _stop(task)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_authorization():
    app = Istos(enable_health=False, enable_metrics=False)

    @app.stream("istos/test/secure", authorizer=lambda ctx: ctx.token == "k")
    async def secure(prompt: str):
        yield prompt

    task = _bg(app)
    try:
        await asyncio.sleep(1.2)
        # No token → denied → the error chunk is raised.
        with pytest.raises(IstosError):
            await _drain(app.stream_query("istos/test/secure", prompt="x", timeout_s=5))
        # With token → streams.
        got = await _drain(
            app.stream_query("istos/test/secure", prompt="x", token="k", timeout_s=5)
        )
        assert got == ["x"]
    finally:
        await _stop(task)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_consumer_can_cancel_early():
    app = Istos(enable_health=False, enable_metrics=False)

    @app.stream("istos/test/long")
    async def long_stream(n: int):
        for i in range(n):
            yield i
            await asyncio.sleep(0.05)

    task = _bg(app)
    try:
        await asyncio.sleep(1.2)
        got = []
        async for chunk in app.stream_query("istos/test/long", n=1000, timeout_s=30):
            got.append(chunk)
            if len(got) == 3:
                break  # cancel early — must not hang, cancels the underlying get
        assert got == [0, 1, 2]
        # Session stays healthy: a fresh stream still works after cancellation.
        again = await _drain(app.stream_query("istos/test/long", n=2, timeout_s=5))
        assert again == [0, 1]
    finally:
        await _stop(task)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_error_mid_stream_raises_after_partial():
    app = Istos(enable_health=False, enable_metrics=False)

    @app.stream("istos/test/boom")
    async def boom(n: int):
        yield {"i": 0}
        raise ValueError("kaboom")

    task = _bg(app)
    try:
        await asyncio.sleep(1.2)
        chunks = []
        with pytest.raises(IstosError):
            async for c in app.stream_query("istos/test/boom", n=1, timeout_s=5):
                chunks.append(c)
        assert chunks == [{"i": 0}]  # partial output delivered before the error
    finally:
        await _stop(task)
