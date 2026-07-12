"""IstosTestClient: in-process coverage of auth, streaming and durability —
no Zenoh network required."""

import pytest

from istos import (
    ChannelClosed,
    ChannelSession,
    Depends,
    Istos,
    IstosTestClient,
    TokenAuthorizer,
    UnauthorizedError,
    current_principal,
    current_token,
)


def _app() -> Istos:
    return Istos(enable_health=False, enable_metrics=False, enable_discovery=False)


# ---------------------------------------------------------------------------
# Auth gate runs in-process
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_query_authorizer_denies_without_token():
    app = _app()

    @app.handle("admin/op", authorizer=TokenAuthorizer("secret"))
    async def op(x: int):
        return {"x": x}

    client = IstosTestClient(app)
    with pytest.raises(UnauthorizedError):
        await client.query("admin/op", x=1)  # no token -> denied


@pytest.mark.asyncio
async def test_query_authorizer_allows_with_token():
    app = _app()

    @app.handle("admin/op", authorizer=TokenAuthorizer("secret"))
    async def op(x: int):
        return {"x": x}

    client = IstosTestClient(app)
    assert await client.query("admin/op", token="secret", x=2) == {"x": 2}


@pytest.mark.asyncio
async def test_query_injects_token_and_principal():
    app = _app()

    def authorizer(ctx):
        return {"sub": "alice"} if ctx.token == "k" else False

    @app.handle("me/whoami", authorizer=authorizer)
    async def whoami(
        principal=Depends(current_principal), token=Depends(current_token)
    ):
        return {"principal": principal, "token": token}

    client = IstosTestClient(app)
    result = await client.query("me/whoami", token="k")
    assert result == {"principal": {"sub": "alice"}, "token": "k"}


# ---------------------------------------------------------------------------
# Durability ledger runs in-process
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_query_exactly_once_dedups_in_process():
    app = _app()
    calls = []

    @app.handle("math/double", durability="exactly_once")
    async def double(x: int):
        calls.append(x)
        return {"result": x * 2}

    client = IstosTestClient(app)
    assert await client.query("math/double", x=5) == {"result": 10}
    assert await client.query("math/double", x=5) == {"result": 10}
    assert calls == [5]  # second call served from the ledger


# ---------------------------------------------------------------------------
# Streaming in-process
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stream_yields_chunks_in_process():
    app = _app()

    @app.stream("llm/echo")
    async def echo(prompt: str):
        for word in prompt.split():
            yield word

    client = IstosTestClient(app)
    chunks = [c async for c in client.stream("llm/echo", prompt="one two three")]
    assert chunks == ["one", "two", "three"]


@pytest.mark.asyncio
async def test_stream_authorizer_denies():
    app = _app()

    @app.stream("llm/secure", authorizer=TokenAuthorizer("k"))
    async def gen(prompt: str):
        yield prompt

    client = IstosTestClient(app)
    with pytest.raises(UnauthorizedError):
        async for _ in client.stream("llm/secure", prompt="hi"):
            pass


@pytest.mark.asyncio
async def test_stream_resolves_dependencies():
    app = _app()

    async def config():
        return "cfg"

    @app.stream("llm/withdep")
    async def gen(prompt: str, cfg: str = Depends(config)):
        yield f"{prompt}:{cfg}"

    client = IstosTestClient(app)
    chunks = [c async for c in client.stream("llm/withdep", prompt="x")]
    assert chunks == ["x:cfg"]


# ---------------------------------------------------------------------------
# Channels in-process (duplex)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_channel_echo_in_process():
    app = _app()

    @app.channel("agent/chat")
    async def chat(session: ChannelSession):
        await session.send({"role": "system", "text": "ready"})
        async for msg in session:
            await session.send({"echo": msg})

    client = IstosTestClient(app)
    async with client.channel("agent/chat") as chan:
        assert await chan.receive() == {"role": "system", "text": "ready"}
        await chan.send("hi")
        assert await chan.receive() == {"echo": "hi"}


@pytest.mark.asyncio
async def test_channel_ends_raises_channel_closed():
    app = _app()

    @app.channel("agent/once")
    async def once(session: ChannelSession):
        await session.send("bye")   # handler returns -> session ends

    client = IstosTestClient(app)
    async with client.channel("agent/once") as chan:
        assert await chan.receive() == "bye"
        with pytest.raises(ChannelClosed):
            await chan.receive()


@pytest.mark.asyncio
async def test_channel_authorizer_denies():
    app = _app()

    @app.channel("agent/secure", authorizer=TokenAuthorizer("k"))
    async def secure(session: ChannelSession):
        await session.send("in")

    client = IstosTestClient(app)
    with pytest.raises(UnauthorizedError):
        async with client.channel("agent/secure"):
            pass


@pytest.mark.asyncio
async def test_channel_injects_principal():
    app = _app()

    def authorizer(ctx):
        return {"sub": "alice"} if ctx.token == "k" else False

    @app.channel("agent/who", authorizer=authorizer)
    async def who(session: ChannelSession, principal=Depends(current_principal)):
        await session.send(principal)

    client = IstosTestClient(app)
    async with client.channel("agent/who", token="k") as chan:
        assert await chan.receive() == {"sub": "alice"}
