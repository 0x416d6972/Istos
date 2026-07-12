"""HTTP/SSE bridge for @stream: pure formatting + route parsing, plus an
end-to-end EventSource-style consumption over a real Zenoh session."""

import asyncio

import pytest

from istos import Istos
from istos.gateway import parse_http_spec, sse_event


# ---------------------------------------------------------------------------
# Pure: SSE frame formatting
# ---------------------------------------------------------------------------
def test_sse_event_basic():
    assert sse_event("hello") == "data: hello\n\n"


def test_sse_event_with_event_name():
    assert sse_event("", event="end") == "event: end\ndata: \n\n"


def test_sse_event_multiline_data_splits():
    # Each physical line gets its own data: field; browser rejoins with \n.
    assert sse_event("a\nb") == "data: a\ndata: b\n\n"


def test_sse_event_with_id_and_event():
    assert sse_event("x", event="msg", id="7") == "id: 7\nevent: msg\ndata: x\n\n"


# ---------------------------------------------------------------------------
# Pure: SSE routes default to GET (EventSource), honor explicit method
# ---------------------------------------------------------------------------
def test_sse_route_defaults_to_get():
    r = parse_http_spec(True, "llm/generate", timeout_s=60.0, sse=True)
    assert (r.method, r.path, r.sse, r.timeout_s) == ("GET", "/llm/generate", True, 60.0)


def test_sse_route_path_only_is_get():
    r = parse_http_spec("/gen", "llm/generate", sse=True)
    assert (r.method, r.path, r.sse) == ("GET", "/gen", True)


def test_sse_route_explicit_method_wins():
    r = parse_http_spec("POST /gen", "llm/generate", sse=True)
    assert (r.method, r.path, r.sse) == ("POST", "/gen", True)


def test_stream_decorator_registers_sse_route():
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)

    @app.stream("llm/generate", http=True)
    async def generate(prompt: str):
        yield prompt

    assert len(app._http_routes) == 1
    route = app._http_routes[0]
    assert route.sse is True
    assert route.method == "GET"
    assert route.key_expr == "llm/generate"


# ---------------------------------------------------------------------------
# Integration: browser-style SSE consumption over HTTP → Zenoh stream
# ---------------------------------------------------------------------------
@pytest.mark.integration
@pytest.mark.asyncio
async def test_sse_streams_chunks_over_http():
    import aiohttp

    port = 18099
    app = Istos(
        http_port=port,
        enable_health=False, enable_metrics=False, enable_discovery=False,
    )

    @app.stream("llm/echo", http="GET /echo")
    async def echo(prompt: str):
        for word in prompt.split():
            await asyncio.sleep(0.01)
            yield word

    task = asyncio.create_task(app.run_async())
    try:
        await asyncio.sleep(1.5)  # let session + HTTP server come up
        received = []
        async with aiohttp.ClientSession() as http:
            async with http.get(
                f"http://localhost:{port}/echo", params={"prompt": "one two three"}
            ) as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"].startswith("text/event-stream")
                async for raw in resp.content:
                    line = raw.decode().rstrip("\n")
                    if line.startswith("data: "):
                        received.append(line[len("data: "):])
                    elif line == "event: end":
                        break
        # trailing empty data: line from the end frame may appear; keep words only
        assert [w for w in received if w] == ["one", "two", "three"]
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
