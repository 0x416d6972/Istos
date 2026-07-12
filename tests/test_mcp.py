"""MCP adapter: JSON-RPC dispatch, tool listing, and tool calls over the mesh."""

import asyncio

import pytest

from istos import Istos, MCPServer


def _app() -> Istos:
    app = Istos(enable_health=False, enable_metrics=False, enable_discovery=False)

    @app.handle("math/add")
    async def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    @app.handle(".istos/internal")
    async def internal() -> dict:
        return {}

    return app


@pytest.mark.asyncio
async def test_initialize():
    srv = MCPServer(_app())
    resp = await srv.handle({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
    assert resp["result"]["serverInfo"]["name"] == "istos"
    assert "tools" in resp["result"]["capabilities"]


@pytest.mark.asyncio
async def test_tools_list_maps_handlers_and_hides_plumbing():
    srv = MCPServer(_app())
    resp = await srv.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    tools = {t["name"]: t for t in resp["result"]["tools"]}
    assert "math-add" in tools                       # '/' -> '-'
    assert not any(n.startswith(".istos") for n in tools)  # plumbing hidden
    assert tools["math-add"]["description"] == "Add two integers."
    assert tools["math-add"]["inputSchema"]["properties"]["a"]["type"] == "integer"


@pytest.mark.asyncio
async def test_notifications_have_no_response():
    srv = MCPServer(_app())
    assert await srv.handle({"jsonrpc": "2.0", "method": "notifications/initialized"}) is None


@pytest.mark.asyncio
async def test_unknown_tool_errors():
    srv = MCPServer(_app())
    resp = await srv.handle({
        "jsonrpc": "2.0", "id": 3, "method": "tools/call",
        "params": {"name": "nope", "arguments": {}},
    })
    assert resp["error"]["code"] == -32602


@pytest.mark.integration
@pytest.mark.asyncio
async def test_tools_call_routes_to_handler():
    app = _app()
    srv = MCPServer(app)
    async with app.serving():
        await asyncio.sleep(0.4)
        resp = await srv.handle({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {"name": "math-add", "arguments": {"a": 2, "b": 3}},
        })
        result = resp["result"]
        assert result["isError"] is False
        assert result["content"][0]["text"] == "5"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_over_http():
    import aiohttp

    port = 18147
    app = _app()
    app._enable_mcp = True  # normally Istos(enable_mcp=True)
    app._http_port = port

    task = asyncio.create_task(app.run_async())
    try:
        await asyncio.sleep(1.5)
        async with aiohttp.ClientSession() as http:
            async with http.post(
                f"http://localhost:{port}/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            ) as r:
                body = await r.json()
                assert any(t["name"] == "math-add" for t in body["result"]["tools"])
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
