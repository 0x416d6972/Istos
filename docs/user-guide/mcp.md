---
title: MCP Tools
---

# MCP tools

`Istos(enable_mcp=True)` serves the node's **`@handle`** endpoints as
[Model Context Protocol](https://modelcontextprotocol.io) tools so an LLM
client can discover and call them over JSON-RPC on the embedded HTTP surface.

Protocol version advertised on `initialize`: **`2025-06-18`**.

## Enable

```python
from istos import Istos

app = Istos(http_port=8080, enable_mcp=True, authorizer=jwt, mcp_path="/mcp")

@app.handle("math/add")
async def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b
```

`tools/list` builds each tool from the handler's name, docstring, and parameter
schema (`math/add` → tool `math-add`). `tools/call` routes through the mesh via
`query_once`, forwarding the `Authorization` bearer token so the authorizer
still runs. Plumbing endpoints (`.istos/*`) are hidden. The tool result is the
handler's reply as text content, with `isError` set when the reply is an error
envelope.

!!! warning "Handle-only"
    MCP lists and calls `@handle` only. `@stream` and `@channel` are not
    exposed as tools — use SSE / WebSocket / `stream_query` / `open_channel`
    for those. Capability discovery (`.istos/capabilities`) is broader than
    MCP; don't assume the two catalogs match.

## HTTP behavior

| Request | Response |
|---------|----------|
| Single JSON-RPC object | JSON-RPC response body |
| JSON-RPC **batch** (array) | Array of responses (notifications omitted) |
| Notification / request with no `id` | HTTP **202** with empty body |
| Bad JSON | `{"jsonrpc":"2.0","error":{"code":-32700,...}}` with status 400 |

Point an MCP-capable client at `http://host:8080/mcp` (or your `mcp_path`)
with a bearer token if the app has an authorizer.

Example `tools/list`:

```bash
curl -s http://localhost:8080/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

Example `tools/call`:

```bash
curl -s http://localhost:8080/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call",
       "params":{"name":"math-add","arguments":{"a":1,"b":2}}}'
```

## Programmatic use

`MCPServer` is exported from `istos` if you want to drive the adapter without
the HTTP surface (tests, custom transports):

```python
from istos import Istos, MCPServer

app = Istos()
# ... register @handle endpoints ...
mcp = MCPServer(app, name="my-tools")
reply = await mcp.handle(
    {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
    token=jwt,
)
```

## See also

- [HTTP Gateway](http-gateway.md) — probes, SSE, co-host
- [Capability Discovery](capabilities.md) — broader catalog than MCP
- [API: MCP](../api/http/mcp.md)
