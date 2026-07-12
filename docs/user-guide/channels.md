---
title: Channels & Agent Sessions
---

# Channels & Agent Sessions

`@stream` is one-way: the server yields chunks, the client reads them. When both
sides need to talk — an agent that takes a turn, streams tokens back, then waits
for the next — use `@channel`. The handler gets a `ChannelSession` and drives it
with `send()` / `receive()` (or `async for`) in any order.

## `@channel` — the server

```python
from istos import Istos, ChannelSession

app = Istos(http_port=8080)

@app.channel("agent/chat", ws="/chat")     # ws= exposes it as a WebSocket
async def chat(s: ChannelSession):
    await s.send({"role": "system", "text": "ready"})
    async for msg in s:                     # inbound message
        async for tok in llm.stream(msg):
            await s.send(tok)               # many out per one in
        await s.send({"done": True})
```

`ws=True` serves it at `/<prefix>`; `ws="/path"` picks the path. Messages are
JSON text frames by default (binary for non-UTF-8 serializers). The
`Authorization` header and trace headers from the WebSocket handshake feed the
same authorizer and request envelope as everything else. The handler resolves
`Depends(...)` and can call the rest of the mesh (`query_once`, `publish`,
`stream_query`) while the session is open. When the peer disconnects,
`receive()` raises `ChannelClosed` (so `async for` simply ends).

!!! note "One-way vs two-way"
    Pick by direction, not by transport: `@stream` for server→client output
    (SSE or a Zenoh queryable), `@channel` for full duplex (WebSocket).
    WebSocket is the channel's transport, not a separate primitive.

### Browser WebSocket auth

Browsers cannot set custom headers on `WebSocket`. The gateway reads
`Authorization` from the handshake — fine for non-browser clients, awkward for
`new WebSocket(...)` in a page. Common workarounds (query-param tokens,
subprotocols) are app-level; Istos does not invent one for you yet.

```javascript
const ws = new WebSocket("ws://gateway:8080/chat");
ws.onmessage = (e) => render(JSON.parse(e.data));
ws.onopen = () => ws.send(JSON.stringify("hello"));
```

## Across the fabric

The same `@channel` works node-to-node over Zenoh — a WebSocket gateway on one
node can front an agent on another. Open a session with `open_channel`:

```python
chan = await app.open_channel("agent/chat", token=jwt)
await chan.send("hello")
async for msg in chan:
    render(msg)
await chan.close()
```

Opening is an authorized handshake (`token=` rides the query attachment, so the
channel's authorizer runs before a session exists). Messages then flow over a
per-session pub/sub pair (`{prefix}/{sid}/up` and `.../down`), and a liveliness
token keeps the session alive — when the client `close()`s or crashes, the
server tears the session down and the handler's `async for` ends.

A FastAPI gateway can bridge a browser socket straight through to a remote
agent: pump the socket into `open_channel` and back. See the
[agent channel recipe](../recipes/agent-channel.md).

## Resumable sessions (`SessionStore`)

`@channel(durable=True)` persists every message to a conversation log over the
app's `StoragePlugin` (in-memory by default; use Redis or SQLAlchemy for
multi-process). Each session has a `conversation_id`; reconnect with the same
one and the handler reloads prior turns with `await session.history()`:

```python
@app.channel("agent/chat", durable=True)
async def chat(s: ChannelSession):
    context = [turn["data"] for turn in await s.history()]   # rebuild LLM context
    async for msg in s:
        reply = await agent.step(msg, context)
        await s.send(reply)
```

```python
chan = await app.open_channel("agent/chat")     # conversation_id generated…
save(chan.conversation_id)                       # …persist it client-side
# later, after a reload / crash:
chan = await app.open_channel("agent/chat", conversation_id=load())
```

`history()` returns entries oldest-first as `{dir: "in"|"out", data, ts}`.
Istos stores the transcript and hands it back; it does **not** replay old
messages into the live loop — the handler decides what to do with them.

`SessionStore` is the thin wrapper over that log (`append` / `history`). You
rarely construct it yourself; `@channel(durable=True)` wires one from
`Istos(... storage=...)`. For production, point the app at Redis or SQL so
resume works across pods.

## Declarative clients

`stream_query` and `open_channel` are the imperative path. For a service that is
a mix of senders and receivers, attach the receiving side with decorators —
the client counterparts to `@query`:

```python
@app.stream_client("llm/generate")     # reaches a @stream
async def generate(chunks):            # body gets the live chunk iterator
    async for tok in chunks:
        print(tok, end="")

@app.channel_client("agent/chat")      # reaches a @channel
async def chat(session):               # body gets an open ChannelClient
    await session.send("hi")
    async for msg in session:
        render(msg)

await generate(prompt="hi")            # call kwargs → params, like @query
await chat(token=jwt)                  # session closes when the body returns
```

Call kwargs become the stream/channel params; `token=` carries auth. On a
router, use `@router.stream_client(...)` / `@router.channel_client(...)`; they
wire up on `include_router`.

## Honest limits

- Middleware does **not** wrap `@channel` (same as `@stream`). Auth, validation,
  and DI still run.
- MCP (`enable_mcp=True`) exposes `@handle` tools only — not channels.
- `export_capabilities()` lists channels (and `websocket` when `ws=` is set);
  AsyncAPI generation does not include them yet.
- Unbounded inbound queues — apply your own backpressure if a peer floods.

## Next steps

- [Handlers & Queries (RPC)](rpc.md) — `@handle` / `@stream`
- [HTTP Gateway](http-gateway.md) — FastAPI co-host, SSE, MCP
- [Recipe: Agent channel](../recipes/agent-channel.md)
- [Authorization](authorization.md) — `token=` on `open_channel`
- [Storage](storage.md) — Redis/SQL ledger behind `SessionStore`
