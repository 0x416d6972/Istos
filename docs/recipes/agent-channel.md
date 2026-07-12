# Recipe: FastAPI gateway + agent channel

Bridge a browser (or any WebSocket client) through FastAPI into a remote
`@channel` agent on the Zenoh fabric. FastAPI owns HTTP; Istos owns the mesh.

## Agent node

```python
# agent.py
from istos import Istos, ChannelSession
from istos.communication.config import IstosZenohConfig

mesh = Istos(
    config=IstosZenohConfig(
        mode="client",
        connect_endpoints=["tcp/router:7447"],
    ),
    # storage=RedisStoragePlugin(...)  # for durable=True across pods
)

@mesh.channel("agent/chat", durable=True)
async def chat(s: ChannelSession):
    context = [t["data"] for t in await s.history()]
    await s.send({"role": "system", "text": "ready", "conversation_id": s.conversation_id})
    async for msg in s:
        # replace with your SLM / tool loop
        reply = {"role": "assistant", "text": f"echo: {msg}", "context_len": len(context)}
        context.append(msg)
        context.append(reply)
        await s.send(reply)

if __name__ == "__main__":
    mesh.run()
```

## FastAPI edge

```python
# gateway.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from istos import Istos
from istos.http.asgi import lifespan
from istos.communication.config import IstosZenohConfig

mesh = Istos(
    config=IstosZenohConfig(
        mode="client",
        connect_endpoints=["tcp/router:7447"],
    ),
)
api = FastAPI(lifespan=lifespan(mesh))

@api.websocket("/chat")
async def chat_ws(ws: WebSocket):
    await ws.accept()
    # Browsers cannot set Authorization on WebSocket — authenticate earlier
    # (cookie / short-lived ticket) and pass token= when you have one.
    conversation_id = ws.query_params.get("conversation_id")
    chan = await mesh.open_channel(
        "agent/chat",
        conversation_id=conversation_id,
        # token=jwt,
    )
    try:
        await ws.send_json({"conversation_id": chan.conversation_id})
        while True:
            msg = await ws.receive_text()
            await chan.send(msg)
            reply = await chan.receive()
            await ws.send_json(reply)
    except WebSocketDisconnect:
        pass
    finally:
        await chan.close()
```

Run the agent with `python agent.py`, the gateway with
`uvicorn gateway:api --host 0.0.0.0 --port 8000`. Do **not** also set
`Istos(http_port=...)` on the FastAPI process — leave HTTP to uvicorn
(`serving()` defaults to `serve_http=False`).

## Embedded Istos WebSocket (no FastAPI)

If the agent node itself exposes HTTP:

```python
app = Istos(http_port=8080, authorizer=jwt)

@app.channel("agent/chat", ws="/chat", durable=True)
async def chat(s: ChannelSession):
    ...
```

Resume from a browser with the query param (auth still needs a non-browser
client that can send `Authorization`, or an open/Public channel for demos):

```javascript
const id = localStorage.getItem("cid");
const q = id ? `?conversation_id=${id}` : "";
const ws = new WebSocket(`ws://host:8080/chat${q}`);
ws.onmessage = (e) => {
  const msg = JSON.parse(e.data);
  if (msg.conversation_id) localStorage.setItem("cid", msg.conversation_id);
};
```

## Resume after reconnect

Persist `conversation_id` from the first `open_channel` (or from the JSON the
gateway sent). On reconnect:

```python
chan = await mesh.open_channel("agent/chat", conversation_id=saved_id)
```

The durable handler reloads history via `session.history()`.

## See also

- [Channels & Agent Sessions](../user-guide/channels.md)
- [HTTP Gateway — co-hosting](../user-guide/http-gateway.md)
- [MCP tools](../user-guide/mcp.md)
- [Storage](../user-guide/storage.md)
