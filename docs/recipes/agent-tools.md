# Recipe: Agent with mesh tools

One process exposes tools as `@handle`. Another (or the same) exposes an agent
as `@channel`. The loop calls tools over Zenoh via `query_once`.

## Tool node

```python
# tools.py
from istos import Istos
from istos.communication.config import IstosZenohConfig

app = Istos(
    service_name="math",
    config=IstosZenohConfig(mode="client", connect_endpoints=["tcp/router:7447"]),
)

@app.handle("math/add")
async def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b

@app.handle("math/mul")
async def mul(a: int, b: int) -> int:
    """Multiply two integers."""
    return a * b

if __name__ == "__main__":
    app.run()
```

## Agent node

```python
# agent.py
from istos import Istos, ChannelSession, MeshTool
from istos.agent import OpenAIChatModel, drive_channel
from istos.communication.config import IstosZenohConfig

app = Istos(
    service_name="agent",
    http_port=8080,
    config=IstosZenohConfig(mode="client", connect_endpoints=["tcp/router:7447"]),
)

# Tools live on the math node — same prefixes, this app's query_once.
tools = [
    MeshTool(
        "math/add",
        app=app,
        description="Add two integers",
        parameters={
            "type": "object",
            "properties": {
                "a": {"type": "integer"},
                "b": {"type": "integer"},
            },
            "required": ["a", "b"],
        },
    ),
    MeshTool(
        "math/mul",
        app=app,
        description="Multiply two integers",
        parameters={
            "type": "object",
            "properties": {
                "a": {"type": "integer"},
                "b": {"type": "integer"},
            },
            "required": ["a", "b"],
        },
    ),
]

model = OpenAIChatModel(
    base_url="http://127.0.0.1:1234/v1",
    model="qwen/qwen3.5-9b",
)

@app.channel("agent/chat", ws="/chat", durable=True)
async def chat(s: ChannelSession):
    await drive_channel(
        s, model, tools,
        system="You are a calculator. Prefer tools over mental arithmetic.",
    )

if __name__ == "__main__":
    app.run()
```

When the tool handlers are on the **same** process, use
`tools_from_handlers(app, prefixes=["math/add", "math/mul"])` instead of
hand-built `MeshTool` entries.

## Try it

```bash
# terminal 1 — zenoh router (or multicast peer mode without a router)
# terminal 2
python tools.py
# terminal 3
python agent.py
```

```bash
# WebSocket client (websocat or similar)
websocat ws://127.0.0.1:8080/chat
> {"text": "what is 6 times 7?"}
```

Frames on the wire look like `{"kind":"tool_call",...}`,
`{"kind":"tool_result",...}`, then `{"kind":"message","content":"..."}`.
Pass `send_events=False` to `drive_channel` if you only want the final string.

Same pattern with FastAPI in front:
[Agent channel](agent-channel.md) — bridge the browser socket with
`open_channel("agent/chat")` and leave the loop on the agent node.
