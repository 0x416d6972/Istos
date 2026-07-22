---
title: Agent Loop
---

# Agent loop

`@channel` keeps a duplex session open; `@handle` is a tool on the fabric.
The agent loop is the glue: the model plans, tools run as `query_once` on key
expressions, results go back into the conversation, and the model answers.

An agent is a **service on a key** — not an in-process graph. Tools are other
services on the same mesh (or local callables for tests).

## Pieces

| Piece | Role |
|-------|------|
| `MeshTool` / `tools_from_handlers` | Catalogue of callable mesh endpoints |
| `Model` / `OpenAIChatModel` | One completion turn (OpenAI-compatible `/v1/chat/completions`) |
| `run_agent` | plan → tool → observe until text or `max_steps` |
| `drive_channel` | Reload durable history, then run the loop per inbound turn |

```python
from istos import Istos, ChannelSession
from istos.agent import OpenAIChatModel, drive_channel, tools_from_handlers

app = Istos(http_port=8080)

@app.handle("math/add")
async def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b

tools = tools_from_handlers(app, prefixes=["math/add"])
model = OpenAIChatModel(
    base_url="http://127.0.0.1:1234/v1",
    model="qwen/qwen3.5-9b",
)

@app.channel("agent/chat", ws="/chat", durable=True)
async def chat(s: ChannelSession):
    await drive_channel(
        s, model, tools,
        system="You are a calculator assistant. Use math/add when needed.",
    )

if __name__ == "__main__":
    app.run()
```

`tools_from_handlers` builds the same name / docstring / JSON Schema catalogue
MCP uses (`math/add` → tool `math-add`). Pass `prefixes=` to whitelist. Plumbing
under `.istos/` is skipped.

## Events

`run_agent` yields `AgentEvent` values. `drive_channel` sends each (except
`done`) as a JSON object on the session:

| `kind` | Meaning |
|--------|---------|
| `tool_call` | Model asked to run a tool (`name`, `arguments`, `tool_call_id`) |
| `tool_result` | Tool returned (`content`); `error=True` when it raised |
| `message` | Final assistant text for this turn (`content`) |
| `done` | Turn finished (not sent on the channel) |

```python
async for event in run_agent(model, tools, messages, token=jwt):
    if event.kind == "message":
        print(event.content)
```

`messages` is mutated in place so you can keep a multi-turn list across
`run_agent` calls. Mesh tool calls forward `token=` on `query_once`, so the
tool's authorizer still runs.

`drive_channel(..., send_events=False)` sends only the final `message` content
(plain string) — useful when the client does not want tool-call frames.

## Remote tools

A tool does not have to live on the same process. Point `MeshTool` at another
node's prefix; `query_once` finds it on the fabric:

```python
from istos.agent import MeshTool

tools = [
    MeshTool(
        "billing/invoice",
        app=app,
        description="Create an invoice",
        parameters={
            "type": "object",
            "properties": {
                "customer": {"type": "string"},
                "amount": {"type": "number"},
            },
            "required": ["customer", "amount"],
        },
    ),
]
```

## Own model

Anything with `async def complete(messages, *, tools=None) -> ModelReply` works.
`OpenAIChatModel` is the battery for OpenAI, LM Studio, vLLM, and similar. Tool
call arguments are parsed from the usual OpenAI `tool_calls` shape.

## Handoff between agents

For a triage-and-specialists setup, give each role its own `Agent` — a model,
its tools, a system prompt, and the agents it may hand off to — and drive the
channel with `drive_agents`. The model transfers by calling a synthetic
`transfer_to_<name>` tool; the loop swaps the active agent but keeps the shared
message history, so context carries across.

```python
from istos.agent import Agent, drive_agents

billing = Agent(
    "billing", model, tools=[refund_tool],
    system="You handle refunds.", description="refunds and billing questions",
)
router = Agent(
    "router", model, handoffs=[billing],
    system="Route the user to the right specialist.",
)
billing.handoffs = [router]        # return handoff: hand back when done

@app.channel("agent/chat", durable=True)
async def chat(s):
    await drive_agents(s, router, token=jwt)   # forwarded to every tool call
```

- Handoff graphs may cycle, so a specialist can hand back to the router
  (triage → specialist → triage).
- The active agent persists across turns within a session, and is restored on
  reconnect from persisted `handoff` frames (`send_events=True`); otherwise a
  resumed session restarts at the entry agent.
- `token` forwards to whichever agent's tools run, so authorizers see the
  original principal no matter how many handoffs occurred.
- A remote specialist on another node is reached as a **mesh tool**
  (`query_once` on its key), not an in-process handoff — handoff switches the
  local driving agent; the mesh is how you call across nodes.

## Tracing

With `Istos(enable_tracing=True)` the loop emits an `istos.agent.completion` span
per model turn and an `istos.agent.tool` span per tool call, nested under the
channel handler's request span — so an agent's model and tool work shows up
inside the same distributed trace that already spans hops over Zenoh.

Completion spans carry GenAI attributes (`gen_ai.response.model`,
`gen_ai.usage.input_tokens` / `output_tokens`, finish reason) and, for
multi-agent, the active `istos.agent.name`. Token counts come from `ModelReply`
(`model` / `finish_reason` / `usage`), which `OpenAIChatModel` fills from the
response; a custom model that leaves them unset simply omits those attributes.
Tool spans carry the tool name and prefix and flag errors. Everything is a no-op
until tracing is configured, so OpenTelemetry stays an optional dependency.

## Honest limits

- The loop is **not** a DAG engine. Branching and long-running workflows stay on
  queues (`chain` / `group` / `chord`) or your own control flow.
- Durable channel history reconstructs the full tool transcript on reconnect
  (assistant `tool_calls` + `tool` results), so a resumed session keeps its tool
  context. A tool call with no recorded result (a crash mid-tool) is dropped to
  keep the message sequence valid; pass `include_tools=False` for text only.
- `OpenAIChatModel` is non-streaming completions. For token streaming without
  tools, keep using `@stream` / `stream_query` as before.
- MCP and `tools_from_handlers` share the catalogue idea; MCP still lists
  **this** node's `@handle` only. An agent can call remote prefixes that MCP
  on this node does not advertise.

See also: [Channels](channels.md), [MCP](mcp.md),
[agent channel recipe](../recipes/agent-channel.md),
[agent with tools recipe](../recipes/agent-tools.md).
