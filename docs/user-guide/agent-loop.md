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
| `tools_from_discovery` | The same catalogue, read off the fabric's manifests |
| `Model` / `OpenAIChatModel` | One completion turn (OpenAI-compatible `/v1/chat/completions`) |
| `run_agent` | plan → tool → observe until text or `max_steps` |
| `drive_channel` | Reload durable history, then run the loop per inbound turn |
| `app.approvals()` | Human approval before an irreversible tool runs |

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
| `approval_request` | Waiting on a human before a gated tool runs (`approval_id`) |
| `approval_decision` | The human answered; `error=True` when refused |
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

Writing another service's schema by hand goes stale. That schema is already
published — [capability discovery](capabilities.md) serves it — so read it
instead:

```python
from istos import tools_from_discovery

# Every remote @handle, with the owner's schema and docstring. Needs an open
# session, so call it from a lifespan or a handler.
tools = await tools_from_discovery(app, services=["billing", "search"])
```

Only `handle` entries become tools — a mesh tool is a `query_once`, so streams
and channels are skipped. `prefixes=` whitelists exact keys. The result is a
snapshot: call it again to pick up nodes that joined later.

## Human approval for irreversible tools

A tool that moves money or deletes data should not fire because a model felt like
it. The endpoint's **owner** declares the requirement, so every agent that
discovers the tool inherits it:

```python
@app.handle("billing/refund", approval="moves real money")
async def refund(order_id: str) -> dict:
    """Refund an order."""
    ...
```

The agent node passes a gate to the loop:

```python
gate = app.approvals(timeout_s=600, authorizer=require_roles("ops"))

@app.channel("agent/chat", ws="/chat", durable=True)
async def chat(s: ChannelSession):
    await drive_channel(s, model, tools, approvals=gate)
```

Now a call to `billing/refund` suspends the turn: an `approval_request` event
goes out (with `approval_id`), and the tool runs only after a yes. Nothing polls
— the waiting agent holds an `asyncio.Event` that the decision wakes.

The decision arrives **over the fabric**, from any node:

```python
from istos import decide_approval, list_approvals

for req in await list_approvals(app):
    print(req["id"], req["tool"], req["arguments"], req["reason"])

await decide_approval(app, req["id"], approved=True, by="amir")
# or correct it instead of refusing outright:
await gate.approve(req["id"], by="amir", arguments={"order_id": "o-42"})
```

`list_approvals` asks `.istos/approvals/*` and `decide_approval` asks
`.istos/approvals/*/decide`; each waiting node has keys of its own, so the
wildcard reaches all of them and only the node holding the request acts. Put the
[HTTP gateway](http-gateway.md) in front for a browser UI.

Fail-closed, deliberately:

- A denial or a timeout never runs the tool. Both come back to the model as a
  failed `tool_result` carrying the reason, so it can tell the user rather than
  retry blindly.
- An undecided request expires after `timeout_s` (`None` waits forever).
- Passing a gated tool with **no** gate raises `ValueError` at the start of the
  run — a missing gate can never read as a pass.
- `approval=` is *advisory*: it tells agents to stop, it does not stop a peer
  that queries the key directly. Keep the handler's `authorizer` as the real
  gate. Deciding is privileged too, so give `app.approvals()` an authorizer —
  Istos warns with `IstosSecurityWarning` when the decide key is left open.

A caller can also gate a prefix its owner did not declare:
`tools_from_discovery(app, approval=["search/purge"])`.

Requests are written through to the app's storage, so with Redis or SQLAlchemy an
operator can still list what was outstanding after a restart. The waiter itself
is in-memory by nature: an agent that crashed is no longer waiting, so its
recovered request is stale and expires.

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
- An approval waits in the process that filed it. Run several replicas of one
  agent service and each holds its own pending set — which the fan-out decide
  handles (every replica is asked, the holder answers), but a replica that dies
  takes its waiter with it.

See also: [Channels](channels.md), [MCP](mcp.md),
[agent channel recipe](../recipes/agent-channel.md),
[agent with tools recipe](../recipes/agent-tools.md).
