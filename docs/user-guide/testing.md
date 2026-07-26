# Testing

Istos provides `IstosTestClient` for testing handlers in-process without a live Zenoh network.

## IstosTestClient

```python
import pytest
from istos import Istos
from istos.testing import IstosTestClient

istos = Istos(enable_health=False)

@istos.handle("robot/move")
async def move(distance: int):
    return {"moved": distance}

@pytest.mark.asyncio
async def test_move():
    client = IstosTestClient(istos)
    result = await client.query("robot/move", distance=10)
    assert result == {"moved": 10}
```

### Testing Pub/Sub

```python
@pytest.mark.asyncio
async def test_telemetry():
    received = []

    @istos.subscribe("drone/telemetry")
    async def on_telemetry(data):
        received.append(data)

    client = IstosTestClient(istos)
    await client.publish("drone/telemetry", {"battery": 85})
    assert received[0]["battery"] == 85
```

### Synchronous API

```python
def test_move_sync():
    client = IstosTestClient(istos)
    result = client.run_query("robot/move", distance=5)
    assert result["moved"] == 5
```

### Testing streams

```python
@istos.stream("llm/generate")
async def generate(prompt: str):
    for tok in prompt.split():
        yield tok

@pytest.mark.asyncio
async def test_stream():
    client = IstosTestClient(istos)
    chunks = [c async for c in client.stream("llm/generate", prompt="hi there")]
    assert chunks == ["hi", "there"]
```

### Testing channels

```python
from istos import ChannelSession

@istos.channel("agent/chat")
async def chat(s: ChannelSession):
    await s.send({"role": "system", "text": "ready"})
    msg = await s.receive()
    await s.send({"echo": msg})

@pytest.mark.asyncio
async def test_channel():
    client = IstosTestClient(istos)
    async with client.channel("agent/chat") as chan:
        assert await chan.receive() == {"role": "system", "text": "ready"}
        await chan.send("hello")
        assert await chan.receive() == {"echo": "hello"}
```

Pass `token=` on `query` / `stream` / `channel` to drive an authorizer. For
durable channels, pass `conversation_id=` the same way you would on
`open_channel`. See [Channels](channels.md).

## Testing agents: record, then replay

An agent's behaviour depends on a model you do not control, so neither shape of
test fits: a live model makes the suite slow, flaky, and expensive, and a
hand-written fake drifts from what the model actually does.

So record one real run and keep it as a file. Replaying pins the model *and* the
mesh to what was recorded, leaving your own code as the only variable — the
prompt, the tool catalogue, the loop, the approval gates.

```python
from istos.testing import Trajectory, record_agent, replay

# Once, against a real model — then commit the file.
traj, events = await record_agent(
    model, tools, [], name="refund-flow", user="refund order o-1",
    system="You are support.",
)
traj.save("trajectories/refund-flow.json")

# In CI: no model, no mesh, no network.
@pytest.mark.asyncio
async def test_refund_flow_still_works():
    result = await replay(Trajectory.load("trajectories/refund-flow.json"))
    assert result.ok, result.diff
```

`result.diff` names what moved: a tool that is no longer called, arguments that
changed, a tool missing from the catalogue, a different final message, an
approval that no longer happens. `compare_text=False` ignores the wording and
checks only the tool calls. A recording that went through an approval gate
replays through one too — a stand-in answers the way the recorded human did.

Or from the command line, over a directory of recordings:

```bash
istos eval trajectories/
istos eval trajectories/ --app main:istos    # also check the tools still match
```

`--app` imports your app and checks each recorded tool still exists **and** still
accepts the arguments that were recorded — the drift a replay alone cannot see,
because the stub answered anyway. Exit code is non-zero on any failure, so it
drops straight into CI.

### Eval cases

A replay asks "does the code still do this?". An eval asks "does the agent do the
right thing?" — looser, and about the whole trajectory rather than one return
value:

```python
from istos.testing import EvalCase, format_eval_report, run_eval

cases = [
    EvalCase(
        name="refund happy path",
        user="please refund order o-1",
        expect_tools=["billing-lookup", "billing-refund"],   # in order, gaps allowed
        expect_text="refunded",
        max_tool_calls=4,
    ),
    EvalCase(
        name="no refund without an order",
        user="just give me money",
        forbid_tools=["billing-refund"],
    ),
]

report = await run_eval(cases, model=lambda: OpenAIChatModel(...), tools=tools)
assert report.ok, format_eval_report(report, verbose=True)
```

Each case runs its own conversation and is recorded, so
`report.trajectories()` can be saved and replayed later without the model.
`check=` takes a callable over the `Trajectory` for anything the built-in
assertions do not cover; judgement stays yours — `expect_text` is a substring, not
an LLM grader.

Pass `model=` a factory (not an instance) when the model holds per-run state, so
each case gets a fresh one.

## Testing with Mocks

For unit tests that don't need handler logic, mock the Zenoh session:

```python
from unittest.mock import MagicMock

@pytest.fixture
def istos(mocker):
    app = Istos()
    mock_session = MagicMock()
    app._session_manager._internal_session = mock_session
    return app
```

## Integration Tests

Tests that require a live Zenoh network are marked with `@pytest.mark.integration`:

```bash
# Unit tests only (CI default)
pytest tests/ -m "not integration"

# Include integration tests
pytest tests/
```

## Scaffold a Testable Project

```bash
istos new my-service
cd my-service
pytest test_main.py
```

This creates `main.py` and `test_main.py` with a working `IstosTestClient` example.

## Dependency Overrides

Swap dependencies on the app for tests:

```python
def get_db():
    return real_db

def fake_db():
    return {"connected": True}

istos.dependency_overrides[get_db] = fake_db

# Named DB sessions:
# istos.dependency_overrides[istos.db_session("app")] = fake_session_dep
```

## Next Steps

- [Dependency Injection](dependency-injection.md)
- [CLI](cli.md)
- [API: TestClient](../api/testing/testclient.md)
