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
