import pytest
import asyncio
from unittest.mock import MagicMock
from istos.Istos import Istos
from istos.consistency.register import PrefixRegistery
from istos.messages.serialization import JsonSerializer


@pytest.fixture
def istos():
    return Istos()


# ---- @istos.agent() on sync and async functions ----

@pytest.mark.asyncio
async def test_agent_sync_function(istos):
    """@istos.agent() wraps a sync def correctly."""

    @istos.agent(prefix="math/add")
    def add(a: int, b: int):
        return a + b

    result = await add(2, 3)
    assert result == 5

    raw = await istos._storage.get("math/add")
    data = JsonSerializer().deserialize(raw)
    assert data["func_name"] == "add"
    assert data["total_calls"] == 1


@pytest.mark.asyncio
async def test_agent_async_function(istos):
    """@istos.agent() wraps an async def correctly."""

    @istos.agent(prefix="math/multiply")
    async def multiply(a: int, b: int):
        return a * b

    result = await multiply(4, 5)
    assert result == 20

    raw = await istos._storage.get("math/multiply")
    data = JsonSerializer().deserialize(raw)
    assert data["func_name"] == "multiply"
    assert data["total_calls"] == 1


@pytest.mark.asyncio
async def test_agent_on_class_method(istos):
    """@istos.agent() works as a descriptor on class methods."""

    class Robot:
        @istos.agent(prefix="robot/move")
        def move(self, dist: int):
            return f"moved {dist}m"

    bot = Robot()
    result = await bot.move(10)
    assert result == "moved 10m"


# ---- Coordinator lifecycle ----

@pytest.mark.asyncio
async def test_coordinator_binds_registry(istos, mocker):
    """Istos.run_async() calls registry.register(session) on startup."""
    registry = PrefixRegistery("test/prefix", istos._storage)
    istos.add_registry(registry)

    mock_session = mocker.Mock()
    istos._session_manager = mocker.AsyncMock()
    istos._session_manager.__aenter__.return_value = mock_session

    task = asyncio.create_task(istos.run_async())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    mock_session.declare_queryable.assert_called_once()


@pytest.mark.asyncio
async def test_agent_collects_into_istos(istos):
    """Every @istos.agent() call is tracked internally."""

    @istos.agent(prefix="a/one")
    async def one(): ...

    @istos.agent(prefix="a/two")
    async def two(): ...

    prefixes = [a.prefix for a in istos._agents]
    assert "a/one" in prefixes
    assert "a/two" in prefixes


# ---- @istos.query() decorator tests ----

def test_query_decorator_registers(istos):
    """@istos.query() is tracked internally."""

    @istos.query("math/add")
    def process(result):
        return result

    assert len(istos._queries) == 1
    assert istos._queries[0].prefix == "math/add"


@pytest.mark.asyncio
async def test_query_decorator_calls_zenoh(istos, mocker):
    """@istos.query() queries Zenoh and feeds the result to the function."""

    @istos.query("sensor/temperature")
    def on_temp(data):
        return f"received: {data}"

    # Simulate having an active session
    mock_session = MagicMock()
    istos._session_manager._internal_session = mock_session
    mock_session._internal_session = mock_session # To handle session manager mock

    # Mock session.get to return a fake reply
    fake_sample = MagicMock()
    fake_sample.key_expr = "sensor/temperature"
    fake_sample.payload = istos._serializer.serialize({"temp": 42})

    fake_reply = MagicMock()
    fake_reply.ok = fake_sample

    mock_session.get.return_value = [fake_reply]

    result = await on_temp()
    assert result == "received: {'temp': 42}"


@pytest.mark.asyncio
async def test_query_decorator_on_class_method(istos, mocker):
    """@istos.query() works as a descriptor on class methods."""

    class Dashboard:
        @istos.query("robot/status")
        def show(self, data):
            return f"status: {data}"

    mock_session = MagicMock()
    istos._session_manager._internal_session = mock_session
    mock_session._internal_session = mock_session

    fake_sample = MagicMock()
    fake_sample.key_expr = "robot/status"
    fake_sample.payload = istos._serializer.serialize({"online": True})
    fake_reply = MagicMock()
    fake_reply.ok = fake_sample
    mock_session.get.return_value = [fake_reply]

    dash = Dashboard()
    result = await dash.show()
    assert result == "status: {'online': True}"
