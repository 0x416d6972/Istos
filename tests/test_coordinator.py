import pytest
from unittest.mock import MagicMock
from istos.messages.serialization import JsonSerializer


# ---- @istos.handle() on sync and async functions ----

@pytest.mark.asyncio
async def test_handler_sync_function(istos):
    """@istos.handle() wraps a sync def correctly."""

    @istos.handle(prefix="math/add")
    def add(a: int, b: int):
        return a + b

    result = await add(2, 3)
    assert result == 5

    raw = await istos._storage.get("math/add")
    data = JsonSerializer().deserialize(raw)
    assert data["func_name"] == "add"
    assert data["total_calls"] == 1


@pytest.mark.asyncio
async def test_handler_async_function(istos):
    """@istos.handle() wraps an async def correctly."""

    @istos.handle(prefix="math/multiply")
    async def multiply(a: int, b: int):
        return a * b

    result = await multiply(4, 5)
    assert result == 20

    raw = await istos._storage.get("math/multiply")
    data = JsonSerializer().deserialize(raw)
    assert data["func_name"] == "multiply"
    assert data["total_calls"] == 1


@pytest.mark.asyncio
async def test_handler_on_class_method(istos):
    """@istos.handle() works as a descriptor on class methods."""

    class Robot:
        @istos.handle(prefix="robot/move")
        def move(self, dist: int):
            return f"moved {dist}m"

    bot = Robot()
    result = await bot.move(10)
    assert result == "moved 10m"


# ---- Coordinator lifecycle ----

@pytest.mark.asyncio
async def test_handler_collects_into_istos(istos):
    """Every @istos.handle() call is tracked internally."""

    @istos.handle(prefix="a/one")
    async def one(): ...

    @istos.handle(prefix="a/two")
    async def two(): ...

    prefixes = [a.prefix for a in istos._handlers]
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
