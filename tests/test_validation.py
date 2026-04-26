import pytest
import asyncio
from unittest.mock import MagicMock
from pydantic import BaseModel
from istos import Istos
from istos.core.validation import validate_params, SchemaValidationError


@pytest.fixture
def istos():
    return Istos()


# ------------------------------------------------------------------
# validate_params unit tests
# ------------------------------------------------------------------

class TestAutoCoercion:
    """Test that string params from Zenoh are coerced to the correct types."""

    def test_int_coercion(self):
        def func(distance: int): ...
        result = validate_params(func, {"distance": "42"})
        assert result == {"distance": 42}
        assert isinstance(result["distance"], int)

    def test_float_coercion(self):
        def func(speed: float): ...
        result = validate_params(func, {"speed": "3.14"})
        assert result == {"speed": 3.14}

    def test_bool_coercion(self):
        def func(active: bool): ...
        result = validate_params(func, {"active": True})
        assert result["active"] is True

    def test_multiple_params(self):
        def func(x: int, y: float, name: str): ...
        result = validate_params(func, {"x": "10", "y": "2.5", "name": "robot1"})
        assert result == {"x": 10, "y": 2.5, "name": "robot1"}

    def test_default_values_used(self):
        def func(speed: int = 50): ...
        result = validate_params(func, {})
        assert result == {"speed": 50}

    def test_invalid_type_raises(self):
        def func(distance: int): ...
        with pytest.raises(SchemaValidationError):
            validate_params(func, {"distance": "not_a_number"})


class TestBaseModelValidation:
    """Test that Pydantic BaseModel parameters are fully validated."""

    def test_valid_model(self):
        class MoveRequest(BaseModel):
            distance: int
            speed: str = "normal"

        def func(request: MoveRequest): ...
        result = validate_params(func, {"distance": "10", "speed": "fast"})
        assert "request" in result
        assert isinstance(result["request"], MoveRequest)
        assert result["request"].distance == 10
        assert result["request"].speed == "fast"

    def test_model_with_defaults(self):
        class MoveRequest(BaseModel):
            distance: int
            speed: str = "normal"

        def func(request: MoveRequest): ...
        result = validate_params(func, {"distance": "20"})
        assert result["request"].distance == 20
        assert result["request"].speed == "normal"

    def test_invalid_model_raises(self):
        class MoveRequest(BaseModel):
            distance: int

        def func(request: MoveRequest): ...
        with pytest.raises(SchemaValidationError):
            validate_params(func, {"distance": "not_a_number"})


class TestPassthrough:
    """Test that untyped functions skip validation."""

    def test_no_hints_passthrough(self):
        def func(x, y): ...
        # No type hints → params pass through untouched
        result = validate_params(func, {"x": "hello", "y": "42"})
        assert result == {"x": "hello", "y": "42"}


# ------------------------------------------------------------------
# Integration: agent_wrapper.on_query with validation
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_agent_coerces_params(istos):
    """Agent with typed params auto-coerces string values from Zenoh."""
    @istos.agent("robot/move")
    async def move(distance: int, speed: str = "normal"):
        return {"moved": distance, "speed": speed}

    fake_query = MagicMock()
    fake_query.selector.key_expr = "robot/move"
    fake_query.selector.parameters = {"distance": "15", "speed": "fast"}

    wrapper = istos._agents[0]
    await wrapper.on_query(fake_query)

    fake_query.reply.assert_called_once()
    args, _ = fake_query.reply.call_args
    result = istos._serializer.deserialize(args[1])
    assert result["moved"] == 15
    assert result["speed"] == "fast"


@pytest.mark.asyncio
async def test_agent_rejects_invalid_params(istos):
    """Agent rejects bad params and replies with a validation error."""
    @istos.agent("robot/move")
    async def move(distance: int):
        return {"moved": distance}

    fake_query = MagicMock()
    fake_query.selector.key_expr = "robot/move"
    fake_query.selector.parameters = {"distance": "not_a_number"}

    wrapper = istos._agents[0]
    await wrapper.on_query(fake_query)

    # Should still reply, but with an error payload
    fake_query.reply.assert_called_once()
    args, _ = fake_query.reply.call_args
    result = istos._serializer.deserialize(args[1])
    assert result["error"] == "validation_error"


@pytest.mark.asyncio
async def test_agent_with_pydantic_model(istos):
    """Agent accepting a Pydantic BaseModel validates and hydrates it."""
    class MoveRequest(BaseModel):
        distance: int
        speed: str = "normal"

    @istos.agent("robot/move")
    async def move(request: MoveRequest):
        return {"moved": request.distance, "speed": request.speed}

    fake_query = MagicMock()
    fake_query.selector.key_expr = "robot/move"
    fake_query.selector.parameters = {"distance": "99", "speed": "turbo"}

    wrapper = istos._agents[0]
    await wrapper.on_query(fake_query)

    fake_query.reply.assert_called_once()
    args, _ = fake_query.reply.call_args
    result = istos._serializer.deserialize(args[1])
    assert result["moved"] == 99
    assert result["speed"] == "turbo"


@pytest.mark.asyncio
async def test_agent_untyped_still_works(istos):
    """Untyped agents still work without validation (backward compat)."""
    @istos.agent("robot/echo")
    def echo(message):
        return {"echo": message}

    fake_query = MagicMock()
    fake_query.selector.key_expr = "robot/echo"
    fake_query.selector.parameters = {"message": "hello"}

    wrapper = istos._agents[0]
    await wrapper.on_query(fake_query)

    fake_query.reply.assert_called_once()
    args, _ = fake_query.reply.call_args
    result = istos._serializer.deserialize(args[1])
    assert result["echo"] == "hello"
