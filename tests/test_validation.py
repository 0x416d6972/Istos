import pytest
from unittest.mock import MagicMock
from pydantic import BaseModel
from istos.core.validation import validate_params, SchemaValidationError


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
# Integration: handler_wrapper.on_query with validation
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handler_coerces_params(istos):
    """Handler with typed params auto-coerces string values from Zenoh."""
    @istos.handle("robot/move")
    async def move(distance: int, speed: str = "normal"):
        return {"moved": distance, "speed": speed}

    fake_query = MagicMock()
    fake_query.selector.key_expr = "robot/move"
    fake_query.selector.parameters = {"distance": "15", "speed": "fast"}

    wrapper = istos._handlers[0]
    await wrapper.on_query(fake_query)

    fake_query.reply.assert_called_once()
    args, _ = fake_query.reply.call_args
    result = istos._serializer.deserialize(args[1])
    assert result["moved"] == 15
    assert result["speed"] == "fast"


@pytest.mark.asyncio
async def test_handler_rejects_invalid_params(istos):
    """Handler rejects bad params and replies with a validation error."""
    @istos.handle("robot/move")
    async def move(distance: int):
        return {"moved": distance}

    fake_query = MagicMock()
    fake_query.selector.key_expr = "robot/move"
    fake_query.selector.parameters = {"distance": "not_a_number"}

    wrapper = istos._handlers[0]
    await wrapper.on_query(fake_query)

    # Should still reply, but with an error payload
    fake_query.reply.assert_called_once()
    args, _ = fake_query.reply.call_args
    result = istos._serializer.deserialize(args[1])
    assert result["error"] == "validation_error"


@pytest.mark.asyncio
async def test_handler_with_pydantic_model(istos):
    """Handler accepting a Pydantic BaseModel validates and hydrates it."""
    class MoveRequest(BaseModel):
        distance: int
        speed: str = "normal"

    @istos.handle("robot/move")
    async def move(request: MoveRequest):
        return {"moved": request.distance, "speed": request.speed}

    fake_query = MagicMock()
    fake_query.selector.key_expr = "robot/move"
    fake_query.selector.parameters = {"distance": "99", "speed": "turbo"}

    wrapper = istos._handlers[0]
    await wrapper.on_query(fake_query)

    fake_query.reply.assert_called_once()
    args, _ = fake_query.reply.call_args
    result = istos._serializer.deserialize(args[1])
    assert result["moved"] == 99
    assert result["speed"] == "turbo"


@pytest.mark.asyncio
async def test_handler_untyped_still_works(istos):
    """Untyped handlers still work without validation (backward compat)."""
    @istos.handle("robot/echo")
    def echo(message):
        return {"echo": message}

    fake_query = MagicMock()
    fake_query.selector.key_expr = "robot/echo"
    fake_query.selector.parameters = {"message": "hello"}

    wrapper = istos._handlers[0]
    await wrapper.on_query(fake_query)

    fake_query.reply.assert_called_once()
    args, _ = fake_query.reply.call_args
    result = istos._serializer.deserialize(args[1])
    assert result["echo"] == "hello"


# ------------------------------------------------------------------
# @subscribe payload validation (network-input boundary)
# ------------------------------------------------------------------

class TestSubscribePayloadValidation:
    """A subscriber's payload is untrusted network input — validate/coerce it
    against the first positional parameter's type hint, like @handle does."""

    @pytest.mark.asyncio
    async def test_basemodel_payload_coerced(self, istos):
        class Telemetry(BaseModel):
            battery: float
            altitude: int

        received = {}

        @istos.subscribe("drone/telemetry")
        def on_t(data: Telemetry):
            received["v"] = data

        wrapper = istos._subscribers[0]
        await wrapper({"battery": 80, "altitude": 100})

        assert isinstance(received["v"], Telemetry)
        assert received["v"].battery == 80.0

    @pytest.mark.asyncio
    async def test_scalar_payload_coerced(self, istos):
        received = {}

        @istos.subscribe("n/count")
        def on_n(data: int):
            received["v"] = data

        wrapper = istos._subscribers[0]
        await wrapper("42")

        assert received["v"] == 42
        assert isinstance(received["v"], int)

    @pytest.mark.asyncio
    async def test_invalid_payload_raises(self, istos):
        class Telemetry(BaseModel):
            battery: float

        @istos.subscribe("drone/telemetry")
        def on_t(data: Telemetry): ...

        wrapper = istos._subscribers[0]
        with pytest.raises(SchemaValidationError):
            await wrapper({"battery": "not-a-number"})

    @pytest.mark.asyncio
    async def test_untyped_payload_passthrough(self, istos):
        received = {}

        @istos.subscribe("raw/data")
        def on_r(data):
            received["v"] = data

        wrapper = istos._subscribers[0]
        await wrapper({"anything": 1})

        assert received["v"] == {"anything": 1}
