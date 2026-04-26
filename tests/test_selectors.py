import pytest
import asyncio
from unittest.mock import MagicMock
from istos import Istos

@pytest.fixture
def istos():
    return Istos()

@pytest.mark.asyncio
async def test_zenoh_selectors_query_parameters(istos):
    # 1. Define an agent that accepts parameters
    @istos.agent("datastore/users")
    async def get_users(limit: int, role: str = "guest", active: str = "true"):
        return {
            "status": "success",
            "requested_limit": int(limit),
            "requested_role": role,
            "is_active": active == "true"
        }

    # 2. Mock a Zenoh query coming in with parameters
    fake_query = MagicMock()
    fake_query.selector.key_expr = "datastore/users"
    # Parameters simulates the Zenoh Map
    fake_query.selector.parameters = {
        "limit": "5",
        "role": "admin",
        "active": "false"
    }

    # 3. Get the registered wrapper and trigger on_query
    wrapper = istos._agents[0]
    await wrapper.on_query(fake_query)
    
    # 4. Verify the core function replied with the CORRECT calculated logic
    fake_query.reply.assert_called_once()
    args, kwargs = fake_query.reply.call_args
    assert args[0] == "datastore/users"
    
    # args[1] is the serialized payload (bytes)
    result_data = istos._serializer.deserialize(args[1])
    
    assert result_data["status"] == "success"
    assert result_data["requested_limit"] == 5
    assert result_data["requested_role"] == "admin"
    assert result_data["is_active"] is False


@pytest.mark.asyncio
async def test_zenoh_selectors_ignore_extra_kwargs(istos):
    istos = Istos()

    # Agent only asks for `name`
    @istos.agent("greeter")
    def greet(name: str):
        return f"Hello, {name}!"

    fake_query = MagicMock()
    fake_query.selector.key_expr = "greeter"
    fake_query.selector.parameters = {
        "name": "Alice",
        "foo": "baz",
        "bar": "123"
    }

    # Trigger it
    wrapper = istos._agents[0]
    await wrapper.on_query(fake_query)

    # Verify foo and bar were ignored and it executed successfully
    fake_query.reply.assert_called_once()
    args, kwargs = fake_query.reply.call_args
    result_data = istos._serializer.deserialize(args[1])
    
    assert result_data == "Hello, Alice!"

@pytest.mark.asyncio
async def test_query_once_formats_selector(istos, mocker):
    """Test that query_once properly formats **kwargs into a Zenoh Selector string."""
    mock_session = MagicMock()
    istos._session_manager._internal_session = mock_session
    
    # Mock to_thread so it doesn't actually block and just executes the inner sync function
    mocker.patch("asyncio.to_thread", side_effect=lambda func, *args: func(*args))
    
    await istos.query_once("greeter", limit=10, sort="desc")
    
    mock_session.get.assert_called_once()
    args, kwargs = mock_session.get.call_args
    # Verify the selector string was properly urlencoded
    assert args[0] == "greeter?limit=10&sort=desc"
