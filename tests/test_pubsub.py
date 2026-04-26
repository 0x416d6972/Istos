import pytest
import asyncio
from unittest.mock import MagicMock
from istos import Istos

@pytest.fixture
def istos():
    return Istos()

@pytest.mark.asyncio
async def test_publish_decorator(istos, mocker):
    """Test that @istos.publish() calculations are sent via session.put()."""
    @istos.publish("drone/telemetry")
    def get_telemetry():
        return {"battery": 85}

    mock_session = MagicMock()
    istos._session_manager._internal_session = mock_session

    result = await get_telemetry()
    assert result == {"battery": 85}
    
    # verify it was published
    mock_session.put.assert_called_once()
    args, kwargs = mock_session.put.call_args
    assert args[0] == "drone/telemetry"

@pytest.mark.asyncio
async def test_publish_once(istos, mocker):
    """Test that istos.publish_once sends raw data."""
    mock_session = MagicMock()
    istos._session_manager._internal_session = mock_session

    await istos.publish_once("robot/status", {"online": True})
    
    # verify it was published
    mock_session.put.assert_called_once()
    args, kwargs = mock_session.put.call_args
    assert args[0] == "robot/status"

@pytest.mark.asyncio
async def test_subscribe_decorator(istos):
    """Test that @istos.subscribe on_sample parses data and triggers the func."""
    received_data = None

    @istos.subscribe("drone/telemetry")
    def on_telemetry(data):
        nonlocal received_data
        received_data = data
        
    # Get the registered wrapper
    wrapper = istos._subscribers[0]
    assert wrapper.prefix == "drone/telemetry"
    
    # Simulate an incoming Zenoh sample
    fake_sample = MagicMock()
    fake_sample.payload = istos._serializer.serialize({"battery": 90})
    
    # Call the on_sample listener
    await wrapper.on_sample(fake_sample)
    
    # Verify the decorated function got the data
    assert received_data == {"battery": 90}


@pytest.mark.asyncio
async def test_subscribe_binds_to_zenoh(istos, mocker):
    """Test that run_async registers the subscriber to the zenoh session."""
    @istos.subscribe("drone/telemetry")
    def on_telemetry(data): ...

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

    # Validate declare_subscriber was called inside `_bind_subscribers`
    mock_session.declare_subscriber.assert_called_once()
    args, kwargs = mock_session.declare_subscriber.call_args
    assert args[0] == "drone/telemetry"
