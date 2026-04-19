import pytest
import asyncio
import zenoh
from unittest.mock import MagicMock
from istos.Istos import Istos

@pytest.fixture
def istos():
    return Istos()

@pytest.mark.asyncio
async def test_delete_once(istos, mocker):
    """Test that istos.delete_once calls session.delete."""
    mock_session = MagicMock()
    istos._session_manager._internal_session = mock_session

    await istos.delete_once("robot/cache")
    
    # verify it was deleted
    mock_session.delete.assert_called_once()
    args, kwargs = mock_session.delete.call_args
    assert args[0] == "robot/cache"

@pytest.mark.asyncio
async def test_publish_shm(istos, mocker):
    """Test that @istos.publish(use_shm=True) allocates from provider."""
    mock_session = MagicMock()
    mock_provider = MagicMock()
    mock_sbuf = bytearray(100) # acts like shm buffer
    mock_provider.alloc.return_value = mock_sbuf

    istos._session_manager._internal_session = mock_session
    istos._shm_provider = mock_provider

    @istos.publish("video/stream", use_shm=True)
    async def send_frame():
        return "FRAME_DATA"

    result = await send_frame()
    assert result == "FRAME_DATA"
    
    # verify allocator was called
    mock_provider.alloc.assert_called_once()
    
    # verify it was published with buffer
    mock_session.put.assert_called_once()
    args, kwargs = mock_session.put.call_args
    assert args[0] == "video/stream"
    assert b"FRAME_DATA" in args[1]

@pytest.mark.asyncio
async def test_liveliness_decorator(istos):
    """Test that @istos.on_liveliness parses data and triggers the func."""
    received = []

    @istos.on_liveliness("robot/**")
    def on_robot_presence(key, is_alive):
        received.append((key, is_alive))
        
    wrapper = istos._liveliness_subs[0]
    assert wrapper.prefix == "robot/**"
    
    # Simulate an incoming Zenoh sample (PUT = alive)
    fake_sample = MagicMock()
    fake_sample.kind = getattr(zenoh.SampleKind, 'PUT', 0)  # Use 0 if fallback
    fake_sample.key_expr = "robot/one"
    
    await wrapper.on_sample(fake_sample)
    
    # Verify the decorated function got the data
    assert received[0] == ("robot/one", True)

@pytest.mark.asyncio
async def test_liveliness_binds_to_zenoh(istos, mocker):
    """Test that run_async registers the liveliness subscriber and token."""
    mock_session = mocker.Mock()
    mock_liveliness_ops = mocker.Mock()
    mock_session.liveliness.return_value = mock_liveliness_ops

    istos._session_manager = mocker.AsyncMock()
    istos._session_manager.__aenter__.return_value = mock_session

    istos.declare_liveliness("robot/two")
    @istos.on_liveliness("robot/**")
    def on_robot(key, state): ...

    task = asyncio.create_task(istos.run_async())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Validate token was declared
    mock_liveliness_ops.declare_token.assert_called_once_with("robot/two")
    
    # Validate subscriber was declared
    mock_liveliness_ops.declare_subscriber.assert_called_once()
    args, kwargs = mock_liveliness_ops.declare_subscriber.call_args
    assert args[0] == "robot/**"
