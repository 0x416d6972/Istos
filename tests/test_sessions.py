import pytest # type: ignore
import zenoh # type: ignore
from istos.communication.sessions import ZenohSession, AsyncZenohSession # type: ignore

def test_zenoh_session_sync_lifecycle(mocker):
    """Verifies that ZenohSession opens and closes the session correctly."""
    mock_session = mocker.Mock()
    mock_open = mocker.patch("zenoh.open", return_value=mock_session)
    
    z_session = ZenohSession()
    with z_session as session:
        assert session == mock_session
        mock_open.assert_called_once()
    
    # Verify close was called on exit
    mock_session.close.assert_called_once()

@pytest.mark.asyncio
async def test_zenoh_session_async_lifecycle(mocker):
    """Verifies that AsyncZenohSession opens and closes the session correctly in async mode."""
    mock_session = mocker.Mock()
    mock_open = mocker.patch("zenoh.open", return_value=mock_session)
    
    z_session = AsyncZenohSession()
    async with z_session as session:
        assert session == mock_session
        mock_open.assert_called_once()
            
    mock_session.close.assert_called_once()

def test_zenoh_session_info_sync(mocker):
    """Verifies get_info handles different Zenoh info implementations (method vs attribute)."""
    # CASE 1: info is a method
    mock_session = mocker.Mock()
    mock_info_obj = mocker.Mock()
    mock_info_obj.zid = "test-zid-1"
    mock_session.info.return_value = mock_info_obj
    
    mocker.patch("zenoh.open", return_value=mock_session)
    z_session = ZenohSession()
    with z_session:
        info = z_session.get_info()
        assert info["zid"] == "test-zid-1"

    # CASE 2: info is a property/attribute (simulated by a non-callable object)
    class PlainInfo:
        def __init__(self, zid):
            self.zid = zid

    mock_info_obj_2 = PlainInfo("test-zid-2")
    
    class FakeSession:
        def __init__(self, info):
            self.info = info
        def close(self):
            pass

    mock_session_2 = FakeSession(mock_info_obj_2)
    
    mocker.patch("zenoh.open", return_value=mock_session_2)
    z_session_2 = ZenohSession()
    with z_session_2:
        info = z_session_2.get_info()
        assert info["zid"] == "test-zid-2"
