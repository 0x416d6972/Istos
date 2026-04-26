import pytest
import asyncio
from contextlib import asynccontextmanager
from unittest.mock import MagicMock
from istos import Istos

pytestmark = pytest.mark.asyncio

async def test_lifespan_execution():
    """
    Test that the lifespan context manager properly executes its startup logic 
    BEFORE the Zenoh session begins, and teardown logic AFTER the session closes.
    """
    state = []
    
    @asynccontextmanager
    async def my_lifespan(app: Istos):
        state.append("startup")
        assert isinstance(app, Istos)
        yield
        state.append("shutdown")

    class MockSessionManager:
        async def __aenter__(self):
            state.append("session_start")
            mock_session = MagicMock()
            # Mock the liveliness method to prevent errors during shutdown calls
            mock_session.liveliness.return_value = MagicMock()
            return mock_session
            
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            state.append("session_end")

    # Pass in the lifespan and mock session manager
    app = Istos(
        session_manager=MockSessionManager(),
        lifespan=my_lifespan
    )
    
    # Start the application as a background task
    task = asyncio.create_task(app.run_async())
    
    # Give it a tiny moment to hit the infinite `while True:` event loop
    await asyncio.sleep(0.1)
    
    # Cancel the task to trigger the shutdown sequences
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        pass
        
    # The order MUST be: 
    # 1. Lifespan Startup 
    # 2. Session Start 
    # 3. Session End 
    # 4. Lifespan Shutdown
    assert state == ["startup", "session_start", "session_end", "shutdown"]
