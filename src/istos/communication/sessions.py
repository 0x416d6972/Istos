import zenoh
import asyncio
from typing import Optional, Any, Protocol, runtime_checkable

@runtime_checkable
class SessionManager(Protocol):
    """
    Pure interface for something that provides access to a Zenoh session.
    """
    @property
    def session(self) -> Any:
        ...

    def get_info(self) -> dict[str, Any]:
        """
        Returns info about the current session.
        """
        ...


class ZenohSession:
    """
    Synchronous Zenoh session manager.
    Implements the SessionManager protocol structurally.
    """
    def __init__(self, config: Optional[zenoh.Config] = None):
        self._config = config or zenoh.Config()
        self._internal_session: Optional[zenoh.Session] = None

    @property
    def session(self) -> Optional[zenoh.Session]:
        return self._internal_session

    def __enter__(self) -> zenoh.Session:
        self._internal_session = zenoh.open(self._config)
        return self._internal_session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._internal_session:
            self._internal_session.close()
            self._internal_session = None

    def get_info(self) -> dict[str, Any]:
        if not self._internal_session:
            return {}
        session_info = self._internal_session.info() if callable(self._internal_session.info) else self._internal_session.info
        return {"zid": str(session_info.zid)}


class AsyncZenohSession:
    """
    Asynchronous Zenoh session manager.
    Implements the SessionManager protocol structurally.
    Offloads blocking Zenoh calls to a thread pool for asyncio compatibility.
    """
    def __init__(self, config: Optional[zenoh.Config] = None):
        self._config = config or zenoh.Config()
        self._internal_session: Optional[zenoh.Session] = None

    @property
    def session(self) -> Optional[zenoh.Session]:
        return self._internal_session

    async def __aenter__(self) -> zenoh.Session:
        self._internal_session = await asyncio.to_thread(zenoh.open, self._config)
        return self._internal_session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._internal_session:
            await asyncio.to_thread(self._internal_session.close)
            self._internal_session = None

    def get_info(self) -> dict[str, Any]:
        if not self._internal_session:
            return {}
        session_info = self._internal_session.info() if callable(self._internal_session.info) else self._internal_session.info
        return {"zid": str(session_info.zid)}
