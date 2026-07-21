import zenoh
import asyncio
import time
import warnings
from typing import Optional, Any, Protocol, cast, runtime_checkable

from istos.logging import get_logger

_logger = get_logger("session")

_INSECURE_DEFAULT_MSG = (
    "Opening a Zenoh session in unauthenticated peer mode with multicast "
    "scouting and no TLS. Any peer on the local network can discover this node, "
    "invoke its handlers, and read its published data. For production configure "
    "IstosZenohConfig with a username/password and/or TLS, and prefer "
    "mode='client' against a trusted router. See the Security section of the README."
)


def _warn_insecure_default() -> None:
    """Warn about opening a session with the insecure default config.

    ``warnings`` deduplicates by default, so this effectively fires once per
    process without a hand-rolled flag; users can filter or escalate it.
    """
    from istos.errors import IstosSecurityWarning

    warnings.warn(_INSECURE_DEFAULT_MSG, IstosSecurityWarning, stacklevel=3)


def open_default_session() -> "zenoh.Session":
    """Open a Zenoh session with the default (insecure) config, warning once.

    Used by the decorator single-run fallbacks. Prefer supplying an explicit,
    authenticated ``IstosZenohConfig`` for anything long-lived.
    """
    _warn_insecure_default()
    return zenoh.open(zenoh.Config())


def _log_open_retry(attempt: int, attempts: int, exc: Exception) -> None:
    _logger.warning(
        "Retrying Zenoh session open (attempt %d/%d): %s",
        attempt + 1, attempts, exc,
        extra={"attempt": attempt + 1, "of": attempts, "error": str(exc)},
    )


def _open_failed(attempts: int, last_exc: Optional[Exception]) -> RuntimeError:
    return RuntimeError(
        f"Failed to open Zenoh session after {attempts} attempt(s). Check the "
        f"endpoints, TLS material, and that the router is reachable. Last error: {last_exc}"
    )


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
    def __init__(
        self,
        config: Optional[zenoh.Config] = None,
        session: Optional[zenoh.Session] = None,
        *,
        open_retries: int = 0,
        open_retry_delay_s: float = 1.0,
    ):
        self._explicit_config = config is not None
        self._config = config or zenoh.Config()
        self._internal_session = session
        # Only close sessions this manager opened; never close an injected one.
        self._owns_session = session is None
        self._open_retries = open_retries
        self._open_retry_delay_s = open_retry_delay_s

    @property
    def session(self) -> Optional[zenoh.Session]:
        return self._internal_session

    @property
    def is_active(self) -> bool:
        """True while a session is open on this manager."""
        return self._internal_session is not None

    def _open(self) -> zenoh.Session:
        """Open the session, warning on insecure defaults and retrying transient
        failures (e.g. a router that is not up yet in ``client`` mode)."""
        if not self._explicit_config:
            _warn_insecure_default()
        attempts = max(1, self._open_retries + 1)
        last_exc: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                return zenoh.open(self._config)
            except Exception as exc:  # noqa: BLE001 - re-raised with context below
                last_exc = exc
                if attempt < attempts - 1:
                    _log_open_retry(attempt, attempts, exc)
                    time.sleep(self._open_retry_delay_s)
        raise _open_failed(attempts, last_exc) from last_exc

    def __enter__(self) -> zenoh.Session:
        if self._internal_session is None:
            self._internal_session = self._open()
            self._owns_session = True
        return self._internal_session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._internal_session and self._owns_session:
            self._internal_session.close()
        self._internal_session = None

    def get_info(self) -> dict[str, Any]:
        if not self._internal_session:
            return {}
        try:
            info = self._internal_session.info
            session_info = info() if callable(info) else info
            return {"zid": str(session_info.zid)}
        except Exception:
            return {}

    def put(self, key_expr: str, payload: Any, **kwargs):
        if not self._internal_session:
            raise RuntimeError("No active Zenoh session")
        return self._internal_session.put(key_expr, payload, **kwargs)

    def get(self, key_expr: str, **kwargs):
        if not self._internal_session:
            raise RuntimeError("No active Zenoh session")
        return self._internal_session.get(key_expr, **kwargs)

    def delete(self, key_expr: str, **kwargs):
        if not self._internal_session:
            raise RuntimeError("No active Zenoh session")
        return self._internal_session.delete(key_expr, **kwargs)


class AsyncZenohSession:
    """
    Asynchronous Zenoh session manager.
    Implements the SessionManager protocol structurally.
    Offloads blocking Zenoh calls to a thread pool for asyncio compatibility.
    """
    def __init__(
        self,
        config: Optional[zenoh.Config] = None,
        session: Optional[zenoh.Session] = None,
        *,
        open_retries: int = 0,
        open_retry_delay_s: float = 1.0,
    ):
        self._explicit_config = config is not None
        self._config = config or zenoh.Config()
        self._internal_session = session
        # Only close sessions this manager opened; never close an injected one.
        self._owns_session = session is None
        self._open_retries = open_retries
        self._open_retry_delay_s = open_retry_delay_s

    @property
    def session(self) -> Optional[zenoh.Session]:
        return self._internal_session

    @property
    def is_active(self) -> bool:
        """True while a session is open on this manager."""
        return self._internal_session is not None

    async def _open(self) -> zenoh.Session:
        """Async variant of :meth:`ZenohSession._open`; yields the loop between
        tries instead of blocking it."""
        if not self._explicit_config:
            _warn_insecure_default()
        attempts = max(1, self._open_retries + 1)
        last_exc: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                return await asyncio.to_thread(zenoh.open, self._config)
            except Exception as exc:  # noqa: BLE001 - re-raised with context below
                last_exc = exc
                if attempt < attempts - 1:
                    _log_open_retry(attempt, attempts, exc)
                    await asyncio.sleep(self._open_retry_delay_s)
        raise _open_failed(attempts, last_exc) from last_exc

    async def __aenter__(self) -> zenoh.Session:
        if self._internal_session is None:
            self._internal_session = await self._open()
            self._owns_session = True
        return self._internal_session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._internal_session and self._owns_session:
            await asyncio.to_thread(self._internal_session.close)
        self._internal_session = None

    def get_info(self) -> dict[str, Any]:
        if not self._internal_session:
            return {}
        try:
            info = self._internal_session.info
            session_info = info() if callable(info) else info
            return {"zid": str(session_info.zid)}
        except Exception:
            return {}

    async def put(self, key_expr: str, payload: Any, **kwargs):
        if not self._internal_session:
            raise RuntimeError("No active Zenoh session")
        return await asyncio.to_thread(self._internal_session.put, key_expr, payload, **kwargs)

    async def get(self, key_expr: str, **kwargs):
        if not self._internal_session:
            raise RuntimeError("No active Zenoh session")
        return await asyncio.to_thread(self._internal_session.get, key_expr, **kwargs)  # type: ignore[arg-type]

    async def delete(self, key_expr: str, **kwargs):
        if not self._internal_session:
            raise RuntimeError("No active Zenoh session")
        return await asyncio.to_thread(self._internal_session.delete, key_expr, **kwargs)
