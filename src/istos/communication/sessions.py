import zenoh
import asyncio
import re
import time
import warnings
from typing import Optional, Any, Protocol, runtime_checkable
import json
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from istos.logging import get_logger

_logger = get_logger("session")

_ENDPOINT_RE = re.compile(r"^[a-z0-9]+/.+")

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
    from istos.core.errors import IstosSecurityWarning

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


class IstosZenohConfig(BaseSettings):
    """
    A unified builder for configuring the Zenoh session, including networking 
    modes, TLS/mTLS encryption, and authentication.
    
    Reads from .env automatically using the prefix 'ISTOS_ZENOH_'.
    Example variables: ISTOS_ZENOH_MODE, ISTOS_ZENOH_USERNAME, ISTOS_ZENOH_ROOT_CA_CERTIFICATE.
    
    For enterprise use cases (Vault, AWS Secrets Manager), you can bypass .env 
    and pass raw strings directly when initializing this class.
    """
    model_config = SettingsConfigDict(
        env_prefix="ISTOS_ZENOH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
    )

    mode: str = Field(default="peer", description="'peer', 'client', or 'router'")
    connect_endpoints: list[str] = Field(default_factory=list, description="Comma-separated via env or list in code")
    listen_endpoints: list[str] = Field(default_factory=list, description="Comma-separated via env or list in code")

    # Multicast scouting auto-discovers peers on the LAN. It is convenient for
    # local development but a discovery/attack surface in production; disable it
    # and use explicit connect_endpoints for locked-down deployments.
    multicast_scouting: bool = Field(default=True, description="Enable UDP multicast peer discovery")

    username: Optional[str] = None
    password: Optional[SecretStr] = None

    root_ca_certificate: Optional[str] = Field(default=None, description="Path to CA file OR raw PEM string")
    listen_certificate: Optional[str] = Field(default=None, description="Path to cert file OR raw PEM string")
    listen_private_key: Optional[SecretStr] = Field(default=None, description="Path to key file OR raw PEM string")
    enable_mtls: bool = False

    additional_config: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
        for key, value in overlay.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                IstosZenohConfig._deep_merge(base[key], value)
            else:
                base[key] = value
        return base

    def _validate(self, has_auth: bool, tls_conf: dict[str, Any]) -> None:
        """Fail fast on structurally broken or misleading security config."""
        for endpoint in [*self.connect_endpoints, *self.listen_endpoints]:
            if not _ENDPOINT_RE.match(endpoint):
                raise ValueError(
                    f"Invalid Zenoh endpoint {endpoint!r}: expected "
                    "'<proto>/<host>:<port>', e.g. 'tls/router.local:7447'."
                )

        if bool(self.listen_certificate) != bool(self.listen_private_key):
            raise ValueError(
                "listen_certificate and listen_private_key must be provided "
                "together (a server-side TLS cert needs its private key)."
            )

        if self.enable_mtls and not self.root_ca_certificate:
            raise ValueError(
                "enable_mtls=True requires root_ca_certificate to verify peer "
                "certificates."
            )

    def build(self) -> zenoh.Config:
        """Constructs a raw zenoh.Config object from these typed settings."""
        conf_dict: dict[str, Any] = {"mode": self.mode}

        if self.connect_endpoints:
            conf_dict["connect"] = {"endpoints": self.connect_endpoints}

        if self.listen_endpoints:
            conf_dict["listen"] = {"endpoints": self.listen_endpoints}

        if not self.multicast_scouting:
            conf_dict["scouting"] = {"multicast": {"enabled": False}}

        transport_conf: dict[str, Any] = {}

        has_auth = bool(self.username and self.password is not None)
        if has_auth:
            transport_conf["auth"] = {
                "usrpwd": {
                    "user": self.username,
                    "password": self.password.get_secret_value(),
                }
            }

        tls_conf: dict[str, Any] = {}
        if self.root_ca_certificate:
            tls_conf["root_ca_certificate"] = self.root_ca_certificate
        if self.listen_certificate:
            tls_conf["listen_certificate"] = self.listen_certificate
        if self.listen_private_key:
            tls_conf["listen_private_key"] = self.listen_private_key.get_secret_value()
        if self.enable_mtls:
            tls_conf["enable_mtls"] = self.enable_mtls

        if tls_conf:
            transport_conf["link"] = {"tls": tls_conf}

        if transport_conf:
            conf_dict["transport"] = transport_conf

        self._validate(has_auth, tls_conf)

        from istos.core.errors import IstosSecurityWarning

        if not has_auth and not tls_conf:
            warnings.warn(
                f"IstosZenohConfig(mode={self.mode!r}) has neither authentication "
                "(username/password) nor TLS configured; traffic is unauthenticated "
                "and unencrypted. Set ISTOS_ZENOH_USERNAME/PASSWORD and/or TLS "
                "certificates for production.",
                IstosSecurityWarning,
                stacklevel=2,
            )
        elif has_auth and not tls_conf:
            warnings.warn(
                "Username/password auth is configured without TLS; credentials "
                "and traffic cross the network unencrypted. Add TLS "
                "(root_ca_certificate + listen_certificate/key) before production.",
                IstosSecurityWarning,
                stacklevel=2,
            )

        if self.additional_config:
            self._deep_merge(conf_dict, self.additional_config)

        json_str = json.dumps(conf_dict)
        return zenoh.Config.from_json5(json_str)


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
        return await asyncio.to_thread(self._internal_session.get, key_expr, **kwargs)

    async def delete(self, key_expr: str, **kwargs):
        if not self._internal_session:
            raise RuntimeError("No active Zenoh session")
        return await asyncio.to_thread(self._internal_session.delete, key_expr, **kwargs)
