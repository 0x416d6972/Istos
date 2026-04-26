import zenoh
import asyncio
from typing import Optional, Any, Protocol, runtime_checkable
import json
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

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
        extra="ignore"
    )

    mode: str = Field(default="peer", description="'peer', 'client', or 'router'")
    connect_endpoints: list[str] = Field(default_factory=list, description="Comma-separated via env or list in code")
    listen_endpoints: list[str] = Field(default_factory=list, description="Comma-separated via env or list in code")
    
    # Basic Authorization
    username: Optional[str] = None
    password: Optional[str] = None

    # TLS Configuration (Paths or Raw PEM Strings)
    root_ca_certificate: Optional[str] = Field(default=None, description="Path to CA file OR raw PEM string")
    listen_certificate: Optional[str] = Field(default=None, description="Path to cert file OR raw PEM string")
    listen_private_key: Optional[str] = Field(default=None, description="Path to key file OR raw PEM string")
    enable_mtls: bool = False

    def build(self) -> zenoh.Config:
        """Constructs a raw zenoh.Config object from these typed settings."""
        conf_dict: dict[str, Any] = {"mode": self.mode}

        if self.connect_endpoints:
            conf_dict["connect"] = {"endpoints": self.connect_endpoints}
        
        if self.listen_endpoints:
            conf_dict["listen"] = {"endpoints": self.listen_endpoints}

        transport_conf: dict[str, Any] = {}
        
        if self.username and self.password:
            transport_conf["auth"] = {
                "usrpwd": {
                    "user": self.username,
                    "password": self.password
                }
            }

        tls_conf: dict[str, Any] = {}
        if self.root_ca_certificate:
            tls_conf["root_ca_certificate"] = self.root_ca_certificate
        if self.listen_certificate:
            tls_conf["listen_certificate"] = self.listen_certificate
        if self.listen_private_key:
            tls_conf["listen_private_key"] = self.listen_private_key
        if self.enable_mtls:
            tls_conf["enable_mtls"] = self.enable_mtls

        if tls_conf:
            transport_conf["link"] = {"tls": tls_conf}

        if transport_conf:
            conf_dict["transport"] = transport_conf

        json_str = json.dumps(conf_dict)
        return zenoh.Config.from_json5(json_str)


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
