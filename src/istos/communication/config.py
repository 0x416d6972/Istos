"""Declarative Zenoh session configuration (networking mode, TLS/mTLS, auth)."""

import json
import re
import warnings
from typing import Annotated, Any, Literal, Optional, cast

import zenoh
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

# A Zenoh endpoint looks like '<proto>/<host>:<port>', e.g. 'tls/router:7447'.
_ENDPOINT_RE = re.compile(r"^[a-z0-9]+/.+")


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
        # Reject unknown ISTOS_ZENOH_* variables so a typo like
        # ISTOS_ZENOH_USERNAM does not silently disable authentication.
        extra="forbid",
    )

    mode: Literal["peer", "client", "router"] = Field(
        default="peer", description="'peer', 'client', or 'router'"
    )
    # Selects which session manager Istos(config=...) wires up for the whole
    # service: 'async' (AsyncZenohSession, the asyncio-friendly default) or
    # 'sync' (ZenohSession).
    session: Literal["async", "sync"] = Field(
        default="async", description="Session manager flavor: 'async' or 'sync'"
    )
    # NoDecode disables pydantic-settings' automatic JSON decoding of env values
    # so _parse_endpoint_list can accept either a JSON array or a comma-separated
    # string (a plain list is passed through unchanged when set in code).
    connect_endpoints: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="JSON array or comma-separated string via env; list in code",
    )
    listen_endpoints: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="JSON array or comma-separated string via env; list in code",
    )

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

    # Escape hatch: raw zenoh config fragments deep-merged over the generated
    # config, for knobs this builder does not model (congestion control,
    # batching, access-control interceptors, etc.).
    additional_config: dict[str, Any] = Field(default_factory=dict)

    # ------------------------------------------------------------------
    # Validation — runs at construction, the idiomatic pydantic way, so an
    # invalid config raises ValidationError immediately instead of at build().
    # ------------------------------------------------------------------

    @field_validator("connect_endpoints", "listen_endpoints", mode="before")
    @classmethod
    def _parse_endpoint_list(cls, value: Any) -> Any:
        """Accept a JSON array, a comma-separated string, or a list."""
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                return json.loads(stripped)
            return [item.strip() for item in stripped.split(",") if item.strip()]
        return value

    @field_validator("connect_endpoints", "listen_endpoints")
    @classmethod
    def _check_endpoint_format(cls, value: list[str]) -> list[str]:
        for endpoint in value:
            if not _ENDPOINT_RE.match(endpoint):
                raise ValueError(
                    f"Invalid Zenoh endpoint {endpoint!r}: expected "
                    "'<proto>/<host>:<port>', e.g. 'tls/router.local:7447'."
                )
        return value

    @model_validator(mode="after")
    def _check_security(self) -> "IstosZenohConfig":
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

        # Developer-facing security warnings, emitted once at construction.
        from istos.errors import IstosSecurityWarning

        has_auth = bool(self.username and self.password is not None)
        has_tls = any([
            self.root_ca_certificate,
            self.listen_certificate,
            self.listen_private_key,
            self.enable_mtls,
        ])
        if not has_auth and not has_tls:
            warnings.warn(
                f"IstosZenohConfig(mode={self.mode!r}) has neither authentication "
                "(username/password) nor TLS configured; traffic is unauthenticated "
                "and unencrypted. Set ISTOS_ZENOH_USERNAME/PASSWORD and/or TLS "
                "certificates for production.",
                IstosSecurityWarning,
                stacklevel=2,
            )
        elif has_auth and not has_tls:
            warnings.warn(
                "Username/password auth is configured without TLS; credentials "
                "and traffic cross the network unencrypted. Add TLS "
                "(root_ca_certificate + listen_certificate/key) before production.",
                IstosSecurityWarning,
                stacklevel=2,
            )
        return self

    @staticmethod
    def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
        for key, value in overlay.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                IstosZenohConfig._deep_merge(base[key], value)
            else:
                base[key] = value
        return base

    def build(self) -> zenoh.Config:
        """Render these validated settings into a raw ``zenoh.Config``.

        Pure transform: the config has already been validated at construction.
        """
        conf_dict: dict[str, Any] = {"mode": self.mode}

        if self.connect_endpoints:
            conf_dict["connect"] = {"endpoints": self.connect_endpoints}

        if self.listen_endpoints:
            conf_dict["listen"] = {"endpoints": self.listen_endpoints}

        if not self.multicast_scouting:
            conf_dict["scouting"] = {"multicast": {"enabled": False}}

        transport_conf: dict[str, Any] = {}

        if self.username and self.password is not None:
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

        if self.additional_config:
            self._deep_merge(conf_dict, self.additional_config)

        # from_json5 is untyped (returns Any); annotate the known return type.
        return cast(zenoh.Config, zenoh.Config.from_json5(json.dumps(conf_dict)))
