"""Structured connection settings for a SQL database (durability ledger or app DB)."""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    from sqlalchemy import URL
except ImportError:  # pragma: no cover - exercised only when the extra is absent
    URL = None  # type: ignore


class DatabaseConfig(BaseSettings):
    """
    Structured connection settings for a SQL database — used both for the
    durability ledger (``storage_database``) and for application databases
    injected via ``Depends`` (``databases``). Fields can be passed directly or
    supplied via environment variables (prefix ``ISTOS_DB_``), the usual path
    for secrets in Kubernetes:

        DatabaseConfig(backend="postgresql", driver="asyncpg", host="db",
                       database="istos", username="svc", password="s3cret")
        # or, from the environment:
        #   ISTOS_DB_BACKEND=postgresql ISTOS_DB_DRIVER=asyncpg
        #   ISTOS_DB_HOST=db ISTOS_DB_DATABASE=istos
        #   ISTOS_DB_USERNAME=svc ISTOS_DB_PASSWORD=s3cret

    `driver` is required and never defaulted: Istos does not bundle a database
    driver, so you name the async driver you installed yourself (e.g. `asyncpg`
    or `psycopg` for PostgreSQL, `asyncmy` for MySQL, `aiosqlite` for SQLite).
    A missing driver fails fast at construction with an actionable message.
    """

    model_config = SettingsConfigDict(env_prefix="ISTOS_DB_", extra="forbid")

    backend: Literal["postgresql", "mysql", "mariadb", "sqlite", "mssql", "oracle"]
    driver: str  # required: you name the async driver you installed (no default)
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None  # for sqlite this is the file path (or ':memory:')
    query: Dict[str, str] = Field(default_factory=dict)  # extra URL params, e.g. sslmode
    echo: bool = False
    pool_size: Optional[int] = None
    max_overflow: Optional[int] = None

    @model_validator(mode="after")
    def _validate(self) -> "DatabaseConfig":
        if self.backend == "sqlite":
            if not self.database:
                raise ValueError(
                    "sqlite backend requires `database` (a file path or ':memory:')."
                )
        else:
            missing = [f for f in ("host", "database") if not getattr(self, f)]
            if missing:
                raise ValueError(
                    f"{self.backend} backend requires: {', '.join(missing)}"
                )
        return self

    def build_url(self) -> "URL":
        """Assemble a SQLAlchemy URL (handles password/host escaping correctly)."""
        if URL is None:
            raise ImportError(
                "SQLAlchemy is not installed. Install with: pip install 'istos[sqlalchemy]'"
            )
        return URL.create(
            drivername=f"{self.backend}+{self.driver}",
            username=self.username,
            password=self.password.get_secret_value() if self.password else None,
            host=self.host,
            port=self.port,
            database=self.database,
            query=self.query,
        )

    def engine_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {"echo": self.echo}
        # Pool sizing is not applicable to SQLite's default pool.
        if self.backend != "sqlite":
            if self.pool_size is not None:
                kwargs["pool_size"] = self.pool_size
            if self.max_overflow is not None:
                kwargs["max_overflow"] = self.max_overflow
        return kwargs


# Backwards-compatible alias: the ledger-specific name this config started life as.
StorageConfig = DatabaseConfig
