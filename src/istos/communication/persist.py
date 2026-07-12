"""Brokerless durable pub/sub persistence — Path B (in-process, no plugin, no router).

Zenoh's own durability (``AdvancedPublisher`` in :mod:`istos.communication.durable`)
keeps a **bounded replay cache in the producer's RAM**: great across subscriber
disconnects, but it dies with the producer and holds only the last N samples.

This module closes that gap *without* a broker or a native Zenoh storage plugin
(which can only run inside a ``zenohd`` router). Instead Istos plays the storage
role itself, in Python:

* a **writer** subscribes to the key expression and persists every sample to an
  object store (S3/MinIO, or in-memory for tests), and
* a **history queryable** answers ``session.get(key)`` by replaying stored
  samples back — so a late-joining or recovering subscriber can fetch history
  even after the original producer has crashed, as long as *some* Istos process
  hosts the role (co-located with the publisher, or a standalone persistence
  node running ``app.persist(key, "s3://…")``).

Object keys are minted per sample (``<key>/<millis>-<seq>``), so the store keeps
the whole *stream* rather than a last-value-wins snapshot — the log semantics a
key-value backend does not give you for free.
"""

from __future__ import annotations

import abc
import asyncio
import logging
import time
from typing import Any, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import zenoh

_logger = logging.getLogger("istos.persist")


# ---------------------------------------------------------------------------
# Object store abstraction
# ---------------------------------------------------------------------------
class ObjectStore(abc.ABC):
    """A minimal append-and-list object store for persisted samples.

    Implementations must be safe to call from an asyncio event loop. Keys are
    opaque strings that sort lexicographically in publish order (the role mints
    them as ``<sample-key>/<millis>-<seq>``).
    """

    @abc.abstractmethod
    async def put(self, key: str, payload: bytes) -> None:
        """Persist ``payload`` under ``key`` (overwrite if the key repeats)."""

    @abc.abstractmethod
    async def history(self, prefix: str) -> List[Tuple[str, bytes]]:
        """Return ``(key, payload)`` for every stored object under ``prefix``,
        ordered oldest-first."""

    async def close(self) -> None:  # pragma: no cover - trivial default
        """Release any resources (network clients). Default: no-op."""
        return None


class InMemoryObjectStore(ObjectStore):
    """In-process store — the zero-dependency default and the test double.

    Durable only for the lifetime of the process, so it does not add
    producer-crash safety; it exists so persistence can be exercised (and unit
    tested) without S3, and as a sane fallback for ``memory://`` URLs.
    """

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    async def put(self, key: str, payload: bytes) -> None:
        self._objects[key] = payload

    async def history(self, prefix: str) -> List[Tuple[str, bytes]]:
        matches = [
            (k, v)
            for k, v in self._objects.items()
            if k == prefix or k.startswith(prefix.rstrip("/") + "/")
        ]
        matches.sort(key=lambda kv: kv[0])
        return matches


class S3ObjectStore(ObjectStore):
    """S3/MinIO-backed store using ``aioboto3`` (the ``istos[s3]`` extra).

    **Credentials** come from the standard AWS chain — environment variables
    (``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` / ``AWS_SESSION_TOKEN``),
    ``~/.aws/credentials``, or an instance/IAM role in production. Secrets are
    never taken from the URL. Pass them explicitly only for local/dev (e.g.
    MinIO) via ``access_key_id`` / ``secret_access_key``.

    **Endpoint / region** (needed for MinIO or a non-default region) may be given
    as constructor kwargs or in the URL query string, e.g.::

        s3://my-bucket/streams?endpoint=http://localhost:9000&region=us-east-1

    Every persisted sample becomes one immutable object, so the bucket
    accumulates the full stream and survives producer restarts — the durability
    the in-RAM replay cache lacks.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
    ) -> None:
        try:
            import aioboto3  # noqa: F401
        except ImportError as e:  # pragma: no cover - exercised via the extra
            raise RuntimeError(
                "S3 persistence requires the 'aioboto3' package. Install it with "
                "`pip install \"istos[s3]\"`."
            ) from e
        import aioboto3

        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        # Explicit creds are optional: when omitted, aioboto3 resolves the
        # standard AWS credential chain (env / shared config / IAM role).
        self._session = aioboto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region_name,
        )

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "S3ObjectStore":
        """Build from ``s3://bucket/prefix?endpoint=…&region=…``.

        Query params ``endpoint``/``endpoint_url`` and ``region``/``region_name``
        configure a custom endpoint (MinIO) and region. Explicit ``kwargs`` win
        over query params. Credentials are intentionally *not* parsed from the URL.
        """
        parsed = urlparse(url)
        bucket = parsed.netloc
        if not bucket:
            raise ValueError(f"S3 store URL is missing a bucket: {url!r}")
        q = parse_qs(parsed.query)

        def pick(*names: str) -> Optional[str]:
            for name in names:
                if name in q and q[name]:
                    return q[name][0]
            return None

        opts: dict[str, Any] = {
            "endpoint_url": pick("endpoint", "endpoint_url"),
            "region_name": pick("region", "region_name"),
        }
        opts.update({k: v for k, v in kwargs.items() if v is not None})
        return cls(bucket, parsed.path.lstrip("/"), **opts)

    def _object_key(self, key: str) -> str:
        return f"{self._prefix}/{key}" if self._prefix else key

    def _client(self) -> Any:
        return self._session.client(
            "s3", endpoint_url=self._endpoint_url, region_name=self._region_name
        )

    async def put(self, key: str, payload: bytes) -> None:
        async with self._client() as s3:
            await s3.put_object(Bucket=self._bucket, Key=self._object_key(key), Body=payload)

    async def history(self, prefix: str) -> List[Tuple[str, bytes]]:
        listing_prefix = self._object_key(prefix.rstrip("/") + "/")
        out: List[Tuple[str, bytes]] = []
        async with self._client() as s3:
            paginator = s3.get_paginator("list_objects_v2")
            keys: List[str] = []
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=listing_prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            keys.sort()
            strip = len(self._object_key("")) if self._prefix else 0
            for full_key in keys:
                resp = await s3.get_object(Bucket=self._bucket, Key=full_key)
                body = await resp["Body"].read()
                out.append((full_key[strip:], body))
        return out


def parse_store_url(url: str) -> ObjectStore:
    """Build an :class:`ObjectStore` from a URL.

    * ``s3://bucket/prefix`` (also MinIO via ``endpoint_url``) → :class:`S3ObjectStore`
    * ``memory://anything`` → :class:`InMemoryObjectStore`
    """
    scheme = urlparse(url).scheme
    if scheme == "s3":
        return S3ObjectStore.from_url(url)
    if scheme in ("memory", ""):
        return InMemoryObjectStore()
    raise ValueError(
        f"Unsupported persistence URL scheme {scheme!r} in {url!r}. "
        "Supported: 's3://…', 'memory://…'."
    )


# ---------------------------------------------------------------------------
# Persistence role: writer subscriber + history queryable
# ---------------------------------------------------------------------------
class PersistRole:
    """Binds a Zenoh key expression to an :class:`ObjectStore`.

    Declares two things on the shared session:

    * a **writer** subscriber that persists every incoming sample, and
    * a **history queryable** that replays persisted samples to any
      ``session.get(key)`` — this is what serves history after a producer dies.

    The store is serializer-agnostic: it persists and replays the raw sample
    payload bytes exactly as they went on the wire.
    """

    def __init__(
        self,
        key_expr: str,
        store: ObjectStore,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.key_expr = key_expr
        self.store = store
        self._logger = logger or _logger
        self._seq = 0
        self._subscriber: Optional[Any] = None
        self._queryable: Optional[Any] = None

    def bind(self, session: "zenoh.Session", loop: asyncio.AbstractEventLoop) -> None:
        def _on_sample(sample: "zenoh.Sample") -> None:
            if loop.is_closed():
                return
            key = str(sample.key_expr)
            payload = bytes(sample.payload)
            asyncio.run_coroutine_threadsafe(self._persist(key, payload), loop)

        def _on_query(query: "zenoh.Query") -> None:
            if loop.is_closed():
                return
            asyncio.run_coroutine_threadsafe(self._answer(query), loop)

        self._subscriber = session.declare_subscriber(self.key_expr, _on_sample)
        self._queryable = session.declare_queryable(self.key_expr, _on_query)
        self._logger.info(
            "Bound persistence role for %s -> %s",
            self.key_expr, type(self.store).__name__, extra={"prefix": self.key_expr},
        )

    async def _persist(self, key: str, payload: bytes) -> None:
        self._seq += 1
        obj_key = f"{key}/{int(time.time() * 1000):013d}-{self._seq:012d}"
        try:
            await self.store.put(obj_key, payload)
        except Exception:  # persistence must never crash the producer/node
            self._logger.exception(
                "Failed to persist sample for %s", key, extra={"prefix": key}
            )

    async def _answer(self, query: "zenoh.Query") -> None:
        selector = str(query.key_expr)
        # Wildcards in the selector become a plain listing prefix.
        listing_prefix = selector.split("*", 1)[0].rstrip("/")
        try:
            history = await self.store.history(listing_prefix)
            # Reply each sample under its own minted key: distinct keys keep
            # Zenoh's reply consolidation from collapsing the stream to a single
            # latest value. Consumers query the wildcard (e.g. "orders/created/**").
            for obj_key, payload in history:
                await asyncio.to_thread(query.reply, obj_key, payload)
        except Exception:
            self._logger.exception(
                "Failed to answer history query for %s", selector,
                extra={"prefix": selector},
            )
        finally:
            # Dropping the Query finalizes the reply stream on the client.
            del query

    def unbind(self) -> None:
        for handle in (self._subscriber, self._queryable):
            if handle is not None:
                try:
                    handle.undeclare()
                except Exception:  # pragma: no cover - best-effort teardown
                    pass
        self._subscriber = None
        self._queryable = None

    async def aclose(self) -> None:
        self.unbind()
        await self.store.close()
