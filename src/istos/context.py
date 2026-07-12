"""Request-scoped context for correlation IDs and metadata."""

from __future__ import annotations

import json
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class RequestEnvelope:
    """Auth token (+ optional correlation/trace) carried in the Zenoh attachment.

    A bare UTF-8 string is still just a token — old clients keep working. When
    you also need ``correlation_id`` or ``traceparent``, we send compact JSON::

        {"tok": "...", "cid": "...", "tp": "..."}
    """

    token: Optional[str] = None
    correlation_id: Optional[str] = None
    traceparent: Optional[str] = None

    def to_attachment(self) -> Optional[bytes]:
        # Token alone stays a bare string; otherwise compact JSON.
        if self.correlation_id is None and self.traceparent is None:
            return self.token.encode("utf-8") if self.token is not None else None
        obj: dict[str, str] = {}
        if self.token is not None:
            obj["tok"] = self.token
        if self.correlation_id is not None:
            obj["cid"] = self.correlation_id
        if self.traceparent is not None:
            obj["tp"] = self.traceparent
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_attachment(cls, raw: Optional[bytes]) -> "RequestEnvelope":
        if raw is None:
            return cls()
        try:
            text = bytes(raw).decode("utf-8")
        except (UnicodeDecodeError, ValueError, TypeError):
            return cls()
        stripped = text.strip()
        # Only a JSON object carrying at least one known key is an envelope;
        # anything else (a JWT, a shared secret, opaque text) is a bare token.
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                obj = json.loads(stripped)
            except (ValueError, TypeError):
                obj = None
            if isinstance(obj, dict) and any(k in obj for k in ("tok", "cid", "tp")):
                return cls(
                    token=obj.get("tok"),
                    correlation_id=obj.get("cid"),
                    traceparent=obj.get("tp"),
                )
        return cls(token=text)


@dataclass
class RequestContext:
    """Per-request context propagated through middleware and handlers."""

    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    prefix: str = ""
    operation: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    #: Identity resolved by the authorizer for this request (``None`` if the
    #: request was allowed without an identity, or came from an in-process call).
    principal: Any = None
    #: Raw request attachment as sent by the caller (envelope or bare token bytes).
    attachment: Optional[bytes] = None
    #: W3C ``traceparent`` for this request, propagated across hops for tracing.
    traceparent: Optional[str] = None

    @property
    def token(self) -> Optional[str]:
        """The auth token from the request attachment (envelope-aware)."""
        return RequestEnvelope.from_attachment(self.attachment).token


_request_context: ContextVar[Optional[RequestContext]] = ContextVar(
    "istos_request_context", default=None
)


def get_request_context() -> RequestContext:
    """Return the current request context, creating one if absent."""
    ctx = _request_context.get()
    if ctx is None:
        ctx = RequestContext()
        _request_context.set(ctx)
    return ctx


def peek_request_context() -> Optional[RequestContext]:
    """Return the active request context without creating one.

    Used by outbound calls to propagate metadata (correlation_id, traceparent)
    *only* when they originate inside a request — a root call carries nothing.
    """
    return _request_context.get()


def set_request_context(ctx: RequestContext) -> None:
    """Set the active request context."""
    _request_context.set(ctx)


def reset_request_context() -> None:
    """Clear the active request context."""
    _request_context.set(None)
