"""Request-scoped context for correlation IDs and metadata."""

from __future__ import annotations

import json
import re
import uuid
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, Optional

_CORRELATION_ID_MAX = 200
_TRACEPARENT_RE = re.compile(r"^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$")


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


def push_request_context(ctx: RequestContext) -> Token:
    """Install ``ctx`` and return the token that restores whatever was active."""
    return _request_context.set(ctx)


def pop_request_context(token: Token) -> None:
    """Restore the context that was active before :func:`push_request_context`."""
    _request_context.reset(token)


def sanitize_correlation_id(value: Optional[str]) -> Optional[str]:
    """A caller-supplied correlation id, or None when it is missing or unsafe.

    Unsafe means empty, too long, or containing a control character, quote, or
    backslash — those are the characters that break a log line or a label.
    """
    if not isinstance(value, str) or not value or len(value) > _CORRELATION_ID_MAX:
        return None
    if any(ord(ch) < 32 or ch in '"\\' for ch in value):
        return None
    return value


def sanitize_traceparent(value: Optional[str]) -> Optional[str]:
    """A caller-supplied W3C ``traceparent``, or None when it is not one.

    The all-zero trace id and span id are invalid per the spec and are rejected
    so a peer cannot pin every span to a single forged trace.
    """
    if not isinstance(value, str):
        return None
    candidate = value.strip().lower()
    if _TRACEPARENT_RE.fullmatch(candidate) is None or candidate.startswith("ff"):
        return None
    _version, trace_id, span_id, _flags = candidate.split("-")
    if trace_id == "0" * 32 or span_id == "0" * 16:
        return None
    return candidate


def ingress_context(
    *,
    prefix: str,
    operation: str,
    attachment: Optional[bytes],
    principal: Any = None,
) -> RequestContext:
    """A fresh context for one delivery.

    The correlation id and traceparent come from the envelope only when they
    pass :func:`sanitize_correlation_id` / :func:`sanitize_traceparent`. Otherwise
    the id is a new UUID and the traceparent is absent — a reused task must not
    keep the previous delivery's values.
    """
    env = RequestEnvelope.from_attachment(attachment)
    correlation_id = sanitize_correlation_id(env.correlation_id) or str(uuid.uuid4())
    return RequestContext(
        correlation_id=correlation_id,
        prefix=prefix,
        operation=operation,
        principal=principal,
        attachment=attachment,
        traceparent=sanitize_traceparent(env.traceparent),
    )
