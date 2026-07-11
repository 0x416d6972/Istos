"""Request-scoped context for correlation IDs and metadata."""

from __future__ import annotations

import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Optional


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
    #: Raw request attachment as sent by the caller (auth token bytes, etc.).
    attachment: Optional[bytes] = None

    @property
    def token(self) -> Optional[str]:
        """Decode the request attachment as a UTF-8 token, if present."""
        if self.attachment is None:
            return None
        try:
            return bytes(self.attachment).decode("utf-8")
        except (UnicodeDecodeError, ValueError, TypeError):
            return None


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


def set_request_context(ctx: RequestContext) -> None:
    """Set the active request context."""
    _request_context.set(ctx)


def reset_request_context() -> None:
    """Clear the active request context."""
    _request_context.set(None)
