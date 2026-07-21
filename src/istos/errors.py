"""Standard error types and exception handler registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Type, cast

from istos.validation import SchemaValidationError


class IstosSecurityWarning(UserWarning):
    """Insecure config (no TLS, no authorizer, …).

    Emitting a warning keeps local demos easy. In CI escalate it::

        import warnings
        from istos import IstosSecurityWarning
        warnings.simplefilter("error", IstosSecurityWarning)
    """


class IstosSecurityError(Exception):
    """Missing a required security setting — e.g. ``require_auth=True`` with no authorizer."""


class IstosError(Exception):
    """Base exception for Istos application errors.

    ``correlation_id`` is set when the error came off a responder's reply (see
    :func:`error_from_payload`) and matches the log line on the node that failed.
    It is ``None`` for an error raised locally.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = "internal_error",
        status: int = 500,
        details: Optional[Any] = None,
        correlation_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status = status
        self.details = details
        self.correlation_id = correlation_id


class NotFoundError(IstosError):
    def __init__(self, message: str = "Resource not found", **kwargs: Any):
        super().__init__(message, code="not_found", status=404, **kwargs)


class UnauthorizedError(IstosError):
    def __init__(self, message: str = "Unauthorized", **kwargs: Any):
        super().__init__(message, code="unauthorized", status=401, **kwargs)


class ForbiddenError(IstosError):
    """Authenticated, but not permitted (e.g. missing a required role)."""

    def __init__(self, message: str = "Forbidden", **kwargs: Any):
        super().__init__(message, code="forbidden", status=403, **kwargs)


class RateLimitError(IstosError):
    def __init__(self, message: str = "Rate limit exceeded", **kwargs: Any):
        super().__init__(message, code="rate_limit_exceeded", status=429, **kwargs)


# Wire ``code`` → the class carrying it. Codes absent here rebuild as a plain
# IstosError keeping their code.
_CODE_TO_ERROR: Dict[str, Type[IstosError]] = {
    "not_found": NotFoundError,
    "unauthorized": UnauthorizedError,
    "forbidden": ForbiddenError,
    "rate_limit_exceeded": RateLimitError,
}

# Wire ``code`` → status, for rebuilding an error off a reply: the subclasses
# carry a status but the wire does not, and codes without a subclass
# (validation_error) still need one. The HTTP gateway maps from here too.
CODE_TO_STATUS: Dict[str, int] = {
    "unauthorized": 401,
    "forbidden": 403,
    "not_found": 404,
    "validation_error": 400,
    "bad_request": 400,
    "rate_limit_exceeded": 429,
}
DEFAULT_ERROR_STATUS = 500


def is_retryable(exc: BaseException) -> bool:
    """Whether retrying ``exc`` could plausibly succeed.

    A 4xx-class error is the caller's own fault and comes back the same however
    often it is asked, so retrying only spends the backoff budget. A 429 is the
    exception: waiting is the remedy. Everything else, transport faults included,
    is retryable.
    """
    if isinstance(exc, IstosError):
        if exc.status == 429:
            return True
        return not (400 <= exc.status < 500)
    return True


ERROR_MARKER = "__istos_error"


@dataclass
class ErrorResponse:
    """Standard wire-format error payload for all Istos endpoints."""

    error: str
    code: str
    message: str
    correlation_id: Optional[str] = None
    details: Optional[Any] = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            ERROR_MARKER: True,
            "error": self.error,
            "code": self.code,
            "message": self.message,
        }
        if self.correlation_id:
            payload["correlation_id"] = self.correlation_id
        if self.details is not None:
            payload["details"] = self.details
        return payload


def is_error_payload(parsed: Any) -> bool:
    """Whether a decoded reply is an :class:`ErrorResponse` wire payload.

    A handler that raises replies with an envelope rather than sending an
    exception, and the envelope answers ``.get()`` like any other dict, so an
    unchecked caller reads a failure as data.

    ``query_once``, ``@query``, ``stream_query`` and ``open_channel`` check this
    themselves. Use it directly for replies you decode yourself, and for
    multi-reply results, which are passed through unchecked. Pair it with
    :func:`error_from_payload` to raise.

    Detection prefers the :data:`ERROR_MARKER` discriminator: present and truthy
    is an error, present and falsy is a normal result — the escape hatch for a
    handler whose success value legitimately carries ``error``/``code``/
    ``message``. When the marker is absent (an old responder, or a client in
    another language), fall back to the legacy rule: all three of ``error``,
    ``code`` and ``message`` present.
    """
    if not isinstance(parsed, dict):
        return False
    marker = parsed.get(ERROR_MARKER)
    if marker is not None:
        return bool(marker)
    return all(field in parsed for field in ("error", "code", "message"))


def error_from_payload(
    parsed: Dict[str, Any], *, default_code: str = "internal_error"
) -> IstosError:
    """Rebuild an exception from an error payload, to re-raise on the caller's side.

    The code selects the class the responder raised, so ``except NotFoundError``
    works across a hop.
    """
    code = parsed.get("code", default_code)
    message = parsed.get("message", "the responder failed")
    details = parsed.get("details")
    correlation_id = parsed.get("correlation_id")

    cls = _CODE_TO_ERROR.get(code)
    if cls is not None:
        return cls(message, details=details, correlation_id=correlation_id)
    return IstosError(
        message,
        code=code,
        # Recovered from the code, not defaulted: the status decides retryability.
        status=CODE_TO_STATUS.get(code, DEFAULT_ERROR_STATUS),
        details=details,
        correlation_id=correlation_id,
    )


def reply_err(
    message: str,
    *,
    code: str = "internal_error",
    correlation_id: Optional[str] = None,
    details: Optional[Any] = None,
) -> dict[str, Any]:
    """Build an error-envelope dict a handler can ``return`` instead of raising.

    Raising is still the usual path — the framework turns any :class:`IstosError`
    into this same envelope. ``reply_err`` is for the handler that wants to reply
    an error inline without an exception; it stamps :data:`ERROR_MARKER`, so the
    result cannot be missing a field the caller's check depends on.
    """
    return ErrorResponse(
        error=code, code=code, message=message,
        correlation_id=correlation_id, details=details,
    ).to_dict()


ExceptionHandler = Callable[[Exception], ErrorResponse]


class ExceptionHandlerRegistry:
    """Maps exception types to handler callables."""

    def __init__(self) -> None:
        self._handlers: Dict[Type[Exception], ExceptionHandler] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        self.register(
            SchemaValidationError,
            lambda e: ErrorResponse(
                error="validation_error",
                code="validation_error",
                message=str(e),
                details=getattr(e, "errors", None),
            ),
        )
        def _from_istos_error(e: Exception) -> ErrorResponse:
            err = cast(IstosError, e)
            return ErrorResponse(
                error=err.code,
                code=err.code,
                message=err.message,
                details=err.details,
            )

        self.register(IstosError, _from_istos_error)

    def register(
        self,
        exc_type: Type[Exception],
        handler: ExceptionHandler,
    ) -> None:
        self._handlers[exc_type] = handler

    def resolve(self, exc: Exception) -> ErrorResponse:
        for exc_type, handler in self._handlers.items():
            if isinstance(exc, exc_type):
                response = handler(exc)
                return response
        return ErrorResponse(
            error="internal_error",
            code="internal_error",
            message=str(exc),
        )


_default_registry = ExceptionHandlerRegistry()


def get_default_registry() -> ExceptionHandlerRegistry:
    return _default_registry


def exception_handler(
    exc_type: Type[Exception],
    registry: Optional[ExceptionHandlerRegistry] = None,
) -> Callable[[ExceptionHandler], ExceptionHandler]:
    """Decorator to register a custom exception handler."""

    def decorator(handler: ExceptionHandler) -> ExceptionHandler:
        target = registry or _default_registry
        target.register(exc_type, handler)
        return handler

    return decorator
