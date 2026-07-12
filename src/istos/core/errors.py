"""Standard error types and exception handler registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Type

from istos.core.validation import SchemaValidationError


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
    """Base exception for Istos application errors."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "internal_error",
        status: int = 500,
        details: Optional[Any] = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status = status
        self.details = details


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
            "error": self.error,
            "code": self.code,
            "message": self.message,
        }
        if self.correlation_id:
            payload["correlation_id"] = self.correlation_id
        if self.details is not None:
            payload["details"] = self.details
        return payload


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
        self.register(
            IstosError,
            lambda e: ErrorResponse(
                error=e.code,
                code=e.code,
                message=e.message,
                details=e.details,
            ),
        )

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
