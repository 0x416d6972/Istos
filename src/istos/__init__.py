from .app import Istos
from .routing import IstosRouter
from .core.errors import (
    IstosError,
    IstosSecurityWarning,
    NotFoundError,
    UnauthorizedError,
    RateLimitError,
    ErrorResponse,
    exception_handler,
)
from .core.authz import Authorizer, AuthContext, Principal, TokenAuthorizer, Public
from .di import Depends, DependencyCycleError, current_principal, current_request, current_token
from .logging import configure_logging, get_logger
from .testing import IstosTestClient

__all__ = [
    "Istos",
    "IstosRouter",
    "IstosTestClient",
    "IstosError",
    "IstosSecurityWarning",
    "NotFoundError",
    "UnauthorizedError",
    "RateLimitError",
    "ErrorResponse",
    "exception_handler",
    "Authorizer",
    "AuthContext",
    "Principal",
    "TokenAuthorizer",
    "Public",
    "Depends",
    "DependencyCycleError",
    "current_principal",
    "current_request",
    "current_token",
    "configure_logging",
    "get_logger",
]


def main() -> None:
    from istos.cli import main as cli_main
    cli_main()
