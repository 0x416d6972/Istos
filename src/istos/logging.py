"""Logging for Istos.

Istos is a **library**: importing it must not reconfigure the root logger or
steal an application's log output. Following the standard-library convention, the
``istos`` logger only ever emits records and carries a :class:`~logging.NullHandler`
so nothing is printed and no "No handlers could be found" warning is raised. All
output configuration (handlers, formatters, level, text vs JSON) is left to the
embedding application.

Applications that want Istos to configure output can opt in with
:func:`configure_logging`. ``Istos.run()`` calls :func:`ensure_configured`, which
installs a default handler **only** when neither Istos nor the app has
already configured one — so a standalone service still prints, while a service
embedded in a larger app that manages logging is left untouched.

Usage in framework code::

    from istos.logging import get_logger
    logger = get_logger("handler")           # -> logging.getLogger("istos.handler")
    logger.info("Bound handler %s", prefix, extra={"prefix": prefix})

Messages are human-readable sentences with lazy ``%`` args; structured context
travels in a flat ``extra=`` dict and is surfaced by the JSON formatter.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional

_ROOT_LOGGER_NAME = "istos"

# Library behavior: a NullHandler so records have somewhere to go by default and
# propagate to whatever the application configured on the root logger.
logging.getLogger(_ROOT_LOGGER_NAME).addHandler(logging.NullHandler())

# Attributes always present on a LogRecord; anything else a caller attaches via
# extra={...} is treated as a structured field by the JSON formatter.
_RESERVED_RECORD_KEYS = set(logging.makeLogRecord({}).__dict__) | {
    "message",
    "asctime",
    "taskName",
    "extra_fields",
}


class StructuredFormatter(logging.Formatter):
    """JSON log formatter for production aggregation.

    Lifts any extra fields — whether passed flat via ``extra={...}`` or in the
    legacy ``extra_fields`` envelope — onto the top-level JSON object.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Legacy nested envelope (kept for backward compatibility).
        legacy = getattr(record, "extra_fields", None)
        if isinstance(legacy, dict):
            payload.update(legacy)
        # Flat extras: any non-standard attribute on the record.
        for key, value in record.__dict__.items():
            if key not in _RESERVED_RECORD_KEYS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


_LEVEL_COLORS = {
    "DEBUG": "\033[36m",     # cyan
    "INFO": "\033[32m",      # green
    "WARNING": "\033[33m",   # yellow
    "ERROR": "\033[31m",     # red
    "CRITICAL": "\033[1;31m",  # bold red
}
_RESET = "\033[0m"


class ColorFormatter(logging.Formatter):
    """Human-readable console formatter that colourises the level name (dev)."""

    def format(self, record: logging.LogRecord) -> str:
        color = _LEVEL_COLORS.get(record.levelname, "")
        record.levelname_colored = (
            f"{color}{record.levelname:<8}{_RESET}" if color else f"{record.levelname:<8}"
        )
        return super().format(record)


def configure_logging(
    level: str = "INFO",
    json_format: bool = False,
    *,
    color: Optional[bool] = None,
    logger_name: str = _ROOT_LOGGER_NAME,
) -> logging.Logger:
    """Attach an output handler to the Istos logger (opt-in).

    Libraries embedding Istos should generally NOT call this and instead
    configure their own root logger; Istos records will propagate to it. Call
    this only for a standalone service, or pass ``Istos(configure_logging=True)``.

    ``color`` defaults to auto (on when stderr is a TTY and ``json_format`` is
    False).
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    # Replace previous output handlers but keep the NullHandler.
    logger.handlers = [h for h in logger.handlers if isinstance(h, logging.NullHandler)]

    handler = logging.StreamHandler(sys.stderr)
    if json_format:
        handler.setFormatter(StructuredFormatter())
    else:
        use_color = (sys.stderr.isatty()) if color is None else color
        if use_color:
            handler.setFormatter(
                ColorFormatter(
                    "%(asctime)s %(levelname_colored)s %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
        else:
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
    logger.addHandler(handler)
    # We own this logger's output now; don't double-log through the root.
    logger.propagate = False
    return logger


def _has_output_handler(logger_name: str = _ROOT_LOGGER_NAME) -> bool:
    """True if the Istos logger or an ancestor already has a real (non-Null) handler."""
    node: Optional[logging.Logger] = logging.getLogger(logger_name)
    while node is not None:
        if any(not isinstance(h, logging.NullHandler) for h in node.handlers):
            return True
        node = node.parent if node.propagate else None
    return False


def ensure_configured(level: str = "INFO", json_format: bool = False) -> None:
    """Install a default handler only if nothing else has configured logging.

    Used by ``Istos.run()`` so a standalone service prints logs, without
    clobbering an application that already manages its own logging.
    """
    if not _has_output_handler():
        configure_logging(level=level, json_format=json_format)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger under the ``istos`` namespace.

    Does not configure output — that is the application's responsibility (see
    :func:`configure_logging`).
    """
    logger_name = _ROOT_LOGGER_NAME if name is None else f"{_ROOT_LOGGER_NAME}.{name}"
    return logging.getLogger(logger_name)


def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    **fields: Any,
) -> None:
    """Emit a log record with structured extra fields (flat ``extra=``)."""
    logger.log(level, message, extra=fields)
