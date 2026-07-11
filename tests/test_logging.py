"""Logging tests: library semantics, formatters, opt-in configuration."""

import json
import logging

import pytest

from istos.logging import (
    StructuredFormatter,
    configure_logging,
    ensure_configured,
    get_logger,
    _has_output_handler,
)


@pytest.fixture
def clean_istos_logger():
    """Reset the ``istos`` logger to pristine library state around each test."""
    logger = logging.getLogger("istos")
    saved_handlers = list(logger.handlers)
    saved_propagate = logger.propagate
    saved_level = logger.level
    logger.handlers = [logging.NullHandler()]
    logger.propagate = True
    logger.setLevel(logging.NOTSET)
    try:
        yield logger
    finally:
        logger.handlers = saved_handlers
        logger.propagate = saved_propagate
        logger.setLevel(saved_level)


# ---------------------------------------------------------------------------
# Library semantics
# ---------------------------------------------------------------------------

def test_logger_namespacing():
    assert get_logger().name == "istos"
    assert get_logger("handler").name == "istos.handler"


def test_import_installs_nullhandler_only(clean_istos_logger):
    # get_logger must never attach a real (output) handler
    get_logger("something")
    assert clean_istos_logger.handlers
    assert all(isinstance(h, logging.NullHandler) for h in clean_istos_logger.handlers)


def test_get_logger_does_not_configure(clean_istos_logger):
    # Isolate the istos subtree from pytest's root capture handler.
    clean_istos_logger.propagate = False
    get_logger("x").info("no handler should be attached by this")
    assert not _has_output_handler()


# ---------------------------------------------------------------------------
# StructuredFormatter
# ---------------------------------------------------------------------------

def _make_record(level, msg, args=(), **extra):
    return logging.getLogger("istos.test").makeRecord(
        "istos.test", level, __file__, 1, msg, args, None, extra=extra or None
    )


def test_structured_formatter_flat_extra():
    out = json.loads(StructuredFormatter().format(
        _make_record(logging.INFO, "hello %s", ("world",), prefix="robot/move", duration_ms=4.2)
    ))
    assert out["message"] == "hello world"
    assert out["level"] == "INFO"
    assert out["logger"] == "istos.test"
    assert out["prefix"] == "robot/move"
    assert out["duration_ms"] == 4.2


def test_structured_formatter_legacy_envelope():
    out = json.loads(StructuredFormatter().format(
        _make_record(logging.WARNING, "msg", extra_fields={"foo": "bar"})
    ))
    assert out["foo"] == "bar"


def test_structured_formatter_excludes_internal_attrs():
    out = json.loads(StructuredFormatter().format(_make_record(logging.INFO, "m")))
    # standard LogRecord internals must not leak into the JSON
    assert "args" not in out and "levelno" not in out and "pathname" not in out


# ---------------------------------------------------------------------------
# Opt-in configuration
# ---------------------------------------------------------------------------

def test_configure_logging_json_formatter(clean_istos_logger):
    configure_logging(json_format=True)
    real = [h for h in clean_istos_logger.handlers if not isinstance(h, logging.NullHandler)]
    assert len(real) == 1
    assert isinstance(real[0].formatter, StructuredFormatter)
    assert clean_istos_logger.propagate is False


def test_configure_logging_keeps_nullhandler(clean_istos_logger):
    configure_logging(json_format=False, color=False)
    assert any(isinstance(h, logging.NullHandler) for h in clean_istos_logger.handlers)
    assert any(isinstance(h, logging.StreamHandler) for h in clean_istos_logger.handlers)


def test_ensure_configured_installs_when_empty(clean_istos_logger):
    # Isolate the istos subtree so pytest's root handler doesn't count as configured.
    clean_istos_logger.propagate = False
    assert not _has_output_handler()
    ensure_configured()
    assert any(isinstance(h, logging.StreamHandler) for h in clean_istos_logger.handlers)


def test_ensure_configured_noop_when_app_configured(clean_istos_logger):
    root = logging.getLogger()
    handler = logging.StreamHandler()
    root.addHandler(handler)
    try:
        ensure_configured()
        # app already has output on root -> Istos must not add its own
        assert all(isinstance(h, logging.NullHandler) for h in clean_istos_logger.handlers)
    finally:
        root.removeHandler(handler)


# ---------------------------------------------------------------------------
# Istos integration
# ---------------------------------------------------------------------------

def test_istos_does_not_configure_by_default(clean_istos_logger):
    from istos import Istos
    Istos()
    assert all(isinstance(h, logging.NullHandler) for h in clean_istos_logger.handlers)


def test_istos_configure_logging_true(clean_istos_logger):
    from istos import Istos
    Istos(configure_logging=True)
    # configure happened on the istos logger itself, not merely inherited from root
    assert any(isinstance(h, logging.StreamHandler) for h in clean_istos_logger.handlers)
    assert _has_output_handler()
