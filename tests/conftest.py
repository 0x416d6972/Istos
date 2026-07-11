"""Shared test fixtures."""

import pytest
from istos import Istos


@pytest.fixture
def istos():
    """Istos instance without built-in health/metrics handlers for isolated tests."""
    return Istos(enable_health=False, enable_metrics=False)
