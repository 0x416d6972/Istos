"""Health and readiness check support."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Awaitable

from istos.logging import get_logger

_logger = get_logger("health")

HealthCheck = Callable[[], Awaitable[Dict[str, Any]]]


@dataclass
class HealthState:
    """Tracks application health status."""

    started_at: float = field(default_factory=time.time)
    ready: bool = False
    checks: Dict[str, HealthCheck] = field(default_factory=dict)

    async def liveness(self) -> Dict[str, Any]:
        return {
            "status": "alive",
            "uptime_seconds": round(time.time() - self.started_at, 2),
        }

    async def readiness(self) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        all_ok = self.ready
        for name, check in self.checks.items():
            try:
                results[name] = await check()
                if results[name].get("status") != "ok":
                    all_ok = False
            except Exception as exc:
                # Readiness is answered to any peer; the exception text can
                # carry DSNs or hostnames, so it goes to the log, not the reply.
                _logger.warning("Readiness check %s failed", name, exc_info=True)
                results[name] = {"status": "error", "error": type(exc).__name__}
                all_ok = False
        return {
            "status": "ready" if all_ok else "not_ready",
            "checks": results,
        }


def register_health_handlers(app: Any, state: HealthState) -> None:
    """Register built-in health/readiness/metrics Zenoh handlers."""

    @app.handle(".istos/health")
    async def _health() -> Dict[str, Any]:
        return await state.liveness()

    @app.handle(".istos/ready")
    async def _ready() -> Dict[str, Any]:
        return await state.readiness()
