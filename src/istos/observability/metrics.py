"""Prometheus-compatible metrics (optional dependency)."""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from istos.middleware.base import HandlerCallable, RequestScope


class MetricsCollector:
    """In-process metrics collector with optional Prometheus export."""

    def __init__(self) -> None:
        self._counters: Dict[str, int] = {}
        self._histograms: Dict[str, list[float]] = {}

    def increment(self, name: str, labels: Optional[Dict[str, str]] = None, value: int = 1) -> None:
        key = self._key(name, labels)
        self._counters[key] = self._counters.get(key, 0) + value

    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        key = self._key(name, labels)
        if key not in self._histograms:
            self._histograms[key] = []
        self._histograms[key].append(value)

    def _key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus text format."""
        lines: list[str] = []
        for key, value in sorted(self._counters.items()):
            lines.append(f"istos_{key} {value}")
        for key, values in sorted(self._histograms.items()):
            if values:
                lines.append(f"istos_{key}_count {len(values)}")
                lines.append(f"istos_{key}_sum {sum(values)}")
        return "\n".join(lines) + "\n"

    def snapshot(self) -> dict[str, Any]:
        return {"counters": dict(self._counters), "histograms": dict(self._histograms)}


class PrometheusMiddleware:
    """Records request counts and latency histograms."""

    def __init__(self, collector: MetricsCollector) -> None:
        self._collector = collector

    async def __call__(
        self,
        scope: RequestScope,
        call_next: HandlerCallable,
    ) -> Any:
        labels = {"operation": scope.operation, "prefix": scope.prefix}
        start = time.perf_counter()
        self._collector.increment("requests_total", labels)
        try:
            result = await call_next(scope)
            self._collector.increment("requests_success_total", labels)
            return result
        except Exception:
            self._collector.increment("requests_error_total", labels)
            raise
        finally:
            duration = time.perf_counter() - start
            self._collector.observe("request_duration_seconds", duration, labels)
