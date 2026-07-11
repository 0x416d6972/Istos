"""Brokerless durable pub/sub over Zenoh's advanced (ext) publishers/subscribers.

Durability without a broker: the *producer* retains a bounded cache of what it
published (its own replay log) and heartbeats a sequence number; a *subscriber*
replays history on join and recovers missed samples by querying the producer
peer-to-peer. No central log, no broker to run.

See https://zenoh.io — ``zenoh.ext.AdvancedPublisher`` / ``AdvancedSubscriber``.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

import zenoh

try:
    import zenoh.ext as _zext
except ImportError:  # pragma: no cover - zenoh always ships ext, but stay defensive
    _zext = None  # type: ignore


def _require_ext() -> Any:
    if _zext is None:
        raise RuntimeError(
            "Durable pub/sub requires zenoh.ext (Zenoh's advanced publisher/"
            "subscriber), which is unavailable in this Zenoh build."
        )
    return _zext


def declare_durable_publisher(
    session: Any,
    key_expr: str,
    *,
    cache: int = 1000,
    heartbeat: float = 1.0,
    reliability: Optional["zenoh.Reliability"] = None,
    congestion_control: Optional["zenoh.CongestionControl"] = None,
) -> Any:
    """
    An ``AdvancedPublisher`` that retains the last ``cache`` samples (its replay
    log) and heartbeats its latest sequence number every ``heartbeat`` seconds so
    subscribers can detect and recover gaps. This is the brokerless durable log.

    Durable defaults harden the transport so ``durable`` actually means it:

    - ``reliability=RELIABLE`` — request reliable delivery on the link.
    - ``congestion_control=BLOCK`` — under backpressure the producer *blocks*
      instead of Zenoh's default ``DROP``, so samples are not silently discarded
      before they reach the replay cache and the wire.

    Pass ``reliability`` / ``congestion_control`` explicitly to override.
    """
    zext = _require_ext()
    if reliability is None:
        reliability = zenoh.Reliability.RELIABLE
    if congestion_control is None:
        congestion_control = zenoh.CongestionControl.BLOCK
    return zext.declare_advanced_publisher(
        session,
        key_expr,
        cache=zext.CacheConfig(max_samples=cache),
        sample_miss_detection=zext.MissDetectionConfig(heartbeat=heartbeat),
        publisher_detection=True,
        reliability=reliability,
        congestion_control=congestion_control,
    )


class DurableSubscription:
    """
    Handle bundling a durable ``AdvancedSubscriber`` with its optional
    sample-miss listener so both share one lifetime.

    Keeping the miss listener referenced here is what keeps it alive; dropping it
    would stop miss notifications. ``undeclare()`` tears down both.
    """

    def __init__(self, subscriber: Any, miss_listener: Any = None) -> None:
        self._subscriber = subscriber
        self._miss_listener = miss_listener

    def undeclare(self) -> None:
        if self._miss_listener is not None:
            try:
                self._miss_listener.undeclare()
            except Exception:
                pass
            self._miss_listener = None
        self._subscriber.undeclare()


def declare_durable_subscriber(
    session: Any,
    key_expr: str,
    callback: Callable[[Any], None],
    *,
    replay: int = 1000,
    recover: bool = True,
    on_miss: Optional[Callable[[str, int], None]] = None,
) -> DurableSubscription:
    """
    An ``AdvancedSubscriber`` that, on join, replays up to ``replay`` historical
    samples from the producer's cache (late-join history) and — when ``recover``
    is set — re-fetches samples it missed during transient disconnects.

    When ``on_miss`` is given, a ``SampleMissListener`` is wired so that gaps which
    could *not* be recovered are surfaced as ``on_miss(source, nb)`` — the honest
    failure signal of at-least-once delivery. ``source`` identifies the producer
    and ``nb`` is the number of samples irrecoverably missed.
    """
    zext = _require_ext()
    history = zext.HistoryConfig(detect_late_publishers=True, max_samples=replay)
    recovery = zext.RecoveryConfig(heartbeat=True) if recover else None
    subscriber = zext.declare_advanced_subscriber(
        session,
        key_expr,
        callback,
        history=history,
        recovery=recovery,
    )

    miss_listener = None
    if on_miss is not None:
        def _on_miss(miss: Any) -> None:
            source = str(getattr(miss, "source", ""))
            nb = int(getattr(miss, "nb", 0))
            on_miss(source, nb)

        miss_listener = subscriber.sample_miss_listener(_on_miss)

    return DurableSubscription(subscriber, miss_listener)
