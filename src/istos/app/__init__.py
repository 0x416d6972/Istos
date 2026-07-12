"""The Istos application object, composed from domain mixins.

The class is assembled here; its behaviour lives in the mixins (messaging,
streaming, queues, web, lifecycle) over the shared IstosBase state."""

from istos.app._base import IstosBase
from istos.app.messaging import _MessagingMixin
from istos.app.streaming import _StreamingMixin
from istos.app.queues import _QueueMixin
from istos.app.web import _WebMixin
from istos.app.lifecycle import _LifecycleMixin


class Istos(
    _MessagingMixin,
    _StreamingMixin,
    _QueueMixin,
    _WebMixin,
    _LifecycleMixin,
):
    """
    Unified entry-point for the Istos framework.

    Usage:
        istos = Istos()

        # Or wire the network from a config; the session is built for you:
        istos = Istos(config=IstosZenohConfig(mode="client"))

        @istos.handle(prefix="robot/move")
        async def move(distance: int):
            return f"moved {distance}m"

        class Drone:
            @istos.handle(prefix="drone/fly")
            def fly(self, altitude: int):
                return f"flying at {altitude}m"

        istos.run()          # sync entry
        await istos.run_async()  # async entry
    """


__all__ = ["Istos", "IstosBase"]
