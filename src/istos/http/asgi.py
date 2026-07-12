"""Run an Istos mesh inside an ASGI app (FastAPI / Starlette).

The ASGI server owns the process and the HTTP port; Istos rides its lifespan —
the Zenoh session opens on startup and closes on shutdown. Routes then reach the
whole mesh in-process through the app (query_once / stream_query / publish_once),
so there is one process and no sidecar.

    from fastapi import FastAPI
    from istos import Istos
    from istos.http.asgi import lifespan

    mesh = Istos(config=IstosZenohConfig(mode="client", ...))
    api = FastAPI(lifespan=lifespan(mesh))

    @api.get("/move")
    async def move(distance: int):
        return await mesh.query_once("robot/move", distance=distance)

If you already have your own lifespan, drop the primitive in directly instead:

    @asynccontextmanager
    async def my_lifespan(api):
        async with mesh.serving():
            ...          # your own startup
            yield
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable

if TYPE_CHECKING:
    from istos.app import Istos


def lifespan(mesh: "Istos") -> Callable[[Any], Any]:
    """An ASGI lifespan that starts and stops ``mesh``. Pass it straight to
    ``FastAPI(lifespan=...)`` or ``Starlette(lifespan=...)``."""

    @contextlib.asynccontextmanager
    async def _lifespan(_app: Any) -> AsyncIterator[None]:
        async with mesh.serving():
            yield

    return _lifespan
