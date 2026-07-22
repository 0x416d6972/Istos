"""Mesh tools — ``@handle`` endpoints as callable tools for an agent loop.

Same catalogue shape MCP uses (name, docstring, parameter schema), but the
caller is another Istos node: each tool is a ``query_once`` on a key expression.
"""

from __future__ import annotations

import inspect
import json
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Union

from istos.discovery.asyncapi import get_function_schemas
from istos.errors import IstosError


def tool_name(prefix: str) -> str:
    # OpenAI / MCP tool names allow [A-Za-z0-9_-]; key expressions use '/'.
    return prefix.replace("/", "-")


class MeshTool:
    """One mesh endpoint the agent may call.

    Built from a local ``@handle`` via :func:`tools_from_handlers`, or by hand
    when the tool lives on another node (pass ``app`` and the remote prefix)::

        MeshTool("math/add", app=app, description="Add two integers",
                 parameters={"type": "object", "properties": {
                     "a": {"type": "integer"}, "b": {"type": "integer"},
                 }, "required": ["a", "b"]})
    """

    def __init__(
        self,
        prefix: str,
        *,
        app: Any = None,
        name: Optional[str] = None,
        description: str = "",
        parameters: Optional[dict] = None,
        invoke: Optional[Callable[..., Awaitable[Any]]] = None,
    ) -> None:
        if app is None and invoke is None:
            raise ValueError("MeshTool needs an app (for query_once) or an invoke callable")
        self.prefix = prefix
        self.name = name or tool_name(prefix)
        self.description = description or self.name
        self.parameters = parameters or {"type": "object", "properties": {}}
        self._app = app
        self._invoke = invoke

    def openai_schema(self) -> dict:
        """Tool definition in the OpenAI chat-completions shape."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }

    async def call(
        self,
        arguments: dict,
        *,
        token: Optional[Union[bytes, str]] = None,
        timeout_s: float = 5.0,
    ) -> Any:
        """Run the tool. Mesh tools go through ``query_once`` so authorizers run."""
        if self._invoke is not None:
            return await self._invoke(**arguments)
        assert self._app is not None
        return await self._app.query_once(
            self.prefix, token=token, timeout_s=timeout_s, **arguments
        )


def tools_from_handlers(
    app: Any,
    *,
    prefixes: Optional[Sequence[str]] = None,
) -> List[MeshTool]:
    """Build :class:`MeshTool` entries from the app's ``@handle`` registry.

    Plumbing under ``.istos/`` is skipped. Pass ``prefixes`` to whitelist
    (exact key expressions). Schemas come from the same path MCP uses.
    """
    allow = set(prefixes) if prefixes is not None else None
    out: List[MeshTool] = []
    for h in app._handlers:
        if h.prefix.startswith(".istos/"):
            continue
        if allow is not None and h.prefix not in allow:
            continue
        try:
            schemas = get_function_schemas(h.func)
        except Exception:
            schemas = {}
        params = schemas.get("payload_schema") or {"type": "object", "properties": {}}
        out.append(
            MeshTool(
                h.prefix,
                app=app,
                description=(inspect.getdoc(h.func) or "").strip() or tool_name(h.prefix),
                parameters=params,
            )
        )
    return out


def format_tool_result(value: Any) -> str:
    """Serialize a tool return value for the model (string content)."""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except TypeError:
        return str(value)


def format_tool_error(exc: BaseException) -> str:
    if isinstance(exc, IstosError):
        return f"{exc.code}: {exc.message}"
    return str(exc)
