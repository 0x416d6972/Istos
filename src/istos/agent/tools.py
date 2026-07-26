"""Mesh tools — ``@handle`` endpoints as callable tools for an agent loop.

Same catalogue shape MCP uses (name, docstring, parameter schema), but the
caller is another Istos node: each tool is a ``query_once`` on a key expression.
"""

from __future__ import annotations

import inspect
import json
from typing import Any, Awaitable, Callable, List, Optional, Sequence, Union

from istos.discovery.asyncapi import get_function_schemas
from istos.discovery.naming import tool_name
from istos.errors import IstosError

__all__ = [
    "MeshTool",
    "format_tool_error",
    "format_tool_result",
    "tool_name",
    "tools_from_discovery",
    "tools_from_handlers",
    "tools_from_manifest",
]


class MeshTool:
    """One mesh endpoint the agent may call.

    Built from a local ``@handle`` via :func:`tools_from_handlers`, or by hand
    when the tool lives on another node (pass ``app`` and the remote prefix)::

        MeshTool("math/add", app=app, description="Add two integers",
                 parameters={"type": "object", "properties": {
                     "a": {"type": "integer"}, "b": {"type": "integer"},
                 }, "required": ["a", "b"]})

    ``approval=True`` (or a string reason) makes the loop stop for a human before
    the call — see :mod:`istos.agent.approval`. The tool's owner can declare it
    instead, with ``@handle(approval=True)``, in which case discovery carries it
    here on its own.
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
        approval: Union[bool, str] = False,
    ) -> None:
        if app is None and invoke is None:
            raise ValueError("MeshTool needs an app (for query_once) or an invoke callable")
        self.prefix = prefix
        self.name = name or tool_name(prefix)
        self.description = description or self.name
        self.parameters = parameters or {"type": "object", "properties": {}}
        self.approval = approval
        self._app = app
        self._invoke = invoke

    @property
    def requires_approval(self) -> bool:
        return bool(self.approval)

    @property
    def approval_reason(self) -> Optional[str]:
        """The reason an approver is shown, when the flag carried one."""
        return self.approval if isinstance(self.approval, str) else None

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
    approval: Optional[Sequence[str]] = None,
) -> List[MeshTool]:
    """Build :class:`MeshTool` entries from the app's ``@handle`` registry.

    Plumbing under ``.istos/`` is skipped. Pass ``prefixes`` to whitelist
    (exact key expressions). Schemas come from the same path MCP uses.

    A handler registered with ``@handle(approval=…)`` produces a tool that needs
    a human; ``approval=["some/prefix"]`` marks extra prefixes from the caller's
    side, for endpoints that did not declare it themselves.
    """
    allow = set(prefixes) if prefixes is not None else None
    needs_human = set(approval or ())
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
                approval=getattr(h, "approval", False) or h.prefix in needs_human,
            )
        )
    return out


def tools_from_manifest(
    app: Any,
    manifest: dict,
    *,
    prefixes: Optional[Sequence[str]] = None,
    approval: Optional[Sequence[str]] = None,
) -> List[MeshTool]:
    """Build :class:`MeshTool` entries from one capability manifest.

    ``manifest`` is what :meth:`Istos.export_capabilities` returns (and what
    :meth:`Istos.discover_capabilities` collects per service). Only ``handle``
    entries become tools: a mesh tool is a ``query_once``, so streams, channels,
    and pub/sub entries are not callable this way and are skipped.

    ``app`` is the *local* node — it issues the query; the handler itself lives
    wherever the manifest came from. An entry the owner marked
    ``@handle(approval=…)`` arrives with its flag intact; ``approval=[…]`` adds
    prefixes this caller wants gated regardless.
    """
    allow = set(prefixes) if prefixes is not None else None
    needs_human = set(approval or ())
    out: List[MeshTool] = []
    for entry in manifest.get("capabilities") or []:
        if not isinstance(entry, dict) or entry.get("kind") != "handle":
            continue
        prefix = entry.get("prefix")
        if not isinstance(prefix, str) or prefix.startswith(".istos/"):
            continue
        if allow is not None and prefix not in allow:
            continue
        out.append(
            MeshTool(
                prefix,
                app=app,
                description=(entry.get("description") or "").strip() or tool_name(prefix),
                parameters=entry.get("params_schema")
                or {"type": "object", "properties": {}},
                approval=entry.get("approval") or prefix in needs_human,
            )
        )
    return out


async def tools_from_discovery(
    app: Any,
    *,
    services: Optional[Sequence[str]] = None,
    prefixes: Optional[Sequence[str]] = None,
    approval: Optional[Sequence[str]] = None,
    timeout_s: float = 3.0,
) -> List[MeshTool]:
    """Inventory the fabric and build tools for every remote ``@handle`` found.

    The counterpart to :func:`tools_from_handlers`: instead of the local
    registry, this asks ``.istos/capabilities/*`` (via
    :meth:`Istos.discover_capabilities`), so a remote handler's schema and
    docstring come from the node that owns it rather than being written out by
    hand::

        tools = await tools_from_discovery(app, services=["billing", "search"])
        async for event in run_agent(model, tools, messages):
            ...

    Pass ``services`` to whitelist by service name, ``prefixes`` to whitelist by
    key expression, ``approval`` to gate extra prefixes on a human (endpoints
    declaring ``@handle(approval=…)`` arrive gated already). Nodes with discovery
    disabled (``Istos(enable_discovery=False)``) do not answer and contribute
    nothing.

    The catalogue is a snapshot: call again to pick up nodes that joined later.
    Duplicate prefixes across services collapse to the first one seen — the same
    key expression is the same endpoint either way.
    """
    manifests = await app.discover_capabilities(timeout_s=timeout_s)
    wanted = set(services) if services is not None else None
    out: List[MeshTool] = []
    seen: set = set()
    for service, manifest in sorted(manifests.items()):
        if wanted is not None and service not in wanted:
            continue
        for tool in tools_from_manifest(app, manifest, prefixes=prefixes, approval=approval):
            if tool.prefix in seen:
                continue
            seen.add(tool.prefix)
            out.append(tool)
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
