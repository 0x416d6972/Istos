"""Model Context Protocol adapter.

Exposes a node's ``@handle`` endpoints as MCP tools so an LLM client (Claude and
others) can discover and call them over JSON-RPC. ``tools/list`` is built from
the same schemas as capability discovery; ``tools/call`` routes to the handler
through ``query_once``, forwarding the bearer token so the authorizer runs.
"""

import inspect
import json
from typing import Any, Dict, List, Optional, Tuple

from istos.discovery.asyncapi import get_function_schemas
from istos.discovery.naming import tool_name
from istos.context import RequestEnvelope
from istos.errors import IstosError, is_error_payload
from istos.security.authz import AuthContext, check_authorized

MCP_PROTOCOL_VERSION = "2025-06-18"


def _jsonrpc_result(mid: Any, result: dict) -> dict:
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def _jsonrpc_error(mid: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": mid, "error": {"code": code, "message": message}}


class MCPServer:
    """Translates MCP JSON-RPC messages into calls on an Istos app."""

    def __init__(self, app: Any, *, name: Optional[str] = None, version: str = "1.0.0") -> None:
        self._app = app
        self._name = name or app._service_name
        self._version = version

    def validate(self) -> None:
        """Raise if two handlers map to one tool name (``a/b`` and ``a.b`` do).
        Called when the MCP route is mounted, so the clash fails startup instead
        of one tool silently shadowing the other."""
        self._tools()

    def _tools(self) -> Tuple[List[dict], Dict[str, str]]:
        tools: List[dict] = []
        by_name: Dict[str, str] = {}
        for h in self._app._handlers:
            if h.prefix.startswith(".istos/"):
                continue
            name = tool_name(h.prefix)
            if name in by_name:
                raise ValueError(
                    f"MCP tool name {name!r} is claimed by both {by_name[name]!r} and "
                    f"{h.prefix!r}; rename one of the handlers."
                )
            by_name[name] = h.prefix
            try:
                schemas = get_function_schemas(h.func)
            except Exception:
                schemas = {}
            tools.append({
                "name": name,
                "description": (inspect.getdoc(h.func) or "").strip() or name,
                "inputSchema": schemas.get("payload_schema")
                or {"type": "object", "properties": {}},
            })
        return tools, by_name

    async def handle(self, message: dict, *, token: Optional[str] = None) -> Optional[dict]:
        """Dispatch one JSON-RPC message. Returns the response, or None for a
        notification (no ``id``)."""
        method = message.get("method")
        mid = message.get("id")

        if method == "initialize":
            return _jsonrpc_result(mid, {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": self._name, "version": self._version},
            })

        if method == "notifications/initialized" or mid is None:
            return None

        if method == "tools/list":
            tools, by_name = self._tools()
            # List only what this caller may call: the handlers' own authorizers
            # decide, the same gate tools/call goes through.
            allowed = await self._callable_prefixes(set(by_name.values()), token)
            visible = [t for t in tools if by_name[t["name"]] in allowed]
            return _jsonrpc_result(mid, {"tools": visible})

        if method == "tools/call":
            params = message.get("params") or {}
            name = params.get("name")
            arguments = params.get("arguments") or {}
            _, by_name = self._tools()
            prefix = by_name.get(name) if isinstance(name, str) else None
            if prefix is None:
                return _jsonrpc_error(mid, -32602, f"Unknown tool: {name!r}")
            return _jsonrpc_result(mid, await self._call(prefix, arguments, token))

        return _jsonrpc_error(mid, -32601, f"Method not found: {method!r}")

    async def _callable_prefixes(self, prefixes: set, token: Optional[str]) -> set:
        attachment = RequestEnvelope(token=token).to_attachment()
        allowed: set = set()
        for h in self._app._handlers:
            if h.prefix not in prefixes:
                continue
            try:
                await check_authorized(
                    getattr(h, "_authorizer", None),
                    AuthContext(
                        prefix=h.prefix, key_expr=h.prefix, params={},
                        attachment=attachment, operation="handle",
                    ),
                )
            except IstosError:
                continue
            allowed.add(h.prefix)
        return allowed

    async def _call(self, prefix: str, arguments: dict, token: Optional[str]) -> dict:
        try:
            reply = await self._app.query_once(prefix, token=token, **arguments)
        except IstosError as e:
            # Keep the code: an MCP client can act on "not_found", not on prose.
            return {
                "content": [{"type": "text", "text": f"{e.code}: {e.message}"}],
                "isError": True,
            }
        except Exception as e:
            return {"content": [{"type": "text", "text": str(e)}], "isError": True}
        data = reply[0] if isinstance(reply, list) and len(reply) == 1 else reply
        # query_once raises on a single error reply but passes lists through, so
        # an envelope can still arrive here.
        if isinstance(data, list) and any(is_error_payload(item) for item in data):
            return {
                "content": [{"type": "text", "text": json.dumps(data)}],
                "isError": True,
            }
        text = data if isinstance(data, str) else json.dumps(data)
        return {"content": [{"type": "text", "text": text}], "isError": False}
