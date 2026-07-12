"""Model Context Protocol adapter.

Exposes a node's ``@handle`` endpoints as MCP tools so an LLM client (Claude and
others) can discover and call them over JSON-RPC. ``tools/list`` is built from
the same schemas as capability discovery; ``tools/call`` routes to the handler
through ``query_once``, forwarding the bearer token so the authorizer runs.
"""

import inspect
import json
from typing import Any, Dict, List, Optional, Tuple

from istos.core.asyncapi import get_function_schemas

MCP_PROTOCOL_VERSION = "2025-06-18"


def _tool_name(prefix: str) -> str:
    # MCP tool names allow [A-Za-z0-9_-]; key expressions use '/'.
    return prefix.replace("/", "-")


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

    def _tools(self) -> Tuple[List[dict], Dict[str, str]]:
        tools: List[dict] = []
        by_name: Dict[str, str] = {}
        for h in self._app._handlers:
            if h.prefix.startswith(".istos/"):
                continue
            name = _tool_name(h.prefix)
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
            tools, _ = self._tools()
            return _jsonrpc_result(mid, {"tools": tools})

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

    async def _call(self, prefix: str, arguments: dict, token: Optional[str]) -> dict:
        try:
            reply = await self._app.query_once(prefix, attachment=token, **arguments)
        except Exception as e:
            return {"content": [{"type": "text", "text": str(e)}], "isError": True}
        data = reply[0] if isinstance(reply, list) and len(reply) == 1 else reply
        is_error = isinstance(data, dict) and "error" in data and "code" in data
        text = data if isinstance(data, str) else json.dumps(data)
        return {"content": [{"type": "text", "text": text}], "isError": is_error}
