"""The HTTP edge: embedded aiohttp server, gateway/SSE/WebSocket/MCP handlers, docs and capability discovery."""

import asyncio
import contextlib
import inspect
import uuid
import warnings
from typing import Any, Callable, List, Optional

from istos.primitives.channel import ChannelSession, channel_wrapper
from istos.discovery.asyncapi import AsyncApiGenerator, get_asyncapi_ui_html
from istos.errors import (
    IstosError,
    IstosSecurityWarning,
    UnauthorizedError,
)
from istos.security.authz import Authorizer
from istos.context import RequestContext, RequestEnvelope, set_request_context
from istos.http.gateway import HttpRoute, build_selector, extract_bearer, status_for_reply, sse_event, decode_params
from istos.http.health import register_health_handlers

from istos.app._base import IstosBase


class _WebMixin(IstosBase):
    """The HTTP edge: embedded aiohttp server, gateway/SSE/WebSocket/MCP handlers, docs and capability discovery."""

    def export_asyncapi(self, title: str = "Istos Network", version: str = "1.0.0") -> str:
        """
        Generates and returns the AsyncAPI YAML specification for the network.
        """
        generator = AsyncApiGenerator(title=title, version=version)
        return generator.generate(self)

    def serve_docs(
        self,
        prefix: str = ".istos/docs",
        title: str = "Istos Network",
        version: str = "1.0.0",
        web_port: Optional[int] = None,
        authorizer: Optional[Authorizer] = None,
    ) -> None:
        """
        Registers a built-in handler to serve the AsyncAPI specification over Zenoh.
        If web_port is provided, it starts an embedded HTTP server to display the UI.

        The docs endpoint publishes your entire API surface. Protect it with an
        ``authorizer`` — which layers on top of the app-wide one — or rely on the
        app-wide authorizer alone. If neither is set a security warning is emitted
        because any peer can then enumerate your API.
        """
        # Used only to decide whether to warn about an ungated docs endpoint.
        effective = authorizer if authorizer is not None else self._authorizer
        if effective is None:
            warnings.warn(
                f"Docs endpoint {prefix!r} has no authorizer: it broadcasts your "
                "full AsyncAPI surface to every peer. Pass authorizer=... or set "
                "Istos(authorizer=...).",
                IstosSecurityWarning,
                stacklevel=2,
            )

        @self.handle(prefix=prefix, authorizer=authorizer)
        def _serve_docs() -> str:
            return self.export_asyncapi(title=title, version=version)

        if web_port is not None:
            self._docs_web_port = web_port
            self._docs_prefix = prefix


    def _http_server_port(self) -> Optional[int]:
        """The port for the embedded HTTP surface: explicit ``http_port`` wins,
        else the docs ``web_port`` (backward compatible)."""
        return self._http_port or self._docs_web_port

    async def _start_http_server(self) -> Any:
        """Start the embedded aiohttp server hosting the HTTP surface:
        K8s probes, Prometheus ``/metrics``, the ingress gateway routes, and
        (when configured) the docs UI. All share one port."""
        from aiohttp import web

        app = web.Application()

        async def _livez(request: web.Request) -> web.Response:
            return web.json_response(await self._health.liveness())

        async def _readyz(request: web.Request) -> web.Response:
            result = await self._health.readiness()
            status = 200 if result.get("status") == "ready" else 503
            return web.json_response(result, status=status)

        app.router.add_get('/livez', _livez)
        app.router.add_get('/healthz', _livez)   # common alias
        app.router.add_get('/readyz', _readyz)

        async def _metrics(request: web.Request) -> web.Response:
            return web.Response(
                text=self._metrics.export_prometheus(),
                content_type='text/plain', charset='utf-8',
            )

        app.router.add_get('/metrics', _metrics)

        for route in self._http_routes:
            handler = (
                self._make_sse_handler(route) if route.sse
                else self._make_gateway_handler(route)
            )
            app.router.add_route(route.method, route.path, handler)

        # WebSocket routes for @channel handlers.
        for path, wrapper in self._ws_channel_routes:
            app.router.add_get(path, self._make_ws_channel_handler(wrapper))

        # MCP endpoint: @handle tools over JSON-RPC.
        if self._enable_mcp:
            app.router.add_post(self._mcp_path, self._make_mcp_handler())

        if self._docs_prefix is not None:
            html = get_asyncapi_ui_html(title="Istos Network Docs", schema_url="/asyncapi.yaml")

            async def web_ui_handler(request: web.Request) -> web.Response:
                return web.Response(text=html, content_type='text/html')

            async def asyncapi_yaml_handler(request: web.Request) -> web.Response:
                try:
                    results = await self.query_once(self._docs_prefix or ".istos/docs", timeout_s=2.0)
                    if results:
                        yaml_content = results[0] if isinstance(results, list) else results
                        return web.Response(text=yaml_content, content_type='application/yaml')
                    return web.Response(text="Docs not found on network", status=404)
                except Exception as e:
                    return web.Response(text=f"Error querying network: {e}", status=500)

            app.router.add_get('/', web_ui_handler)
            app.router.add_get('/asyncapi.yaml', asyncapi_yaml_handler)

        port = self._http_server_port()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        self._logger.info(
            "HTTP surface on http://localhost:%s (probes /livez /readyz, /metrics, "
            "%d gateway route(s))", port, len(self._http_routes),
            extra={"port": port, "gateway_routes": len(self._http_routes)},
        )
        return runner

    def _make_gateway_handler(self, route: HttpRoute) -> Any:
        """Build an aiohttp handler that bridges an HTTP request to a Zenoh query
        against ``route.key_expr``, forwarding the Authorization header as the
        query attachment (so the authorizer gate runs)."""
        import json as _json

        from aiohttp import web

        async def _handler(request: web.Request) -> web.Response:
            params: dict = dict(request.query)
            if request.body_exists:
                text = await request.text()
                if text.strip():
                    try:
                        data = _json.loads(text)
                    except _json.JSONDecodeError:
                        return web.json_response(
                            {"error": "bad_request", "code": "bad_request",
                             "message": "Request body must be valid JSON."},
                            status=400,
                        )
                    if not isinstance(data, dict):
                        return web.json_response(
                            {"error": "bad_request", "code": "bad_request",
                             "message": "JSON body must be an object of params."},
                            status=400,
                        )
                    params.update(data)

            token = extract_bearer(request.headers.get("Authorization"))
            selector = build_selector(route.key_expr, params)
            # Keep one cid / traceparent from HTTP into the Zenoh hop.
            envelope = RequestEnvelope(
                token=token,
                correlation_id=(request.headers.get("X-Correlation-ID")
                                or request.headers.get("X-Request-ID")),
                traceparent=request.headers.get("traceparent"),
            )
            outbound_attachment = envelope.to_attachment()

            def _query() -> Optional[bytes]:
                session = self._session_manager.session
                if session is None:
                    return None
                kwargs: dict = {"timeout": route.timeout_s}
                if outbound_attachment is not None:
                    kwargs["attachment"] = outbound_attachment
                for reply in session.get(selector, **kwargs):
                    try:
                        return bytes(reply.ok.payload)
                    except Exception:
                        continue  # skip error replies from other queryables
                return None

            try:
                payload = await asyncio.to_thread(_query)
            except Exception as e:
                self._logger.error(
                    "Gateway query failed for %s: %s", route.key_expr, e,
                    exc_info=True, extra={"prefix": route.key_expr},
                )
                return web.json_response(
                    {"error": "gateway_error", "code": "gateway_error",
                     "message": "Upstream query failed."},
                    status=502,
                )

            if payload is None:
                return web.json_response(
                    {"error": "not_found", "code": "not_found",
                     "message": f"No handler replied for {route.key_expr!r}."},
                    status=504,
                )

            try:
                parsed = _json.loads(payload)
            except Exception:
                return web.Response(body=payload, content_type='application/octet-stream')
            return web.json_response(parsed, status=status_for_reply(parsed))

        return _handler

    def _make_sse_handler(self, route: HttpRoute) -> Any:
        """aiohttp handler that relays a ``@stream`` handler's chunks as SSE.
        Forwards the Authorization and trace headers into the Zenoh envelope."""
        import json as _json

        from aiohttp import web

        async def _handler(request: web.Request) -> web.StreamResponse:
            params: dict = dict(request.query)
            if request.body_exists:
                text = await request.text()
                if text.strip():
                    try:
                        data = _json.loads(text)
                    except _json.JSONDecodeError:
                        return web.json_response(
                            {"error": "bad_request", "code": "bad_request",
                             "message": "Request body must be valid JSON."},
                            status=400,
                        )
                    if isinstance(data, dict):
                        params.update(data)

            token = extract_bearer(request.headers.get("Authorization"))
            # stream_query reads cid/trace from the ambient context.
            set_request_context(RequestContext(
                correlation_id=(request.headers.get("X-Correlation-ID")
                                or request.headers.get("X-Request-ID")
                                or str(uuid.uuid4())),
                traceparent=request.headers.get("traceparent"),
                prefix=route.key_expr,
                operation="stream",
            ))

            response = web.StreamResponse(
                status=200,
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",  # disable proxy buffering (nginx)
                },
            )
            await response.prepare(request)

            try:
                async for chunk in self.stream_query(
                    route.key_expr, timeout_s=route.timeout_s, token=token, **params
                ):
                    text_chunk = chunk if isinstance(chunk, str) else _json.dumps(chunk)
                    await response.write(sse_event(text_chunk).encode("utf-8"))
                await response.write(sse_event("", event="end").encode("utf-8"))
            except IstosError as e:
                err = _json.dumps({"code": e.code, "message": e.message})
                await response.write(sse_event(err, event="error").encode("utf-8"))
            except asyncio.CancelledError:
                raise  # client disconnected
            except Exception as e:
                self._logger.error(
                    "SSE stream failed for %s: %s", route.key_expr, e,
                    exc_info=True, extra={"prefix": route.key_expr},
                )
                err = _json.dumps({"code": "stream_error", "message": "Upstream stream failed."})
                try:
                    await response.write(sse_event(err, event="error").encode("utf-8"))
                except Exception:
                    pass
            finally:
                with contextlib.suppress(Exception):
                    await response.write_eof()
            return response

        return _handler

    def _make_ws_channel_handler(self, wrapper: channel_wrapper) -> Any:
        """aiohttp handler that runs a @channel over a WebSocket. The socket is
        the duplex pipe: inbound frames feed the session, session.send() writes
        back. Auth + trace headers come off the handshake."""
        import json as _json

        from aiohttp import WSMsgType, web

        async def _handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse(heartbeat=30.0)
            await ws.prepare(request)

            token = extract_bearer(request.headers.get("Authorization"))
            attachment = RequestEnvelope(
                token=token,
                correlation_id=(request.headers.get("X-Correlation-ID")
                                or request.headers.get("X-Request-ID")),
                traceparent=request.headers.get("traceparent"),
            ).to_attachment()
            params = decode_params(dict(request.query))
            conversation_id = params.pop("conversation_id", None)
            if wrapper.durable and conversation_id is None:
                conversation_id = uuid.uuid4().hex

            async def sink(raw: bytes) -> None:
                # Prefer text frames (browser-friendly JSON); fall back to binary.
                try:
                    await ws.send_str(raw.decode("utf-8"))
                except UnicodeDecodeError:
                    await ws.send_bytes(raw)

            session = ChannelSession(
                wrapper.serializer, sink, attachment=attachment,
                store=wrapper.session_store, conversation_id=conversation_id,
            )

            async def pump_inbound() -> None:
                async for msg in ws:
                    if msg.type == WSMsgType.TEXT:
                        session.feed(msg.data.encode("utf-8"))
                    elif msg.type == WSMsgType.BINARY:
                        session.feed(msg.data)
                    elif msg.type == WSMsgType.ERROR:
                        break
                session.close()

            reader = asyncio.create_task(pump_inbound())
            try:
                await wrapper.run(session, attachment=attachment, params=params)
            except UnauthorizedError:
                with contextlib.suppress(Exception):
                    await ws.send_str(_json.dumps(
                        {"error": "unauthorized", "code": "unauthorized",
                         "message": "Not authorized for this channel."}))
            except Exception as e:
                self._logger.error(
                    "Channel error on %s: %s", wrapper.prefix, e,
                    exc_info=True, extra={"prefix": wrapper.prefix},
                )
            finally:
                session.close()
                reader.cancel()
                with contextlib.suppress(Exception):
                    await reader
                with contextlib.suppress(Exception):
                    await ws.close()
            return ws

        return _handler

    def _make_mcp_handler(self) -> Any:
        """aiohttp POST handler speaking MCP JSON-RPC over the mesh's tools."""
        from aiohttp import web

        from istos.http.mcp import MCPServer

        server = MCPServer(self)

        async def _handler(request: web.Request) -> web.StreamResponse:
            token = extract_bearer(request.headers.get("Authorization"))
            try:
                body = await request.json()
            except Exception:
                return web.json_response(
                    {"jsonrpc": "2.0", "id": None,
                     "error": {"code": -32700, "message": "Parse error"}},
                    status=400,
                )
            if isinstance(body, list):
                out = [r for m in body if (r := await server.handle(m, token=token)) is not None]
                return web.json_response(out)
            resp = await server.handle(body, token=token)
            if resp is None:
                return web.Response(status=202)
            return web.json_response(resp)

        return _handler

    def _register_builtin_handlers(self) -> None:
        if self._builtin_handlers_registered:
            return
        self._builtin_handlers_registered = True

        # Warn if built-ins would be open (they inherit the app-wide authorizer).
        if self._authorizer is None and (self._enable_health or self._enable_metrics or self._enable_discovery):
            exposed = []
            if self._enable_health:
                exposed += [".istos/health", ".istos/ready"]
            if self._enable_metrics:
                exposed.append(".istos/metrics")
            if self._enable_discovery:
                exposed.append(".istos/capabilities")
            warnings.warn(
                f"Built-in endpoints {exposed} are reachable by any peer with no "
                "authorization. Set Istos(authorizer=...) to protect them.",
                IstosSecurityWarning,
                stacklevel=2,
            )

        if self._enable_health:
            register_health_handlers(self, self._health)

        if self._enable_metrics:
            @self.handle(".istos/metrics")
            def _metrics() -> str:
                return self._metrics.export_prometheus()

        if self._enable_discovery:
            @self.handle(".istos/capabilities")
            def _capabilities() -> dict:
                return self.export_capabilities()

    def export_capabilities(self) -> dict:
        """What this node exposes — handlers/streams with schemas when available.

        Served at ``.istos/capabilities``. Query ``**/.istos/capabilities`` (or
        per node) to inventory the fabric. Each entry: ``prefix``, ``kind``,
        optional ``description``, and ``params_schema`` / ``return_schema``.
        """
        from istos.discovery.asyncapi import get_function_schemas

        def _describe(prefix: str, kind: str, func: Callable) -> dict:
            try:
                schemas = get_function_schemas(func)
            except Exception:
                schemas = {}
            entry: dict = {
                "prefix": prefix,
                "kind": kind,
                "description": (inspect.getdoc(func) or "").strip() or None,
            }
            if schemas.get("payload_schema"):
                entry["params_schema"] = schemas["payload_schema"]
            if schemas.get("return_schema"):
                entry["return_schema"] = schemas["return_schema"]
            return entry

        capabilities: List[dict] = []
        # Skip .istos/* plumbing endpoints.
        for h in self._handlers:
            if not h.prefix.startswith(".istos/"):
                capabilities.append(_describe(h.prefix, "handle", h.func))
        for s in self._streams:
            capabilities.append(_describe(s.prefix, "stream", s.func))
        for c in self._channels:
            entry = _describe(c.prefix, "channel", c.func)
            ws_path = next((p for p, w in self._ws_channel_routes if w is c), None)
            if ws_path is not None:
                entry["websocket"] = ws_path
            capabilities.append(entry)
        for p in self._publishers:
            capabilities.append(_describe(p.prefix, "publish", p.func))
        for sub in self._subscribers:
            capabilities.append(_describe(sub.prefix, "subscribe", sub.func))
        return {"service": self._service_name, "capabilities": capabilities}

