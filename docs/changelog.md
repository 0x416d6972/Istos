# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07

### Added
- `@handle` / `@query` — 1-to-1 RPC with selector → function args
- `@stream` / `stream_query` — chunked RPC replies
- `@channel` / `open_channel` / `ChannelClient` — duplex agent sessions (WebSocket via `ws=`, fabric via Zenoh; `?conversation_id=` resume on WS)
- `@stream_client` / `@channel_client` — declarative clients (mirrors `@query`)
- `@channel(durable=True)` + `SessionStore` — resumable conversations over the app storage ledger
- `@publish` / `@subscribe` — 1-to-many events; `durable=True` brokerless replay
- `@publish(persist="s3://…")` / `app.persist(...)` / `app.replay(...)` (`istos[s3]`)
- Work queues — `app.queue(...)` owner, `@app.worker(...)` competing consumers, `app.enqueue(...)`, `app.dead_letters(...)`. Lease-based redelivery, exponential-backoff retries, dead-letter, delayed jobs (`delay_s`) + priorities, result backend (`keep_results` / `app.result(...)`), periodic scheduling (`app.schedule(...)`, interval or `cron=`), workflows (`app.chain` / `app.group` / `app.chord`), owner failover (`ha=True`, liveliness leader election), O(log n) heap store, push-nudge delivery; durable via the storage plugin
- HTTP gateway (`http_port`) for `@handle(..., http=…)` and `@stream(..., http=…)` (SSE)
- `Istos(enable_mcp=True)` — MCP JSON-RPC tools from `@handle` endpoints (`/mcp`; batch + 202 notifications)
- `istos.http.asgi.lifespan` / `Istos.serving(serve_http=…)` — co-host the mesh inside FastAPI/Starlette
- `GET /livez`, `/readyz`, `/metrics` when `http_port` is set
- `.istos/capabilities` + `export_capabilities()` (includes `channel` + optional `websocket`)
- AsyncAPI / `serve_docs()` includes `@stream` and `@channel`
- Request envelope on attachments (`tok` / `cid` / `tp`)
- `require_auth=True` (raises `IstosSecurityError` without an authorizer)
- `JWTAuthorizer`, `require_roles` (`istos[jwt]`)
- `authorizer=` on subscribers
- `token=` on `@query`, `query_once`, `publish_once`, `stream_query`, `open_channel`
- `RateLimitMiddleware` — token-bucket rate limits
- Middleware wraps `@handle`, `@stream`, `@channel`, `@subscribe` (stream/channel once per session)
- Structured logging (`log_level`, `json_logs`)
- `IstosError` / `ErrorResponse` / `@exception_handler`
- `IstosTestClient` (query, stream, channel, publish)
- `.istos/health`, `.istos/ready`, `.istos/metrics`
- Prometheus metrics + optional OTel (`istos[otel]`)
- SIGINT/SIGTERM shutdown
- `InMemoryStoragePlugin`, `RedisStoragePlugin`, `SqlAlchemyStoragePlugin`
- Pydantic / type-hint validation at the boundary
- Retry with backoff
- Liveliness
- Dependency injection
- Shared memory transfers
- CLI: `istos new` / `docs` / `version` / `analyze`
- Architecture fitness (`istos analyze`) — abstractness / instability / distance
- CI, Docker Compose, deployment docs, `SECURITY.md`, `py.typed`
- Wire-protocol reference (including `@channel` fabric keys)

### Extras
- `istos[all]` = redis + sqlalchemy + otel + s3 + jwt
