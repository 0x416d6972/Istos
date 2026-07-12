# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-07

### Added
- `@stream` / `stream_query` — chunked RPC replies
- HTTP gateway (`http_port`) for `@handle(..., http=…)` and `@stream(..., http=…)` (SSE)
- `GET /livez`, `/readyz`, `/metrics` when `http_port` is set
- `.istos/capabilities` + `export_capabilities()`
- Request envelope on attachments (`tok` / `cid` / `tp`)
- `require_auth=True` (raises `IstosSecurityError` without an authorizer)
- `JWTAuthorizer`, `require_roles` (`istos[jwt]`)
- `authorizer=` on subscribers
- `attachment=` on `@query`, `query_once`, `publish_once`
- `@publish(persist="s3://…")` / `app.persist(...)` (`istos[s3]`)
- Structured logging (`log_level`, `json_logs`)
- `IstosError` / `ErrorResponse` / `@exception_handler`
- Middleware stack (correlation ID, logging, …)
- `IstosTestClient`
- `.istos/health`, `.istos/ready`, `.istos/metrics`
- Prometheus metrics + optional OTel (`istos[otel]`)
- SIGINT/SIGTERM shutdown
- `RedisStoragePlugin`, `SqlAlchemyStoragePlugin`
- CLI: `istos new` / `docs` / `version`
- CI, Docker Compose, deployment docs, `SECURITY.md`, `py.typed`
- Wire-protocol reference

### Changed
- Logging instead of `print()`
- Handler errors go out as structured JSON replies
- Version → 0.2.0

### Extras
- `istos[all]` = redis + sqlalchemy + otel + s3 + jwt

## [0.1.0] - 2025

### Added
- Initial release
- `@handle`, `@query`, `@publish`, `@subscribe`
- Selector → function args
- Pydantic / type-hint validation at the boundary
- Retry with backoff
- Liveliness
- `InMemoryStoragePlugin`
- JSON + msgpack serializers
- `run()` / `run_async()`
- `IstosRouter`
- TLS/mTLS via `IstosZenohConfig`
- Dependency injection
- Shared memory transfers
- AsyncAPI generator + `serve_docs()`
