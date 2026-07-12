# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-07

### Added
- Streaming RPC: `@stream` / `stream_query` for chunked replies (SLM/LLM tokens)
- HTTP ingress gateway: `Istos(http_port=…)` with `@handle(..., http=…)` JSON routes
- HTTP SSE bridge: `@stream(..., http=…)` → `text/event-stream` for browsers / FastAPI
- K8s-friendly HTTP probes: `GET /livez`, `/readyz`, `/metrics` when `http_port` is set
- Capability discovery: `.istos/capabilities` tool manifest (`enable_discovery`, `export_capabilities()`)
- Request envelope: correlation ID + W3C `traceparent` (+ token) on Zenoh attachments
- Fail-closed auth mode: `Istos(require_auth=True, authorizer=…)` → `IstosSecurityError` if no authorizer
- `JWTAuthorizer` and `require_roles` (`istos[jwt]`)
- Subscriber-side `authorizer=` gating
- Attachment support on `@query`, `query_once`, and `publish_once`
- Brokerless producer-crash persistence: `@publish(persist="s3://…")` / `app.persist(...)` (`istos[s3]`)
- Structured logging with JSON output (`log_level`, `json_logs`)
- Standard error protocol with `IstosError`, `ErrorResponse`, and `@exception_handler`
- Middleware pipeline (`add_middleware`) with logging and correlation ID support
- `IstosTestClient` for in-process testing without Zenoh networking
- Built-in health (`.istos/health`), readiness (`.istos/ready`), and metrics (`.istos/metrics`) endpoints
- Prometheus-compatible metrics collector
- OpenTelemetry tracing integration (`pip install 'istos[otel]'`)
- Graceful shutdown on SIGINT/SIGTERM
- `RedisStoragePlugin` (`pip install 'istos[redis]'`)
- `SqlAlchemyStoragePlugin` for any SQL database (`pip install 'istos[sqlalchemy]'` + your async driver)
- CLI: `istos new`, `istos docs`, `istos version`
- CI/CD workflows (test matrix, lint, docs deploy, PyPI publish)
- Docker Compose with Zenoh router, Redis, and Postgres
- Deployment and testing documentation
- `SECURITY.md` and `py.typed` marker
- Wire-protocol reference for polyglot peers

### Changed
- Replaced `print()` statements with structured logging throughout
- Handler errors now return standardized JSON error responses on the wire
- Version bumped to 0.2.0

### Optional extras
- `istos[all]` now includes `redis`, `sqlalchemy`, `otel`, `s3`, and `jwt`

## [0.1.0] - 2025

### Added
- Initial release of Istos framework
- Decorator-based API: `@handle`, `@query`, `@publish`, `@subscribe`
- Smart Selectors with automatic parameter mapping
- Pydantic schema validation at the network boundary
- Retry policies with exponential backoff
- Liveliness tracking for node discovery
- Pluggable storage: `InMemoryStoragePlugin`
- Pluggable serialization: `JsonSerializer` with msgpack support
- Async & Sync compatibility (`run()` / `run_async()`)
- `IstosRouter` for modular route organization
- Transport security with TLS/mTLS via `IstosZenohConfig`
- Dependency injection system
- Shared memory (zero-copy) support for high-performance transfers
- Built-in AsyncAPI 3.0.0 documentation generator
- Embedded web documentation server via `serve_docs()`
