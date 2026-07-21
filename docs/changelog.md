# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-07-21

### Added

- Error replies now carry an explicit `__istos_error` discriminator, so a failure is recognised by one field rather than guessed from body shape. It is authoritative in both directions: `true` is an error, `false` marks a normal result even when the success value legitimately carries `error`/`code`/`message` keys — closing the false-positive where such a success was misread as a failure. The change is backward-additive: when the field is absent (an older responder, or a client in another language), detection falls back to the legacy rule of a dict carrying all three of `error`, `code` and `message`, so nothing on the wire breaks. New helper `istos.reply_err(...)` builds a stamped envelope a handler can `return` instead of raising.

## [0.2.0] - 2026-07-17

### Changed

- **Breaking.** Capability manifests are served per service at `.istos/capabilities/<service_name>`, and `app.discover_capabilities()` returns every service's manifest keyed by name. The bare `.istos/capabilities` is unchanged and still answers, but it cannot answer for a fleet: every node serves it on the same key and `@handle` declares its queryable `complete=True`, so Zenoh asks exactly one node and never reaches the rest. A wildcard did not help either, since the key was identical everywhere. Distinct keys are what Zenoh fans out over, the same way `*/health` reaches `a/health` and `b/health`. Services sharing a name share a key and one of them answers, so name them distinctly; replicas of one service are meant to share it, as the manifest describes the service rather than the process.

    ```python
    # one arbitrary node, whichever Zenoh picked
    manifest = await app.query_once(".istos/capabilities")

    # every service
    fleet = await app.discover_capabilities()
    for service, manifest in fleet.items():
        ...
    ```

- **Breaking.** `query_once` and `@query` raise when the responder failed, instead of handing back the error payload as data. A handler that raises replies with an `ErrorResponse` payload, which is an ordinary dict on the wire, so `reply.get("clients")` on a failed reply returned `[]` and an outage could not be told apart from an empty result. `stream_query` and `open_channel` already raised; queries now match them. The `code` picks the class, so `except NotFoundError` (also `UnauthorizedError`, `ForbiddenError`, `RateLimitError`) works across a hop; any other code arrives as `IstosError` with its `code` kept, and `correlation_id` comes with it for matching the responder's log line. Callers that tested for errors by hand can drop that code:

    ```python
    # before
    reply = await app.query_once("clients/get", id="acme")
    if reply and reply.get("code") == "not_found":
        return None

    # after
    try:
        return await app.query_once("clients/get", id="acme")
    except NotFoundError:
        return None
    ```

    Multi-reply queries (several responders on one key) are unchanged: the list holds whatever each responder said, error envelopes included. Use `is_error_payload` on each.

- **Breaking.** The queue calls check the same envelope, at the single Zenoh get they all share. `app.result(...)` reported `{"state": "unknown"}` for a refusal, `app.dead_letters(...)` reported `[]`, and `app.enqueue(...)` raised `UnauthorizedError` whatever the owner actually sent. All three now raise the owner's error. A worker whose token the owner rejects died on a `KeyError`; it now logs the refusal and keeps polling.

- **Breaking.** The queue owner's refusal reply is a standard `ErrorResponse` envelope. It carried `error` and `code` but no `message`, which is why callers had to hand-roll the check. Its `error` field now holds the code, as everywhere else, so a caller reading `reply["error"]` sees `unauthorized` rather than the sentence.

- **Breaking.** `retry=` no longer retries an error the responder will only repeat. `not_found`, `unauthorized`, `forbidden` and `validation_error` fail on the first attempt; `rate_limit_exceeded`, 5xx and transport faults retry as before. This applies to `@handle` retry as well, so a handler raising `NotFoundError` under `retry=3` runs once. `is_retryable(exc)` is the rule.

- `IstosError` takes `correlation_id` as a constructor argument instead of having it attached afterwards.

- `CODE_TO_STATUS` and `DEFAULT_ERROR_STATUS` moved from `istos.http.gateway` to `istos.errors`, re-exported from their old home. The status decides retryability and is not on the wire, so an error rebuilt from a reply recovers it from the code rather than defaulting to 500.

### Added

- `query_once(..., consolidate_replies=False)` for wildcard fan-out. Zenoh consolidates replies by default and drops some even when the responders answered on different keys, so a `*/health` sweep could silently return a subset. `discover_capabilities()` uses it.
- `is_error_payload(reply)`, `error_from_payload(reply)` and `is_retryable(exc)` on the top-level `istos` namespace, for replies you decode yourself and for multi-reply results. `is_error_payload` previously lived in `istos.http.gateway`, which put an HTTP import on the fabric path; it is re-exported there.

### Fixed

- The RPC guide said that multiple handlers on one key produce a list. They do not. `@handle` declares its queryable `complete=True`, meaning one responder can answer the whole key, so Zenoh asks exactly one and the others are never asked; the default reply consolidation collapses same-key replies in any case. Fan-out needs distinct keys, as in `*/health` over `a/health` and `b/health`, which does work. The same applies to `.istos/capabilities`, which every node serves on the identical key: a query returns one arbitrary node's manifest, and a wildcard does not help. It is a self-description endpoint, not fleet discovery. Namespacing the manifest per service would fix it and is a wire change, so it is not in 0.1.x.

### Known limits

- An error is recognised by shape, not by the transport: a reply is an error if it is a dict carrying `error`, `code` and `message`. A handler that legitimately returns all three is read as a failure. The structural fix is out-of-band signalling, either Zenoh's `Query.reply_err()` or a flag in the request envelope Istos already attaches, and both change the wire protocol. **Addressed in [Unreleased]** — an in-band `__istos_error` discriminator (backward-additive, no wire break) rather than out-of-band signalling.

## [0.1.2] - 2026-07-15

### Fixed
- `@stream` now ends its Zenoh query as soon as the generator finishes, instead of leaving it to be garbage-collected. The consumer's `get()` returns only once every matching queryable has finished, so the lingering query left every `stream_query` / SSE client waiting out the full timeout after the last chunk — a browser `EventSource` sat idle for `http_timeout_s` (60s by default) on a stream that had already completed, and `event: end` arrived only when the timeout expired. Streams now close immediately; the test suite dropped from 260s to 67s as a side effect.

### Added
- `examples/fable-workflow` gained `--serve`: the loop behind `GET /run` as SSE, so it can be driven with `curl -N` instead of the CLI.

## [0.1.1] - 2026-07-15

### Added
- `JobContext` — a worker that names a `ctx` parameter is handed the delivery's `job_id`, `queue`, `attempt`, `max_attempts` and `last_error` (the previous attempt's failure), plus `is_retry` / `is_last_attempt`. Opt-in and additive: a worker without `ctx` is unchanged. Resolves alongside `Depends(...)`, which still wins on the same name.
- `examples/fable-workflow` — the Fable Method as four cooperating nodes over work queues, driven by a local LLM.

### Changed
- The queue owner's claim reply now carries `last_error`, so a redelivered job can tell its worker why the last attempt failed. Backward compatible in both directions: an older worker ignores the field, and a newer worker against an older owner sees `last_error=None`.

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
