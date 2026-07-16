---
title: Wire Protocol (Polyglot)
---

# Istos Wire Protocol

Istos is a Python framework, but it puts **nothing proprietary on the wire** — it
speaks plain [Zenoh](https://zenoh.io). Any language with a Zenoh client (Rust, C,
C++, Python, Kotlin/Java, TypeScript/JS, …) can call an Istos service, be called
by one, or publish/subscribe alongside one, by following the conventions on this
page. There is no Istos runtime to port; there is only this contract.

This document is the normative spec for cross-language interop.

## What you need

A Zenoh session in your language, joined to the same Zenoh network as the Istos
service (same multicast scouting domain, or a shared router/endpoint). Everything
below is expressed in Zenoh primitives: **key expressions**, **queryables/get**
(for RPC), **put/subscribe** (for pub/sub), and **attachments** (for auth tokens).

## 1. Key expressions

Istos endpoints are addressed by ordinary Zenoh key expressions — the `prefix`
you pass to a decorator *is* the key expression:

| Istos decorator | Zenoh primitive | Key |
|---|---|---|
| `@handle("robot/move")` | queryable | `robot/move` |
| `@query("robot/move")` | get (client) | `robot/move` |
| `@publish("drone/telemetry")` | put | `drone/telemetry` |
| `@subscribe("drone/telemetry")` | subscriber | `drone/telemetry` |

Wildcards follow Zenoh semantics: `*` matches one chunk, `**` matches zero or
more. Reserved built-in endpoints live under `.istos/`:

| Key | Meaning |
|---|---|
| `.istos/health` | liveness (returns `{"status":"alive",...}`) |
| `.istos/ready` | readiness |
| `.istos/metrics` | metrics snapshot |
| `.istos/capabilities/<service>` | machine-readable capability manifest (discovery) |
| `.istos/capabilities` | the same manifest on a key shared by every node; answers for one of them |
| `.istos/docs` | AsyncAPI document (when `serve_docs` is enabled) |

## 2. Serialization

Payloads are raw Zenoh bytes. The default codec is **JSON (UTF-8)**; two others
are available per-endpoint.

| Codec | Bytes on the wire |
|---|---|
| **JSON** (default) | `json(value)` encoded UTF-8. Non-native types (datetime, UUID, Decimal…) are stringified. |
| **MessagePack** | `msgpack.packb(value, use_bin_type=True)` (string vs binary preserved). |
| **Raw** | the bytes verbatim (opaque/binary payloads). |

Both peers of an endpoint must agree on the codec. JSON is the interop default;
choose it unless you control both sides.

## 3. RPC — calling an Istos handler

An `@handle(...)` handler is a Zenoh **queryable**. To invoke it, issue a Zenoh
**get**.

**Request:**

- **Selector** = `<key>?<params>`. Parameters are the handler's arguments,
  **separated by `;`** (Zenoh's separator — *not* `&`), keys and values
  percent-encoded. Example: `robot/move?distance=5;speed=fast`.
  The handler coerces each string value to its declared parameter type
  (ints, floats, bools, and Pydantic model fields).
- **Attachment** = the auth token, if any (see [§5 Attachment envelope](#attachment-envelope)).

**Response:** a single reply whose **payload** is the serialized return value.

**Example (Rust):**

```rust
let replies = session
    .get("robot/move?distance=5;speed=fast")
    .attachment("Bearer eyJhbGci…")   // token, if the endpoint is gated
    .await
    .unwrap();
let reply = replies.recv_async().await.unwrap();
let body = reply.result().unwrap().payload().to_bytes();   // JSON bytes
```

A handler that returns nothing sends no reply. Errors are replied as an error
envelope ([§6](#6-error-envelope)) — inspect the payload, not the transport.

### Streaming RPC

A streaming handler (`@stream`) is a **multi-reply queryable**: it calls
`query.reply(key, chunk)` once per chunk and finalizes when done. To consume a
stream from another language, issue the `get` with **`consolidation = None`** and
iterate replies as they arrive — each reply payload is one chunk, in send order.
(Without `None`, Zenoh's reply consolidation would collapse same-key chunks to the
last one.) An error envelope may appear as a final chunk. Selector params and the
attachment envelope work exactly as for single-reply RPC.

## 4. Pub/Sub

**Publish** to an Istos `@subscribe` by putting the serialized message on the key:

```rust
session.put("drone/telemetry", serde_json::to_vec(&telemetry)?)
       .attachment("Bearer …")     // if the subscriber is gated
       .await?;
```

**Subscribe** to an Istos `@publish` by declaring a subscriber on the key and
deserializing each sample's payload.

### Durable pub/sub

`durable=True` endpoints use Zenoh's **advanced** pub/sub (`zenoh.ext`):
`AdvancedPublisher` with a replay cache + heartbeat, and `AdvancedSubscriber`
with history + recovery. To interoperate durably, use your language's advanced
pub/sub API against the same key.

### Persisted streams

When a publisher sets `persist="s3://…"`, each sample is also stored under a
**per-sample key** of the form `<key>/<unix_millis>-<seq>` and served back by a
queryable. To fetch history from another language, **get the wildcard**:

```
session.get("drone/telemetry/**")   // each reply payload is one historical sample
```

Query the wildcard, not the bare key — replies carry distinct per-sample keys so
Zenoh's reply consolidation does not collapse the stream.

<a id="attachment-envelope"></a>
## 5. Attachment envelope (auth + correlation + tracing)

Istos uses the Zenoh **attachment** — on the query (RPC) or sample (pub/sub) — as
its per-request out-of-band channel. It carries the auth token and, across hops,
request metadata. The attachment takes one of two UTF-8 forms:

- **Bare token** — the attachment is just the token string (e.g. `"my-secret"`
  or a JWT `"eyJ…"`). Simplest, and the recommended form for other-language
  clients that only need auth.
- **JSON envelope** — a compact object with short keys, used when metadata must
  travel with the request:

  ```json
  { "tok": "<auth token>", "cid": "<correlation id>", "tp": "<W3C traceparent>" }
  ```

Parsing rule: an attachment that is a JSON object containing at least one of
`tok` / `cid` / `tp` is an **envelope**; anything else is a **bare token**. So a
JWT or opaque secret is never mistaken for an envelope.

### Authentication

The authorizer reads the token as `AuthContext.token` (from either form):

- **`TokenAuthorizer`**: token = the shared secret string.
- **`JWTAuthorizer`**: token = the JWT compact string. A leading `Bearer ` scheme
  is accepted and stripped, so both `eyJ…` and `Bearer eyJ…` work.

Set the attachment on every gated call — there is no separate handshake. Absent
or invalid tokens are denied: RPC gets an error-envelope reply
([§6](#6-error-envelope)), a pub/sub sample is dropped.

### Cross-hop correlation & tracing

`cid` (correlation id) and `tp` (W3C `traceparent`) link the hops of one logical
operation. An Istos service **inherits** them from the inbound envelope and
**re-emits** them on any query/publish it makes while handling the request, so a
whole call chain shares one correlation id and one trace. To participate from
another language:

- **inbound**: if the attachment envelope has `cid`/`tp`, adopt them for this
  hop's logs/spans; otherwise mint your own.
- **outbound**: when you call further services while handling a request, put your
  current `cid`/`tp` in the envelope you send.

If you only need auth, you can ignore `cid`/`tp` and keep sending a bare token —
correlation is additive, never required.

## 6. Error envelope

When a handler denies or fails, the reply payload is a JSON object (even under a
non-JSON codec, errors are JSON):

```json
{
  "error": "unauthorized",
  "code": "unauthorized",
  "message": "Not authorized for 'robot/move'",
  "correlation_id": "…",        // optional
  "details": { }                 // optional
}
```

`code` is stable and maps to the HTTP status the [gateway](../user-guide/http-gateway.md)
would return:

| `code` | Meaning | HTTP |
|---|---|---|
| `unauthorized` | missing/invalid token | 401 |
| `forbidden` | authenticated, lacks role | 403 |
| `not_found` | no such resource | 404 |
| `validation_error` | params failed schema | 400 |
| `rate_limit_exceeded` | throttled | 429 |
| `internal_error` (or other) | handler error | 500 |

A reply is an error envelope iff it is a JSON object containing `error`, `code`,
and `message`. Anything else is a normal result. All three fields are required —
a payload carrying only some of them is a normal result, and a client written to
this contract will hand it to the caller as data.

A client reading a single reply is expected to raise on the envelope rather than
return it, since it otherwise answers field lookups like any other object and an
outage becomes an empty answer. (The Python client does this in `query_once`,
`@query`, `stream_query` and `open_channel`; `istos.is_error_payload` is the
check.) A **multi-reply** result is different: one responder failing is not the
call failing, so each reply is judged on its own.

## 7. Serving an Istos-compatible endpoint from another language

The contract is symmetric — a non-Python service can *be* an Istos handler:

1. Declare a **queryable** on the key expression.
2. Parse arguments from the query's selector parameters (`;`-separated).
3. Optionally read the attachment token and authorize.
4. Reply with the serialized result on the query's key, or an error envelope.

Publishers/subscribers are the mirror of [§4](#4-pubsub). Because it is all
Zenoh, an Istos Python client (`@query`) and a Rust queryable interoperate with
no gateway and no broker.

## 8. Capability discovery

Every node answers `.istos/capabilities/<service_name>` with a machine-readable
manifest of what it can do, for agents that discover tools at runtime rather than
hard-coding them:

```json
{
  "service": "fleet",
  "capabilities": [
    { "prefix": "robot/move", "kind": "handle",
      "description": "Move the robot.",
      "params_schema": { "type": "object", "properties": { "distance": {"type":"integer"} }, "required": ["distance"] } },
    { "prefix": "llm/generate", "kind": "stream", "description": "…",
      "params_schema": { … } }
  ]
}
```

`kind` is `handle` / `stream` / `channel` / `publish` / `subscribe`;
`params_schema` / `return_schema` are JSON Schema. Channel entries may include
`websocket` when `ws=` is set. The endpoint inherits the app-wide authorizer;
disable it with `Istos(enable_discovery=False)`.

Query `.istos/capabilities/*` for a fleet-wide tool catalog, then invoke the tools
using the RPC/stream/channel conventions. Two rules a client must follow:

* **Ask on the per-service key.** Every node also answers the bare
  `.istos/capabilities`, which is the same key everywhere. Queryables are declared
  complete for their key expression, so a query on the shared key is routed to one
  node and the rest never see it — including through a wildcard, since the key is
  identical on every node. Only the per-service keys are distinct enough to fan
  out. The bare key remains for older clients and describes whichever node
  answered.
* **Turn reply consolidation off** (`ConsolidationMode::None`) on the wildcard.
  Zenoh's default consolidation drops replies that arrive on different keys, so a
  catalog built with it is quietly incomplete.

A service name is free text and a key chunk is not: replace anything outside
`[A-Za-z0-9_.-]` with `-`. Services sharing a name share a key, and one of them
answers; replicas of one service are expected to share it. The manifest repeats
its `service` field, so a client does not have to parse keys.

## 9. Not on the fabric?

If a service cannot speak Zenoh (a browser, a managed platform, an existing HTTP
system), use the [HTTP gateway](../user-guide/http-gateway.md) instead: it bridges
HTTP → the same Zenoh queries described here, forwarding the `Authorization`
header as the attachment token. For duplex agents, use WebSocket
(`@channel(..., ws=…)`) or bridge via FastAPI + `open_channel` — see
[Channels](../user-guide/channels.md).

## 10. Bidirectional channels

An Istos `@channel` is a duplex session over Zenoh (HTTP WebSocket is an
optional edge transport for the same handler).

### Open handshake

1. Client picks a session id `S` (hex UUID) and optionally a `conversation_id`
   for durable resume.
2. Client declares a subscriber on `{P}/{S}/down` and a liveliness token at
   `{P}/{S}` **before** opening (so early replies are not lost).
3. Client `get`s `{P}/{S}/open?conversation_id=…` with the attachment envelope
   (auth). Consolidation does not matter for a single reply.
4. Server authorizes, then replies `{"ok": true, "sid": "<S>"}` and starts the
   handler with a `ChannelSession`.

### Session keys

| Key | Direction | Role |
|-----|-----------|------|
| `{P}/{S}/up` | client → server | inbound messages to the handler |
| `{P}/{S}/down` | server → client | outbound messages from `session.send` |
| `{P}/{S}` | liveliness | client presence; drop → server closes |

Payloads are the channel serializer's bytes (JSON UTF-8 by default). Closing:
drop the liveliness token / unsubscribe; the peer's `receive` / `async for`
ends with a closed session.

Wire details and Python helpers live in `istos.primitives.channel_fabric`.
