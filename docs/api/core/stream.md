---
title: Stream API
---

# Stream API

`@stream` turns an async generator into a multi-reply Zenoh queryable. Each
`yield` is one chunk. Callers use `Istos.stream_query`.

For HTTP, pass `http=True` (or `http="GET /path"`) — chunks go out as SSE.
Details: [RPC](../../user-guide/rpc.md), [HTTP Gateway](../../user-guide/http-gateway.md).

::: istos.core.stream
    options:
      show_root_heading: false
      show_source: true
      heading_level: 2
