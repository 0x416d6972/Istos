---
title: Stream API
---

# Stream API Reference

Streaming (multi-reply) RPC powered by the `@stream` decorator — async generators
whose `yield`s become progressive reply chunks (e.g. SLM/LLM tokens).

Consume with `Istos.stream_query`. Optional HTTP SSE ingress via
`@stream(..., http=True)` — see [HTTP Gateway](../../user-guide/http-gateway.md)
and [RPC streaming](../../user-guide/rpc.md).

::: istos.core.stream
    options:
      show_root_heading: false
      show_source: true
      heading_level: 2
