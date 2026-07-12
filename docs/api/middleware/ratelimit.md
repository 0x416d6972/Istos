---
title: RateLimitMiddleware API
---

# RateLimitMiddleware API

Token-bucket rate limiting for `@handle` / `@stream` / `@channel` /
`@subscribe`. Raises `RateLimitError` (HTTP 429 via the gateway).

Details: [Middleware](../../user-guide/middleware.md#rate-limiting).

::: istos.middleware.ratelimit
    options:
      show_root_heading: false
      show_source: true
      heading_level: 2
