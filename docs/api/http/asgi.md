---
title: ASGI API
---

# ASGI API

Run an Istos mesh inside FastAPI or Starlette. The ASGI server owns the HTTP
port; Istos rides the lifespan (`serving()` with HTTP off).

Details: [HTTP Gateway — co-hosting](../../user-guide/http-gateway.md).

`Istos.serving(serve_http=False)` (the default under `lifespan`) leaves HTTP to
the ASGI host. Pass `serve_http=True` only if you also want the embedded
aiohttp surface in the same process.

::: istos.http.asgi
    options:
      show_root_heading: false
      show_source: true
      heading_level: 2
