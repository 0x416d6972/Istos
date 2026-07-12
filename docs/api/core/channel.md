---
title: Channel API
---

# Channel API

`@channel` is a full-duplex session over Zenoh (and optionally WebSocket).
The handler receives a `ChannelSession`. Callers use `Istos.open_channel` or
`@channel_client`.

Details: [Channels & Agent Sessions](../../user-guide/channels.md).

::: istos.core.channel
    options:
      show_root_heading: false
      show_source: true
      heading_level: 2

::: istos.core.channel_fabric
    options:
      show_root_heading: false
      show_source: true
      heading_level: 2

::: istos.core.client_decorators
    options:
      show_root_heading: false
      show_source: true
      heading_level: 2
