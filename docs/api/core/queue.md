---
title: Work Queue API
---

# Work Queue API

Brokerless work queues: a `QueueRole` owner (enqueue / claim / ack / nack + lease
sweeper) and the competing-consumer `worker` loop. `QueueStore` holds the job
state in memory and writes it through to the app's `StoragePlugin`.

Guide: [Work Queues](../../user-guide/work-queues.md).

::: istos.core.queue
    options:
      show_root_heading: false
      show_source: true
      heading_level: 2
