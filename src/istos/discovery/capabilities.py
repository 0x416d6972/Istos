"""Where a node's capability manifest lives on the fabric.

Two keys, because one of them cannot answer for a fleet:

``.istos/capabilities``
    The same key on every node. ``@handle`` declares its queryable
    ``complete=True`` — one responder can answer the whole key — so Zenoh asks
    exactly one node and the others are never reached. A wildcard does not help:
    the key is identical everywhere, so it still resolves to that one key
    expression. Kept for callers written against it; it answers for whichever
    node Zenoh picked.

``.istos/capabilities/<service>``
    A key per service, so ``.istos/capabilities/*`` matches many distinct
    queryables and every node answers. Same shape as ``*/health`` over
    ``a/health`` + ``b/health``, which is the only fan-out Zenoh performs.

The manifest carries its own ``service`` field, so a caller reading the wildcard
does not have to parse keys.
"""

from __future__ import annotations

import re

CAPABILITIES_KEY = ".istos/capabilities"
CAPABILITIES_WILDCARD = ".istos/capabilities/*"

# `*`, `?`, `#`, `$` are selector syntax and `/` is the chunk separator, so a
# service name (free text) cannot be dropped into a key as-is.
_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")


def capabilities_key(service_name: str) -> str:
    """The key this service answers its manifest on.

    Services sharing a name share the key, and only one of them answers — name
    them distinctly to be discoverable separately. Replicas of one service are
    meant to share it: the manifest describes the service, not the process.
    """
    chunk = _UNSAFE.sub("-", service_name or "").strip("-")
    return f"{CAPABILITIES_KEY}/{chunk or 'istos'}"
