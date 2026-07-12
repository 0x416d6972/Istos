"""HTTP → Zenoh helpers for the ingress gateway.

Turns an HTTP request into a Zenoh query so ``@handle`` / ``@stream`` run the
usual pipeline (auth, validation, DI, middleware):

    POST /robot/move  {"distance": 5}  Authorization: Bearer <token>
        → session.get("robot/move?distance=5", attachment=<token>)
        → @handle("robot/move")
        → JSON (or SSE chunks for ``@stream``)

Parsing / encoding / status mapping live here (no aiohttp). Wire-up is in
:mod:`istos.app`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union
from urllib.parse import quote, unquote

# Istos error ``code`` (from ErrorResponse) → HTTP status. Mirrors the status
# codes the IstosError subclasses carry (which are not on the wire payload).
CODE_TO_STATUS: Dict[str, int] = {
    "unauthorized": 401,
    "forbidden": 403,
    "not_found": 404,
    "validation_error": 400,
    "bad_request": 400,
    "rate_limit_exceeded": 429,
}
DEFAULT_ERROR_STATUS = 500


@dataclass
class HttpRoute:
    """HTTP path mapped to a Zenoh key. ``sse=True`` → stream as SSE."""

    method: str
    path: str
    key_expr: str
    timeout_s: float = 5.0
    sse: bool = False


def parse_http_spec(
    spec: Union[bool, str],
    prefix: str,
    timeout_s: float = 5.0,
    *,
    sse: bool = False,
) -> HttpRoute:
    """Turn a handler's ``http=`` value into an :class:`HttpRoute`.

    * ``True``            → ``POST /<prefix>`` (``GET`` for ``sse=True``)
    * ``"/custom/path"``  → ``POST /custom/path`` (``GET`` for ``sse=True``)
    * ``"GET /things"``   → method + path

    SSE routes default to ``GET`` (what ``EventSource`` uses); an explicit method
    wins.
    """
    default_method = "GET" if sse else "POST"
    default_path = "/" + prefix.lstrip("/")
    if spec is True:
        return HttpRoute(default_method, default_path, prefix, timeout_s, sse)
    if isinstance(spec, str):
        parts = spec.split()
        if len(parts) == 1:
            method, path = default_method, parts[0]
        elif len(parts) == 2:
            method, path = parts[0], parts[1]
        else:
            raise ValueError(
                f"Invalid http spec {spec!r}: expected 'METHOD /path', '/path', or True."
            )
        if not path.startswith("/"):
            path = "/" + path
        return HttpRoute(method.upper(), path, prefix, timeout_s, sse)
    raise ValueError(f"Invalid http spec {spec!r}: expected str or True.")


def sse_event(data: str, event: Optional[str] = None, *, id: Optional[str] = None) -> str:
    """Format one SSE frame. Multi-line ``data`` becomes one ``data:`` line each;
    a blank line terminates the frame."""
    lines = []
    if id is not None:
        lines.append(f"id: {id}")
    if event is not None:
        lines.append(f"event: {event}")
    for line in data.split("\n"):
        lines.append(f"data: {line}")
    return "\n".join(lines) + "\n\n"


def extract_bearer(auth_header: Optional[str]) -> Optional[str]:
    """Pull the token out of an ``Authorization`` header.

    Accepts ``Bearer <token>`` (case-insensitive scheme) or a bare token.
    """
    if not auth_header:
        return None
    parts = auth_header.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return auth_header.strip()


def encode_params(params: Dict[str, Any]) -> Dict[str, str]:
    """Stringify request params for a Zenoh selector query string.

    Scalars become their string form; ``None`` is dropped; nested
    objects/arrays are JSON-encoded (the handler's validator can parse them back).
    """
    out: Dict[str, str] = {}
    for k, v in params.items():
        if v is None:
            continue
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (str, int, float)):
            out[k] = str(v)
        else:
            out[k] = json.dumps(v)
    return out


def decode_params(raw: Dict[str, str]) -> Dict[str, str]:
    """Percent-decode Zenoh selector parameters on the server side.

    Clients percent-encode keys/values (so ``;``, ``=``, spaces survive the
    selector syntax), but Zenoh does **not** decode them on receipt — so handlers
    must. Decodes once, matching the single ``quote`` in :func:`build_selector`
    and the query client.
    """
    return {unquote(k): unquote(v) for k, v in raw.items()}


def build_selector(key_expr: str, params: Dict[str, Any]) -> str:
    """Combine a key expression with encoded params into a Zenoh selector.

    Zenoh separates selector parameters with ``;`` (not ``&``); keys and values
    are percent-encoded so reserved characters survive.
    """
    encoded = encode_params(params)
    if not encoded:
        return key_expr
    query = ";".join(f"{quote(str(k))}={quote(str(v))}" for k, v in encoded.items())
    return f"{key_expr}?{query}"


def is_error_payload(parsed: Any) -> bool:
    """Whether a decoded reply is an Istos ``ErrorResponse`` wire payload."""
    return isinstance(parsed, dict) and all(
        field in parsed for field in ("error", "code", "message")
    )


def status_for_reply(parsed: Any) -> int:
    """HTTP status for a decoded handler reply (200, or mapped from its error code)."""
    if is_error_payload(parsed):
        return CODE_TO_STATUS.get(parsed["code"], DEFAULT_ERROR_STATUS)
    return 200
