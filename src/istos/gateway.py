"""HTTP → Zenoh ingress gateway helpers.

The gateway exposes selected ``@handle`` endpoints over HTTP so non-Zenoh callers
(FastAPI services, browsers, external partners) can invoke them. An HTTP request
is translated into a Zenoh query against the handler's key expression, so it flows
through the *entire* handler pipeline — authorization, validation, DI, middleware
— with nothing bypassed:

    HTTP POST /robot/move  {"distance": 5}   Authorization: Bearer <token>
        → session.get("robot/move?distance=5", attachment=b"<token>")
        → the @handle("robot/move") queryable runs
        → its reply is returned as the HTTP JSON response

The ``Authorization`` header is forwarded as the Zenoh query **attachment**, which
is where the authorizer reads the token (``current_token``) — so the auth gate and
``Principal`` work across the HTTP boundary.

This module holds the pure, network-free logic (spec parsing, param encoding,
status mapping) so it is unit-testable without aiohttp or a live session; the
aiohttp wiring lives in :mod:`istos.app`.
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
    """One HTTP route bridged to a Zenoh handler key expression."""

    method: str
    path: str
    key_expr: str
    timeout_s: float = 5.0


def parse_http_spec(spec: Union[bool, str], prefix: str, timeout_s: float = 5.0) -> HttpRoute:
    """Turn a handler's ``http=`` value into an :class:`HttpRoute`.

    * ``True``            → ``POST /<prefix>``
    * ``"/custom/path"``  → ``POST /custom/path``
    * ``"GET /things"``   → method + path
    """
    default_path = "/" + prefix.lstrip("/")
    if spec is True:
        return HttpRoute("POST", default_path, prefix, timeout_s)
    if isinstance(spec, str):
        parts = spec.split()
        if len(parts) == 1:
            method, path = "POST", parts[0]
        elif len(parts) == 2:
            method, path = parts[0], parts[1]
        else:
            raise ValueError(
                f"Invalid http spec {spec!r}: expected 'METHOD /path', '/path', or True."
            )
        if not path.startswith("/"):
            path = "/" + path
        return HttpRoute(method.upper(), path, prefix, timeout_s)
    raise ValueError(f"Invalid http spec {spec!r}: expected str or True.")


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
