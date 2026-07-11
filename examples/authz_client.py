"""Authorization demo — the CLIENT (queries).

Start `examples/authz_server.py` first, then run this:

    uv run python examples/authz_client.py

It fires the same handlers with different tokens/roles and prints whether each
call was ALLOWED or DENIED, so you can see the authorizer working across the
wire between two separate services.

How a client presents a credential
----------------------------------
The token travels in the Zenoh request *attachment*. Only `query_once` can send
one today (the `@query` decorator can't), so this client uses:

    await istos.query_once("fleet/status", attachment="app-token")

Selector params (like the role) are just keyword args:

    await istos.query_once("fleet/shutdown", attachment="app-token", role="admin")
"""

import asyncio
import contextlib

from istos import Istos


def verdict(reply) -> str:
    """Turn a query reply into a human-readable ALLOWED / DENIED line."""
    # query_once returns the decoded reply (single), a list (many), or [] (none).
    if isinstance(reply, list):
        reply = reply[0] if reply else None
    if reply is None:
        return "NO REPLY (no handler answered / timed out)"
    if isinstance(reply, dict) and reply.get("code") == "unauthorized":
        return f"DENIED   -> {reply.get('message', 'unauthorized')}"
    return f"ALLOWED  -> {reply}"


async def call(istos: Istos, label: str, key: str, *, token=None, **params) -> None:
    reply = await istos.query_once(key, attachment=token, **params)
    print(f"{label:<48} {verdict(reply)}")


async def main() -> None:
    istos = Istos()  # a pure client: registers no handlers, just queries

    # Open a session in the background and let Zenoh discover the server.
    server_task = asyncio.create_task(istos.run_async())
    await asyncio.sleep(2.0)

    print("\n=== Authorization scenarios ===\n")
    try:
        # 1. Public handler — no token, still allowed (opts out of the app gate).
        await call(istos, "health/ping           (no token)", "health/ping")

        # 2. Inherited app-wide gate — no token -> denied.
        await call(istos, "fleet/status          (no token)", "fleet/status")

        # 3. Inherited app-wide gate — correct token -> allowed.
        await call(istos, "fleet/status          (app-token)", "fleet/status",
                   token="app-token")

        # 4. Wrong token -> denied by the app-wide layer.
        await call(istos, "fleet/status          (wrong token)", "fleet/status",
                   token="nope")

        # 5. LAYERED: valid token AND role=admin -> allowed (both layers pass).
        await call(istos, "fleet/shutdown        (app-token + role=admin)",
                   "fleet/shutdown", token="app-token", role="admin")

        # 6. LAYERED: valid token but role=user -> denied by the per-handler layer.
        await call(istos, "fleet/shutdown        (app-token + role=user)",
                   "fleet/shutdown", token="app-token", role="user")

        # 7. LAYERED: right role but no token -> denied by the app-wide layer.
        await call(istos, "fleet/shutdown        (no token  + role=admin)",
                   "fleet/shutdown", role="admin")

        # 8. IDENTITY: the authorizer resolved a Principal; the handler injected
        #    it via Depends(current_principal) and echoes who we are.
        await call(istos, "fleet/whoami          (app-token)", "fleet/whoami",
                   token="app-token")
    finally:
        print()
        server_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server_task


if __name__ == "__main__":
    asyncio.run(main())
