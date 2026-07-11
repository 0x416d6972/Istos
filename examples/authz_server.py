"""Authorization demo — the SERVER (handlers).

Run this in one terminal:

    uv run python examples/authz_server.py

Then run the client in another:

    uv run python examples/authz_client.py

------------------------------------------------------------------------------
This service exposes four handlers, each showing a different layer of the
authorization model:

  fleet/status    -> inherits the APP-WIDE authorizer  (needs "app-token")
  fleet/shutdown  -> LAYERED: app-wide AND admins_only  (needs "app-token" + role=admin)
  fleet/whoami    -> injects the resolved identity with Depends(current_principal)
  health/ping     -> Public: opts OUT of the app-wide gate (no token needed)

IDENTITY
--------
The app-wide authorizer here doesn't just return True/False — it returns a
`Principal` describing *who* the caller is. Istos stashes it on the request and
any handler can inject it with `Depends(current_principal)`. The gate resolves
identity once; the handler body reuses it (no re-parsing the token).

NOTE ON THE TWO LAYERS
----------------------
This demo has NO *authentication* (no TLS, no Zenoh username/password), so Istos
prints an IstosSecurityWarning: "any peer can invoke handlers." That is exactly
the point — *authorization* (this file's `authorizer=`) works INDEPENDENTLY of
transport authentication. In production you would ALSO configure
IstosZenohConfig(username=..., password=..., tls=...) to keep strangers off the
fabric in the first place. See docs/user-guide/authorization.md.
"""

from istos import (
    Istos,
    AuthContext,
    Principal,
    Public,
    Depends,
    current_principal,
)

# Map known tokens to identities. In a real app this is a JWT decode, a session
# lookup, an API-key table — anything that turns a credential into a principal.
_IDENTITIES = {"app-token": Principal(id="ops-console", roles=frozenset({"operator"}))}


# --- App-wide authorizer: the BASELINE applied to every handler that doesn't
#     opt out. It returns a Principal (allowed + WHO) instead of a bare bool, so
#     handlers can inject the identity. Unknown/absent token -> None -> denied. ---
def authenticate(ctx: AuthContext) -> Principal | None:
    principal = _IDENTITIES.get(ctx.token)
    print(f"    [authz] authenticate token={ctx.token!r} -> {principal} on {ctx.key_expr}")
    return principal  # a falsy None denies; a Principal allows and identifies


istos = Istos(authorizer=authenticate)


# --- A per-handler policy that layers ON TOP of the app-wide one. It reads a
#     `role` selector param off the request (attribute-based authorization). ---
def admins_only(ctx: AuthContext) -> bool:
    print(f"    [authz] admins_only checking role={ctx.params.get('role')!r} on {ctx.key_expr}")
    return ctx.params.get("role") == "admin"


@istos.handle("fleet/status")
async def status() -> dict:
    # Reached ONLY when the app-wide authorizer allowed the request.
    print("  [handler] fleet/status body ran")
    return {"ok": True, "fleet": "nominal"}


@istos.handle("fleet/shutdown", authorizer=admins_only)
async def shutdown(role: str = "user") -> dict:
    # Reached ONLY when BOTH layers passed: valid app-token AND role == admin.
    print(f"  [handler] fleet/shutdown body ran (role={role})")
    return {"stopping": True}


@istos.handle("fleet/whoami")
async def whoami(user: Principal = Depends(current_principal)) -> dict:
    # The identity the app-wide authorizer resolved is injected here — no need to
    # re-read or re-decode the token. The gate decides; DI materializes.
    print(f"  [handler] fleet/whoami body ran (user={user.id})")
    return {"id": user.id, "roles": sorted(user.roles)}


@istos.handle("health/ping", authorizer=Public)
async def ping() -> dict:
    # Public: the app-wide gate is bypassed for this one handler.
    print("  [handler] health/ping body ran (no token required)")
    return {"pong": True}


if __name__ == "__main__":
    print("authz_server: serving fleet/status, fleet/shutdown, fleet/whoami, health/ping ...")
    try:
        istos.run()
    except KeyboardInterrupt:
        print("Shutting down...")
