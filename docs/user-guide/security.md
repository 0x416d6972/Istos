# Security & TLS

Istos is **unauthenticated by default**. With no configuration, a session runs in Zenoh **peer mode** with multicast scouting and no TLS — any peer on the local network can discover your node, invoke every `@handle`, and read every published value.

Before deploying, do **both**:

1. **Secure the transport** — authenticate and encrypt the fabric (this page).
2. **Authorize handlers** — gate who may invoke them (below).

Istos raises an `IstosSecurityWarning` whenever it opens an insecure session. Escalate it in CI:

```python
import warnings
from istos import IstosSecurityWarning

warnings.simplefilter("error", IstosSecurityWarning)  # insecure config → exception
```

## Transport security

`IstosZenohConfig` auto-loads credentials from environment variables using `pydantic-settings`.

### Environment-based configuration

Create a `.env` file in your project root:

```env
# Connection mode
ISTOS_ZENOH_MODE=client
ISTOS_ZENOH_CONNECT_ENDPOINTS=["tls/zenoh-router.local:7447"]

# Basic Authentication
ISTOS_ZENOH_USERNAME=robot_1
ISTOS_ZENOH_PASSWORD=super_secret

# TLS
ISTOS_ZENOH_ROOT_CA_CERTIFICATE=/path/to/ca.pem

# mTLS (optional)
# ISTOS_ZENOH_LISTEN_CERTIFICATE=/path/to/cert.pem
# ISTOS_ZENOH_LISTEN_PRIVATE_KEY=/path/to/key.pem
# ISTOS_ZENOH_ENABLE_MTLS=true

# Lock down discovery
# ISTOS_ZENOH_MULTICAST_SCOUTING=false
```

Then pass the config to Istos:

```python
from istos import Istos
from istos.communication.sessions import IstosZenohConfig

config = IstosZenohConfig()  # loads from .env / environment
istos = Istos(config=config)
```

### Programmatic configuration

For secret managers (HashiCorp Vault, AWS Secrets Manager, etc.):

```python
from istos import Istos
from istos.communication.sessions import IstosZenohConfig

secrets = vault.get_secret("istos/prod")

config = IstosZenohConfig(
    mode="peer",
    connect_endpoints=["tls/svc-a.internal:7447"],
    multicast_scouting=False,
    username=secrets["zenoh_user"],
    password=secrets["zenoh_pass"],
    root_ca_certificate=secrets["raw_ca_pem_string"],  # raw PEM OK
)

istos = Istos(config=config)
```

!!! tip "Raw PEM support"
    Zenoh accepts **raw multiline PEM strings**, so you never need to write certificates to disk.

A production config is just the plain constructor with the security fields set.
You do **not** need to run a Zenoh router — staying brokerless is the point.
Lock down a peer directly: keep `mode="peer"`, turn multicast discovery off, dial
your peers explicitly, and put mTLS on the links so only nodes holding a
CA-signed cert can join:

```python
config = IstosZenohConfig(
    mode="peer",
    connect_endpoints=["tls/svc-a.internal:7447", "tls/svc-b.internal:7447"],
    multicast_scouting=False,          # no LAN auto-discovery
    root_ca_certificate=secrets["raw_ca_pem_string"],
    listen_certificate=secrets["node_cert_pem"],
    listen_private_key=secrets["node_key_pem"],
    enable_mtls=True,                  # every peer must present a signed cert
)
istos = Istos(config=config, authorizer=jwt, require_auth=True)
```

With mTLS, membership *is* the cert: an unauthorized node can't complete the
handshake, so no router is needed to police who joins. Layer username/password on
top (`username`/`password`) when you want per-identity credentials — note that in
peer mode the accepting side also needs Zenoh's credential dictionary
(`transport.auth.usrpwd.dictionary_file`, via `additional_config`) to *reject*
unauthenticated peers; mTLS does that on its own.

A `client`-mode config against a shared router is also supported (set
`mode="client"` and point `connect_endpoints` at the router) — reach for it only
when you actually want centralized routing or cross-subnet scale, not as a
security requirement.

Any config with neither auth nor TLS raises an `IstosSecurityWarning` at
construction, so an insecure deployment is loud.

### Why edge auth isn't enough

Authenticating callers at an HTTP gateway (or FastAPI co-host) secures
north-south traffic — who reaches your routes. It does nothing for east-west
traffic on the fabric: an *unsecured* node (the zero-config default: peer mode
with multicast scouting and no TLS) lets any peer on the same network segment
join the mesh, discover queryables, and invoke handlers directly, never touching
your gateway. Two independent layers close this:

- **Transport** — mTLS (or TLS + auth) so an untrusted node can't join at all,
  plus multicast scouting off. This stays brokerless — it locks down the peer
  links themselves; no router required.
- **Application** — an app-wide `authorizer` (with `require_auth=True`) so even a
  joined peer needs a valid token to invoke anything.

The risk is *unauthenticated* peers, not peer mode itself: a locked-down peer
mesh (mTLS + auth + no multicast) is a perfectly good production posture. The
open default is only fine on a genuinely private overlay you control end to end
(a locked-down namespace, a WireGuard mesh). On any shared network, treat both
layers as mandatory.

### Configuration options

| Environment Variable | Description |
|---------------------|-------------|
| `ISTOS_ZENOH_MODE` | Zenoh mode: `peer` (default) or `client` |
| `ISTOS_ZENOH_CONNECT_ENDPOINTS` | JSON array or comma-separated router endpoints |
| `ISTOS_ZENOH_USERNAME` | Basic auth username |
| `ISTOS_ZENOH_PASSWORD` | Basic auth password |
| `ISTOS_ZENOH_ROOT_CA_CERTIFICATE` | Path or raw PEM for CA certificate |
| `ISTOS_ZENOH_LISTEN_CERTIFICATE` | Path or raw PEM for client certificate (mTLS) |
| `ISTOS_ZENOH_LISTEN_PRIVATE_KEY` | Path or raw PEM for private key (mTLS) |
| `ISTOS_ZENOH_ENABLE_MTLS` | Enable mutual TLS (`true`/`false`) |
| `ISTOS_ZENOH_MULTICAST_SCOUTING` | Enable/disable UDP multicast discovery |

## Authorization

Transport security (this page) controls *who joins the fabric*. **Authorization**
controls *what a joined peer may invoke* — a per-request `authorizer` layered
app-wide and per-handler, with `AuthContext`, tokens, `TokenAuthorizer`, custom
RBAC/ABAC policies, and the `Public` opt-out.

That is a topic of its own — see the dedicated **[Authorization](authorization.md)**
page. The short version:

```python
from istos import Istos, TokenAuthorizer

istos = Istos(
    require_auth=True,
    authorizer=TokenAuthorizer("super-secret-token"),
)

@istos.handle("fleet/status")
async def status():
    return {"ok": True}

await istos.query_once("fleet/status", token="super-secret-token")
```

Transport auth and app authorizers are separate — you usually want both.
JWT / RBAC: [Authorization](authorization.md).

## Serialization note

No pickle serializer. On a fabric where anyone can publish, `pickle.loads` is RCE.
Use JSON, msgpack, or the other serializers in `istos.messages.serialization`.

## Security checklist

- [ ] TLS to the router
- [ ] Zenoh username/password or mTLS
- [ ] `require_auth=True` + authorizer
- [ ] Multicast scouting off when using explicit endpoints
- [ ] Certs from a secret store when you can
- [ ] `IstosSecurityWarning` → error in CI
- [ ] Network policy for Zenoh (+ HTTP if gateway is on)

## Next Steps

- [Deployment](deployment.md) — production config, Docker, Kubernetes
- [Observability](observability.md) — health, metrics, tracing
- [Recipe: Secure RPC](../recipes/secure-rpc.md)
