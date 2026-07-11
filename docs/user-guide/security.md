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
    mode="client",
    connect_endpoints=["tls/zenoh-router.local:7447"],
    username=secrets["zenoh_user"],
    password=secrets["zenoh_pass"],
    root_ca_certificate=secrets["raw_ca_pem_string"],  # raw PEM OK
)

istos = Istos(config=config)
```

!!! tip "Raw PEM support"
    Zenoh accepts **raw multiline PEM strings**, so you never need to write certificates to disk.

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

istos = Istos(authorizer=TokenAuthorizer("super-secret-token"))

@istos.handle("fleet/status")
async def status():
    return {"ok": True}

# callers attach the token
await istos.query_once("fleet/status", attachment="super-secret-token")
```

Authentication and authorization are **independent, complementary layers** — configure
both before deploying.

## Serialization note

Istos does **not** ship a pickle serializer. `pickle.loads` executes arbitrary code — on a fabric where any peer can publish, that is remote code execution by design. Use JSON, MsgPack, or other safe serializers from `istos.messages.serialization`.

## Security checklist

- [ ] Enable TLS on Zenoh router connections
- [ ] Configure authentication (username/password or mTLS)
- [ ] Set an app-wide `authorizer` (or per-handler rules)
- [ ] Disable multicast scouting in production when using explicit endpoints
- [ ] Use secret managers for certificates (prefer raw PEM in memory)
- [ ] Escalate `IstosSecurityWarning` to errors in CI
- [ ] Restrict network policies to the Zenoh router port

## Next Steps

- [Deployment](deployment.md) — production config, Docker, Kubernetes
- [Observability](observability.md) — health, metrics, tracing
- [Recipe: Secure RPC](../recipes/secure-rpc.md)
