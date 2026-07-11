# DSN Reference

A vmon DSN selects one or more `vmon serve` gateways and connection policy. Parsing constructs a client only; it does not contact a gateway. The SDKs use generated `vmon.v1` gRPC for resource operations. HTTP(S) URLs in a DSN identify the same gateway for the limited HTTP infrastructure surfaces, not a JSON resource API. See [API Surface](api.md).

## Grammar

```text
DSN       = network-dsn [ "?" options ]
          / unix-dsn    [ "?" options ]
          / context-dsn [ "?" options ]
network-dsn = ( "vmon://" / "vmons://" ) host-list [ path ]
            / ( "http://" / "https://" ) authority [ path ]
host-list = authority *( "," authority )
unix-dsn  = "vmon+unix:///" absolute-socket-path
context-dsn = "vmon+context://" context-name
options   = option *( "&" option )
option    = "token=" value / "discover=" ( "on" / "off" ) / "timeout=" positive-seconds
```

`authority` is a host name, IPv4 address, or bracketed IPv6 literal, optionally followed by a port. A comma separates hosts only outside IPv6 brackets. `path` is an optional gateway prefix. It is retained and trailing slashes are removed when endpoints are normalized. URL userinfo (`user@host`), an empty host, an empty list member, and fragments are invalid in Python and Go; do not use them in a portable DSN.

Only `token`, `discover`, and `timeout` are defined. Unknown parameters are invalid. Python and Go also reject repeated parameters, so write each option once for cross-SDK portability. `discover` defaults to `on`; `timeout` defaults to `60` seconds and must be finite and greater than zero.

## Schemes and examples

| Form | Endpoint result | Availability |
| --- | --- | --- |
| `vmon://edge-a,edge-b:9000/team-a` | Plaintext `http://edge-a:8000/team-a`, then `http://edge-b:9000/team-a` | Python, Go, TypeScript |
| `vmons://api.example.test` | TLS `https://api.example.test:8000` | Python, Go, TypeScript |
| `vmon://[2001:db8::10]:9443,[2001:db8::11]` | Plaintext IPv6 endpoints; the second gets port `8000` | Python, Go, TypeScript |
| `https://api.example.test/gateway` | One explicit TLS endpoint; no default port is added | Python, Go, TypeScript |
| `http://127.0.0.1:8000` | One explicit plaintext endpoint | Python, Go, TypeScript |
| `vmon+unix:///Users/me/.vmon/vmond.sock` | Local gRPC and HTTP-over-UDS endpoint | Python, Go only |
| `vmon+context://production` | Ordered saved endpoints and, if needed, saved context credential | Python, Go, TypeScript |

`vmon` adds port `8000` to every host without an explicit port; `vmons` does the same while selecting HTTPS/WSS. `http` and `https` accept exactly one endpoint and preserve their explicit-or-implicit URL port. Context entries themselves must be HTTP(S) URLs. A UDS path must be absolute and `vmon+unix` must not contain a host.

Examples:

```text
# Seed two gateways, enable discovery, and apply a 15-second request deadline.
vmons://gw-a.example.test,gw-b.example.test?discover=on&timeout=15

# Use a path-prefixed plaintext gateway. Percent-encode query values as a URL.
vmon://127.0.0.1:8000/tenant-a?token=example-token&discover=off

# Resolve a saved roster instead of embedding hosts.
vmon+context://production?timeout=30

# Local Python or Go daemon.
vmon+unix:///home/alice/.vmon/vmond.sock
```

The `token` option is accepted, but an environment variable or a saved context credential is safer: query strings commonly reach process listings, browser history, proxy logs, and telemetry. Use `vmons`/`https` for remote credentials; plaintext `vmon`/`http` provides no transport confidentiality.

## Defaults, contexts, and precedence

When the caller supplies no DSN (an empty string is treated as absent), each implementation resolves it in this order:

| SDK | Resolution order | Default endpoint |
| --- | --- | --- |
| Python | explicit DSN → `VMON_DSN` → `VMON_CONTEXT` → local socket | `vmon+unix://$VMON_HOME/vmond.sock` |
| Go | explicit DSN → `VMON_DSN` → `VMON_CONTEXT` → local socket | `vmon+unix://$VMON_HOME/vmond.sock` |
| TypeScript | explicit DSN → `VMON_DSN` → `VMON_CONTEXT` → browser origin → loopback HTTP | Browser: `location.origin` only when it is HTTP(S); otherwise `http://127.0.0.1:8000` |

Thus TypeScript has no UDS fallback and browser builds are deliberately network-only. The TypeScript parser supports only the network and context forms; Python and Go additionally support `vmon+unix`.

A context resolves its ordered saved HTTP(S) endpoints. For Python and Go, token selection for a context is DSN `token`, then `VMON_API_TOKEN`, then the saved context token. For every other Python/Go DSN it is DSN `token`, then `VMON_API_TOKEN`. In Python, an explicit `connect(token=...)` takes precedence over every DSN, environment, or saved-context token. `connect(timeout=60)` and `connect(discover=True)` are default sentinels: they preserve `timeout` and `discover` values in the DSN query. Pass a non-default `timeout` or `discover=False` to override those DSN values. TypeScript selects an explicit parser/client token override first, then DSN `token`, then `VMON_API_TOKEN`, then the saved context token. The TypeScript client applies explicit timeout or discovery options over parsed DSN values; use the SDK's client API for language-specific option syntax.

`VMON_CONTEXT` chooses a saved context only when no explicit DSN is supplied. It does not change the endpoint of an explicit network or UDS DSN. Context naming, storage, and credential permissions are described in [Connection Strings and Contexts](../sdk/connection-strings.md) and [Security](../platform/security.md).

## Endpoint roster behavior

The endpoint list is ordered. A client starts with its seed endpoint; with `discover=on`, it lazily learns advertised peers from mesh status after a successful request. It can fail over only for a transport-level failure that did not reach a daemon. gRPC status failures and HTTP responses are never replayed to another endpoint. Set `discover=off` to keep the roster to the DSN/context seeds.

This is a connection policy, not a distributed transaction or request-retry guarantee. Make mutating operations safe for the service semantics rather than relying on endpoint failover. For mesh behavior, see [Mesh and High Availability](../platform/mesh.md).
