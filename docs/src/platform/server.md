# Server operation

`vmon serve` runs the Rust server implemented by the `vmond` crate. It owns the sandbox registry and starts a `vmon vmm` child for each microVM. It is the normal operator entry point; use [`vmon vmm`](low-level-vmm.md) only when you deliberately need to operate one monitor directly.

## Start a local server

Install or build `vmon`, then start the server on the loopback interface:

```sh
vmon serve --host 127.0.0.1 --port 8000 --token "$(openssl rand -hex 32)"
```

The built-in bind address is `127.0.0.1:8000`. The server state home defaults to `$HOME/.vmon`. The process needs the host privileges and virtualization prerequisites appropriate for the microVMs it will launch; use [`vmon doctor`](cli.md#mesh-server-and-diagnostics) to inspect local prerequisites.

To make the gateway reachable from another host, choose an address exposed by that host and supply a bearer token:

```sh
vmon serve --host 0.0.0.0 --port 8000 --token "$VMON_API_TOKEN"
```

Do not expose an unauthenticated listener. Token, TLS, and scoped-client-token policy are covered in [Security](security.md). A TLS listener requires both paths:

```sh
vmon serve --host 0.0.0.0 --tls-cert /etc/vmon/tls.crt --tls-key /etc/vmon/tls.key
```

Use the matching `https://` server URL when creating remote contexts or advertising a mesh node. Configuration can be supplied from a TOML file, environment, or flags; see [Configuration](configuration.md).

## Protocol boundary

The authoritative resource API is the `vmon.v1` gRPC API defined in `proto/vmon/v1/api.proto`. Native clients use gRPC over the server transport (including cleartext HTTP/2 where configured). Browser and TypeScript clients use the `/grpc` WebSocket bridge for the same protobuf services.

HTTP is intentionally narrower:

- `GET /healthz` returns `{"ok":true}`.
- `GET /metrics` emits Prometheus text metrics.
- `/v1/sandboxes/{id}/ports/{port}` and its path and WebSocket variants proxy a sandbox port.
- The embedded web panel is served as the fallback static application.

There is no OpenAPI documentation endpoint at `/docs`. Do not treat ad-hoc HTTP paths as the control-plane contract; use the gRPC API or the regular [`vmon` CLI](cli.md).

## Authentication at the boundary

The server middleware protects the routes. The normal admin credential is the configured `token`; clients send it as a bearer token. A configured `client_token` is a restricted credential for ordinary sandbox work, while administration remains an admin-token operation. The WebSocket bridge accepts a bearer header or a `token` query parameter during upgrade; local Unix-domain-socket peer credentials bypass that bridge authentication path. Follow [Security](security.md) for credential handling and exposure policy.

## Operational endpoints

Use `GET /healthz` for a simple liveness check and scrape `GET /metrics` with a Prometheus-compatible collector. `vmon metrics` retrieves platform metrics through the normal CLI transport. The server also exposes the local Unix-domain-socket endpoint used by the CLI and supported SDK transports; selecting the `local` context directs the Rust CLI there.

For cluster setup, warm pools, storage, and snapshot operations, use the corresponding CLI control-plane commands and their gRPC services rather than sending lifecycle commands to a VMM socket. See [Mesh and High Availability](mesh.md), [Storage and Volumes](storage.md), and [Snapshots, Restore, and Fork](snapshots.md).

## Production cluster storage

`VMON_CLUSTER_MODE=production` makes PostgreSQL the authority for sandbox
ownership epochs, owner leases, lifecycle markers, rollback/migration claims,
and deletion tombstones. S3 is the authoritative byte store for encrypted
portable recovery history, replicas, and function artifacts. Gossip remains
the liveness and capacity plane; it cannot override a PostgreSQL owner.

Production startup requires `VMON_POSTGRES_URL`, `VMON_S3_ENDPOINT`,
`VMON_S3_BUCKET`, `VMON_S3_REGION`, `VMON_S3_ACCESS_KEY`,
`VMON_S3_SECRET_KEY`, and a non-`default`
`VMON_PORTABLE_HISTORY_KEY_ID`. `VMON_S3_PREFIX` is optional. The selected
32-byte hex key file must exist with identical material on every node at
`$VMON_HOME/security/keys/<id>.key`. Missing requirements are startup errors, not
degraded local operation.

The server renews every serving owner's PostgreSQL lease and arms a local
watchdog from the database-reported remaining TTL. A node that cannot renew in
time stops the VM and removes local serving authority before a successor can
activate. Suspend, rollback, migration, and deletion publish bytes first and
commit their authoritative marker transactionally; failed publication or
commit leaves the previous owner/state in force. See
[Configuration](configuration.md) and
[Mesh and High Availability](mesh.md) for complete setup and failure
semantics.

Dockerfile builds also require `buildctl` on `PATH` and `VMON_BUILDKIT_ADDR` pointing to an isolated BuildKit daemon. There is no fallback to Docker, Buildah, or a host shell.
