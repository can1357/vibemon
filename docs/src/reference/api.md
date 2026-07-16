# API Surface

`vmon serve` is the control plane. Its public resource API is **gRPC**, not a REST or raw-TCP API. The Python, Go, and TypeScript SDKs are thin clients for the same `vmon.v1` protobuf services; they do not run a VM, daemon, or VMM locally. Choose an SDK for normal use, and use the protobuf definitions when generating another gRPC client.

## Transports and boundaries

| Consumer | Transport | API surface |
| --- | --- | --- |
| Go and Python SDKs; local CLI | Native gRPC over HTTP/2 (h2c over TCP or Unix-domain socket; TLS when configured) | All `vmon.v1` services |
| Browser and TypeScript SDK | One gRPC call over one WebSocket at `GET /grpc` | The same `vmon.v1` services, through the bridge |
| Operators and probes | Plain HTTP | Health, Prometheus metrics, sandbox port proxying, and the embedded static UI only |

The HTTP routes are infrastructure surfaces, not an alternate JSON resource API. In particular, do not build against old-looking `/v1/...` resource paths: resource operations are gRPC. Mesh-internal template-distribution and administrative HTTP routes are likewise not a public SDK contract.

For a server endpoint and authentication setup, see [Server Operation](../platform/server.md), [Configuration](../platform/configuration.md), and [Security](../platform/security.md). Connection selection is specified in the [DSN Reference](dsn.md).

## Services at a glance

The table is a navigation map, not a replacement for `proto/vmon/v1/api.proto`. RPC names below are fully typed protobuf calls unless noted as a JSON view.

| Service | Primary operations | Streaming / notable contract |
| --- | --- | --- |
| `SandboxService` | Create, list, get, stop, remove, terminate, pause, resume, suspend, extend, metrics; network, tunnels, migration, snapshots, recovery history and rollback; guest files | `Logs` is server-streaming; `Exec` and `Shell` are bidirectional; `Attach` is server-streaming. `Exec` must begin with `ExecStart`; `Shell` begins with shell parameters and emits a resolved sandbox ID in `Ready`. Sandbox views and several engine-owned documents are JSON carried in `JsonView` strings. |
| `SnapshotService` | List, restore, fork, and permanently delete saved snapshots | Restore and fork return a sandbox JSON view. Delete removes the encrypted snapshot storage. |
| `CredentialService` | List non-secret credential metadata; create or atomically rotate a credential; revoke a credential | Credential values are accepted only in `Put` headers and are never returned. Tenant callers are confined to their own namespace; optional tenant selectors are administrator-only. |
| `VolumeService` | List, create, and delete named persistent volumes | Create/delete return an empty `Ok`; volumes remain tenant-owned and delete is refused while attached. |
| `PoolService` | List, set, and delete warm pools | Pool documents are engine-owned JSON views. |
| `SystemService` | Engine info, events, and mesh status | `Events` is server-streaming. `MeshStatus` supplies the roster used for optional SDK discovery. |
| `ArtifactService` | Put, get, and stat immutable content-addressed artifacts | `Put` is client-streaming and verifies the digest; `Get` is server-streaming. |
| `FunctionService` | Register/get/list/activate/delete immutable function revisions; activate/get/rollback apps; create/get/list/delete schedules | Functions and applications resolve pinned or active revisions. |
| `CallService` | Create, feed, close, inspect, list, cancel durable calls; fetch results | `StreamInputs` is bidirectional; `Watch` is server-streaming from a durable sequence cursor. |
| `ActorService` | Create/get/checkpoint/restore/fork/delete durable actors | Actors are pinned to function revisions; checkpoints are immutable artifacts. |

The resource guides explain lifecycle semantics: [Sandboxes](../sdk/shared-concepts.md), [Snapshots](../platform/snapshots.md), [Storage and Volumes](../platform/storage.md), and [Mesh and High Availability](../platform/mesh.md).

## Authentication and errors
Clients authenticate with bearer credentials. An operator token has full control; a configured client token is restricted from mesh administration and sandbox migration. A tenant token maps to one tenant ID and cannot read or mutate another tenant's resources. The selected customer key ID encrypts that tenant's new snapshots, credential records, and persistent volume archives. The server accepts token rotation lists; keep tokens out of DSNs, source control, logs, and shell history. See [Security](../platform/security.md).

Every RPC reports ordinary gRPC status. On failures, the stable daemon error code is carried in response metadata/trailers named `vmon-code`.

| `vmon-code` | gRPC status | Meaning |
| --- | --- | --- |
| `not_found` | `NOT_FOUND` | Referenced resource does not exist. |
| `invalid` | `INVALID_ARGUMENT` | Request or configuration is invalid. |
| `unauthorized` | `UNAUTHENTICATED` | Credential is absent or not accepted. |
| `not_running` | `FAILED_PRECONDITION` | Operation requires an applicable running state. |
| `busy` | `ABORTED` | A required resource is locked or conflicts. |
| `unsupported` | `UNIMPLEMENTED` | Capability is unavailable. |
| `engine` | `UNAVAILABLE` | The engine or hypervisor could not serve the request. |

SDKs expose their own error types around these failures; malformed resource envelopes are protocol errors. Do not retry a daemon status or HTTP response automatically as failover: SDK endpoint failover is limited to transport failures that did not reach a daemon.

## Browser bridge

The WebSocket bridge exists for clients without native HTTP/2, especially browser TypeScript. It is protobuf framing, not JSON-RPC:

1. Open `GET /grpc`; use a bearer header when the environment permits it, otherwise the bridge supports `?token=`. Local UDS peer-credential handling is unchanged.
2. Send a binary `BridgeFrame.call` containing the full gRPC method path (for example, `/vmon.v1.SandboxService/Exec`) and ASCII metadata.
3. Send zero or more binary `BridgeFrame.message` frames. Each payload is exactly one encoded protobuf request message, without gRPC's five-byte wire prefix.
4. Send `BridgeFrame.half_close`. The server emits zero or more message frames, then one `BridgeFrame.end` with numeric gRPC status, message, and ASCII trailers, including `vmon-code` on failure, and closes the socket.

Text WebSocket messages, binary metadata, and multiple RPCs on one bridge connection are protocol errors. The bridge maps frames onto the in-process gRPC services, so service behavior is identical; only framing and transport differ.
