# SDK Overview

The Python, Go, and TypeScript packages are clients for a running `vmon serve`
daemon. They do not start a daemon, run a VMM, bundle the guest agent, or
implement a local VM runtime. The daemon owns the sandbox registry and starts
the per-sandbox `vmon vmm` processes; SDK calls cross that control-plane
boundary.

This distinction is useful when choosing where code runs:

| Concern | Owner |
| --- | --- |
| Creating, listing, stopping, and inspecting sandboxes | SDK calls to `vmon serve` |
| VM lifecycle, registry, images, pools, mesh, and volumes | `vmon serve` |
| Per-microVM hypervisor process | `vmon vmm`, started by the daemon |
| Commands, files, and interactive processes in a guest | Guest agent, reached through the daemon |

For host, hypervisor, and server setup, see [Server Operation](../platform/server.md)
and [Security](../platform/security.md). Start a daemon before using an SDK.

## One control plane, two transports

The public resource API is `vmon.v1` gRPC, not a JSON-over-HTTP or raw-TCP
API. Native clients use gRPC over HTTP/2 (h2c for plaintext) to TCP endpoints;
the Python and Go clients can also use it over a local Unix-domain socket.

TypeScript is deliberately network-only. It sends the same protobuf RPCs over
the daemon's `/grpc` gRPC-over-WebSocket bridge: an `http` endpoint becomes
`ws`, and an `https` endpoint becomes `wss`. This is what makes the package
usable in a browser; it is not native gRPC and it has no UDS form.

Plain HTTP is intentionally narrow:

| Surface | Purpose |
| --- | --- |
| `/healthz` | Daemon health |
| `/metrics` | Prometheus metrics |
| Sandbox port proxy | Reaching a published guest port |

Those HTTP endpoints do not turn the resource API into an HTTP JSON API.
Sandbox lifecycle, guest files, execution, snapshots, volumes, pools, and
mesh operations use protobuf RPCs. See [Shared Concepts](shared-concepts.md)
for error and routing behavior.

## Common object model

Every client begins with a root client made by its language's connection
factory. The root owns a connection driver and exposes daemon-wide operations
such as health, server information, metrics, events, and close. It also
exposes resource namespaces:

| Namespace | Role |
| --- | --- |
| `sandboxes` | Create, list, get, or reference microVM sandboxes |
| `snapshots` | List, restore, or fork saved VM state |
| `volumes` | Create, list, and remove persistent named volumes |
| `pools` | Inspect and configure warm pools |
| `mesh` | Inspect mesh status and nodes |

A sandbox returned by `create`, `get`, restore, or fork is a resource object
bound to the root client. It provides lifecycle actions and the sandbox-bound
objects used for guest work: processes, files, and ports. These are remote
handles, not local resources or copies of guest state. A reference made from
an ID is usable before it is fetched; operations resolve it through the
daemon.

The merged [Connect](connect.md) guide shows language-specific spelling,
async conventions, and transport limitations. The shared connection forms
and defaults are in [Connection Strings and Contexts](connection-strings.md).

## Typical flow

1. Run and secure `vmon serve`.
2. Construct a client from a DSN or the environment. Construction parses and
   resolves configuration but makes no network request.
3. Use a namespace to create or find a resource.
4. Use the returned sandbox's process, file, or port handle for guest work.
5. Close the client when the language API requires or supports explicit
   cleanup.

The same workflow works for a local daemon, a single remote endpoint, or a
mesh roster. The network address, discovery policy, and credential selection
are client configuration—not sandbox configuration.
