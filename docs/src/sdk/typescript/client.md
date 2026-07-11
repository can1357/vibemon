# Root client and namespaces

A `Client` is the root for daemon-wide operations and typed resource namespaces.
Create one with [`connect`](connect.md), or bind a driver directly when you own
transport construction. Keep one client for the lifetime of related work so
bound resources retain their endpoint affinity.

```ts
import { connect } from "@vmon/sdk";

const client = connect("https://vmon.example");
try {
  const sandbox = await client.sandboxes.create({ image: "alpine" });
  await sandbox.remove();
} finally {
  await client.close();
}
```

## Daemon-wide operations

The root client exposes these methods:

| Method | Result | Purpose |
| --- | --- | --- |
| `health()` | `Promise<Health>` | Reads the daemon health document from `GET /healthz`. |
| `info()` | `Promise<ServerInfo>` | Gets daemon and node information. |
| `metrics()` | `Promise<string>` | Reads Prometheus metrics text from `GET /metrics`. |
| `events()` | `Promise<EventStream>` | Opens the daemon event RPC stream. |
| `shell(request?)` | `Promise<Process>` | Opens an interactive shell and waits for its ready frame. |
| `close()` | `void \| Promise<void>` | Releases the underlying driver. |

For example, consume an event stream until the application chooses to stop it:

```ts
const events = await client.events();
try {
  for await (const event of events) {
    console.log(event);
  }
} finally {
  events.close();
}
```

`EventStream.close()` cancels that stream. `shell()` returns a `Process` with
writable `stdin`, async output iteration, `wait()`, and `close()`; the process
page covers its lifecycle in [Processes](processes.md).

## Resource namespaces

Every namespace is created by the root client and exposed as a readonly
property:

| Property | Use |
| --- | --- |
| `sandboxes` | Create, get, reference, and list sandboxes. |
| `snapshots` | List, restore, and fork snapshots. |
| `volumes` | List, create, and delete volumes. |
| `pools` | Inspect and configure warm pools. |
| `mesh` | Read mesh status and nodes. |
| `functions` | Look up a deployed durable function by name. |
| `apps` | Look up a deployed application by name. |

The resource returned from a lookup or creation remains associated with the
client and the endpoint that served it. Do not instantiate these namespace
classes yourself. Use the appropriate namespace instead:

```ts
const sandbox = await client.sandboxes.get("sandbox-id");
const snapshotNames = await client.snapshots.list();
const deployed = await client.functions.fromName("thumbnail");

console.log(sandbox.id, snapshotNames, deployed);
```

See [Sandboxes](sandboxes.md), [Snapshots](snapshots.md), [Volumes and
Secrets](volumes-and-secrets.md), and [Durable Functions](functions.md) for
operation-specific APIs. The [Mesh](../shared-concepts.md) and connection
pages explain how a `MeshDriver` discovers and selects endpoints.

## `MeshDriver` and lifecycle

`connect` builds `new Client(new MeshDriver(dsn, options))`. `MeshDriver` owns
the endpoint roster, optional mesh discovery, HTTP requests, WebSocket RPC
transport, and endpoint selection. Its public `endpoints()` method returns a
snapshot of each endpoint's URL, current health, and whether it was a seed or
discovered endpoint; `refresh()` requests a discovery refresh when discovery
is enabled.

Call `client.close()` when the client is no longer needed. For a `MeshDriver`,
closing marks the driver closed, and later client operations fail rather than
opening new requests. Close individual `EventStream` or `Process` instances
when ending those streams; closing the client is not a substitute for ending
application-level stream work deliberately.

The client does not promise application-level retries. Consult [Shared
Concepts](../shared-concepts.md) for the driver's limited transport failover
and endpoint-affinity behavior, and [Errors](errors.md) for failures exposed
to callers.
