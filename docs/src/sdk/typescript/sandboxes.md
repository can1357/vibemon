# Sandboxes

A TypeScript `Sandbox` is a handle to a guest managed by a remote `vmon serve`
daemon or mesh. It is not a local VM runtime. Create a client as described in
[Connect](connect.md), then create, retrieve, or reference the remote guest.
For DSN selection and the distinction between daemon, transport, and protocol
errors, use [Connection Strings and Contexts](../connection-strings.md) and
[Shared Concepts](../shared-concepts.md).

```ts
import { connect } from "@vmon/sdk";

const client = connect("vmon://127.0.0.1:7777");
const sandbox = await client.sandboxes.create({
  image: "alpine",
  command: ["sh", "-lc", "sleep 600"],
  cpus: 1,
  memory: 512,
  ports: [8080],
  tags: { service: "preview" },
});

console.log(sandbox.id, sandbox.node, sandbox.info);
```

`Sandbox.info` is the latest view held by this handle and is read-only from the
caller's perspective. `id` is stable, `node` is the reporting node or `null`,
and `endpoint` is the preferred daemon endpoint when one is known. Call
`refresh()` to fetch a current view and merge it into the handle.

## Create, get, ref, and list

`client.sandboxes.create(request)` sends a `SandboxCreateRequestWithSecrets`
and returns a handle already associated with the daemon endpoint that created
it. The request accepts the sandbox fields such as `image`, `command`,
`env`, `workdir`, resource values, `ports`, `volumes`, tags, and the creation
network fields (`block_network`, `egress_allow`, and `egress_allow_domains`).
The daemon defines which image and resource combinations it accepts.

Use **`get(id)`** when the current remote sandbox view is needed immediately.
It performs a remote lookup, fails if the sandbox cannot be fetched, and
returns a handle pinned to the endpoint that answered.

```ts
const current = await client.sandboxes.get(sandbox.id);
console.log(current.info.state);
```

Use **`ref(id)`** when an ID is already known and no initial read is needed.
It constructs an unfetched handle with only that ID; its first operation
performs the necessary remote work. It does not check that the ID exists.

```ts
const knownSandbox = client.sandboxes.ref("sbx_123");
await knownSandbox.stop();
```

`list()` returns handles from the live mesh endpoints and merges entries with
the same ID. Optionally restrict the result by exact tag pairs or reporting
node:

```ts
const previews = await client.sandboxes.list({
  tags: { service: "preview" },
  node: "node-a",
});
```

See [Shared Concepts](../shared-concepts.md#resource-handles-and-namespaces)
for endpoint affinity and what an ID-only reference means in a mesh.

## Lifecycle, placement, and snapshots

The methods below operate on the remote sandbox. Operations returning a
`SandboxInfo` update this handle's cached view with the returned fields.

| Method | Effect | Return |
| --- | --- | --- |
| `refresh()` | Fetch the current sandbox view. | `Promise<SandboxInfo>` |
| `stop(wait = true)` | Request graceful shutdown; with the default, poll until `stopped` or `exited`. | `Promise<SandboxInfo>` |
| `terminate(wait = true)` | Force termination; with the default, poll until `terminated`, `stopped`, or `exited`. | `Promise<void>` |
| `remove()` | Remove the sandbox record. | `Promise<void>` |
| `pause()` / `resume()` | Pause or resume execution. | `Promise<SandboxInfo>` |
| `extend(secs)` | Extend the lease by seconds. | `Promise<SandboxInfo>` |
| `migrate(target)` | Migrate to a target and re-resolve the serving endpoint. | `Promise<SandboxInfo>` |
| `snapshot(name?, stop = false)` | Make a full VM snapshot and return its name. | `Promise<string>` |
| `snapshotFilesystem(name?)` | Make a filesystem snapshot and return its image name. | `Promise<string>` |
| `metrics()` | Fetch runtime metrics. | `Promise<SandboxMetrics>` |

Use the snapshot name with the client snapshot APIs documented in
[Snapshots](snapshots.md). A successful lifecycle request is not a substitute
for inspecting the returned view when an application needs a particular state.
The implicit state wait in `stop` and `terminate` is bounded at five minutes;
pass `false` to return after the request instead.

## Readiness

`waitReady()` is opt-in. With no `probe`, it immediately returns the cached
`SandboxInfo`; it does not infer readiness from `state` or wait for boot.
Supply a port number, `{ port }`, or a shell command string:

```ts
await sandbox.waitReady({ probe: 8080, timeout: 30, interval: 250 });
await sandbox.waitReady({ probe: { port: 8080 }, timeout: 30 });
await sandbox.waitReady({ probe: "test -f /tmp/ready", timeout: 30 });
```

`timeout` is in seconds and defaults to `300`; `interval` is in milliseconds
and defaults to `100`. A port probe makes `GET` requests through the sandbox
port proxy and succeeds only for an HTTP `ok` response. A command probe runs
`sh -lc <command>` with an execution timeout of five seconds and succeeds only
with exit code zero. The method retries probe failures until its deadline, then
throws an `Error` whose cause is the latest probe failure.

## Network policy and tunnels

`network()` retrieves the current `NetworkPolicy`; `setNetwork(policy)` applies
a presence-aware update and returns the policy reported by the daemon. An
omitted or `null` `block_network`, `cidr_allow`, or `domain_allow` remains
unchanged; an explicitly supplied list replaces that allow-list, so `[]`
clears it. The policy shape is `block_network`, `cidr_allow`, and
`domain_allow`:

```ts
await sandbox.setNetwork({
  block_network: false,
  cidr_allow: ["10.0.0.0/8"],
  domain_allow: ["packages.example.test"],
});

const policy = await sandbox.network();
```

These are guest network policy fields, not a promise that the SDK itself
performs filtering or that a port becomes publicly reachable. Creation also
has its own network fields; use the daemon's configured policy and deployment
controls for the effective boundary. See [Networking](../../platform/networking.md).

`tunnels()` returns the daemon's `TunnelSet`. When its response includes a
`connect_token`, the SDK caches that token for the HTTP and WebSocket port
proxies. Most applications should call `sandbox.ports.http()` or
`sandbox.ports.websocket()` directly; those methods obtain the token when
needed. Their behavior is covered in [Files and Ports](files-and-ports.md).
