# Go client and services

`*vmon.Client` is the root Go SDK object. Obtain it with [`vmon.Connect`](connect.md), keep it for the lifetime of the caller that owns the connection, and close it once:

```go
client, err := vmon.Connect(dsn)
if err != nil {
    return err
}
defer client.Close()
```

The client is a remote API client for a running daemon. Its methods and namespaces send requests to that daemon; they do not provide a local VMM or server runtime.

## Root methods

The root methods are useful for probing and observing the daemon:

| Method | Result | Purpose |
| --- | --- | --- |
| `Health(ctx)` | `vmon.Health` | Reads the daemon health response. `Health.OK` is the required health flag. |
| `Info(ctx)` | `vmon.ServerInfo` | Reads daemon version, platform, architecture, backend, and capabilities. |
| `Metrics(ctx)` | `string` | Reads Prometheus metrics text. |
| `OpenAPI(ctx)` | `json.RawMessage` | Reads the daemon OpenAPI document after validating that it is JSON. |
| `Events(ctx)` | `*vmon.EventStream` | Opens a lifecycle-event stream. |
| `Driver()` | `vmon.Driver` | Returns the transport backing the client. |
| `Close()` | `error` | Releases cached gRPC connections and closes the backing driver. |

The root network calls listed above take `context.Context` and return an error. Local accessor and lifetime methods do not: `Driver` returns the stored transport, while `Close` releases client resources. Check a remote-call error before consuming the returned value:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

info, err := client.Info(ctx)
if err != nil {
    return err
}
if !info.Capabilities["snapshots"] {
    return errors.New("connected daemon does not advertise snapshots")
}
log.Printf("vmon %s using %s", info.Version, info.Backend)
```

This example uses the standard-library `context`, `errors`, `log`, and `time` packages. A missing key in `Capabilities` reads as `false`; capability names are daemon-defined.

## Service namespaces

The client exposes service objects as fields. They are initialized with the client and should be used through that client rather than constructed directly.

| Field | Service | Operations |
| --- | --- | --- |
| `client.Sandboxes` | `*vmon.SandboxService` | Create, get, list, and manage sandbox resources. |
| `client.Snapshots` | `*vmon.SnapshotService` | List snapshot names, restore a snapshot, and fork one or more sandbox clones. |
| `client.Volumes` | `*vmon.VolumeService` | List, create, and delete persistent volumes. |
| `client.Pools` | `*vmon.PoolService` | List, configure, and clear warm sandbox resource pool allocations. |
| `client.Mesh` | `*vmon.MeshService` | Read mesh status and known nodes. |

Use the resource-specific guides for complete request and response examples: [Sandboxes](sandboxes.md), [Processes](processes.md), [Files and Ports](files-and-ports.md), [Volumes and Secrets](volumes-and-secrets.md), [Snapshots and Pools](snapshots.md), and [Durable Functions](functions.md).

For example, the mesh namespace returns typed topology data:

```go
status, err := client.Mesh.Status(ctx)
if err != nil {
    return err
}

log.Printf(
    "node %s has %d peers; quorum is %d",
    status.Self.NodeID,
    len(status.Peers),
    status.Quorum,
)

nodes, err := client.Mesh.Nodes(ctx)
if err != nil {
    return err
}
for _, node := range nodes {
    log.Printf("node %s advertises %s", node.NodeID, node.Advertise)
}
```

`Mesh.Nodes` is a convenience view of `Mesh.Status`: it returns the local node followed by its peer nodes.

## Health, metrics, and events

Use `Health` for a small health call and `Info` for typed daemon identity. `Metrics` returns the daemon's Prometheus exposition as text, so preserve it or pass it to your metrics integration rather than expecting a parsed Go model:

```go
metrics, err := client.Metrics(ctx)
if err != nil {
    return err
}
if err := os.WriteFile("vmond.prom", []byte(metrics), 0o600); err != nil {
    return err
}
```

`Events` opens an event stream. Read one event at a time with `Next`, and close the stream when done:

```go
stream, err := client.Events(ctx)
if err != nil {
    return err
}
defer stream.Close()

for {
    event, err := stream.Next(ctx)
    if err != nil {
        return err
    }

    rawID, ok := event.Value("id")
    if ok {
        log.Printf("event id: %s", rawID)
    }
}
```

An event is a lossless object represented by `vmon.Event`; `Value(name)` returns a `json.RawMessage` for a field. The SDK does not impose a typed event schema, so decode only fields your application understands. Use a cancellable context to stop `Next`; call `EventStream.Close` explicitly when ending consumption. See [Shared Concepts](../shared-concepts.md) for API-wide resource and error conventions.

## Advanced transport injection: `NewClient`

`vmon.NewClient(driver, options...)` binds the public object model to an existing `vmon.Driver`:

```go
func newServiceClient(driver vmon.Driver, token string) *vmon.Client {
    return vmon.NewClient(driver, vmon.WithToken(token))
}
```

This is an integration seam for applications that own a `Driver`; it does not parse a DSN or construct the default mesh driver. The driver implements request execution, WebSocket dialing, endpoint reporting and refresh, and close behavior. `Client.Close` will call the injected driver's `Close`, so do not share an injected driver across clients unless its lifecycle supports that.

`NewClient` accepts the same `Option` values as `Connect`, but applies only client settings to an injected driver. `WithToken` configures client gRPC bearer authentication and `WithMaxResponseBytes` configures the client's buffered-response limit. It does not construct or reconfigure the injected transport, so `WithHTTPClient`, `WithDiscovery`, `WithTimeout`, and `WithUserAgent` have no effect. `WithUserAgent` is meaningful when passed to `Connect`, which constructs the mesh driver. Prefer `Connect` unless you intentionally provide that transport boundary.

## Mesh discovery and failover

For a client made with `Connect`, the default mesh driver starts from the DSN's seed endpoints. Discovery is enabled by default in the DSN and can be changed with its `discover` parameter or `WithDiscovery`. It is deliberately lazy: after a successful request, the driver may refresh advertised mesh peers; refreshes are throttled, and discovery failures do not turn a successful API operation into an error.

Endpoint selection favors an operation's endpoint hint, then the preferred endpoint, then the remaining roster. The driver fails over only when it sees a transport-level connection failure. A daemon API error or HTTP status response is returned as that operation's error and does not cause the SDK to retry the call on another node. Failed endpoints are temporarily cooled down before they are retried. This behavior is connection resilience, not an execution or retry guarantee.

Inspect the current driver roster when your integration needs operational visibility:

```go
for _, endpoint := range client.Driver().Endpoints() {
    log.Printf("endpoint=%s healthy=%t source=%s", endpoint.URL, endpoint.Healthy, endpoint.Source)
}
```

`EndpointInfo.Source` identifies whether an entry is a configured seed or a discovered peer. For server-side topology and high-availability operation, see [Mesh and High Availability](../../platform/mesh.md). For DSN discovery controls, return to [Connect](connect.md).

## Error handling

Daemon failures can be classified with `errors.As` without relying on error strings:

```go
health, err := client.Health(ctx)
if err != nil {
    var apiErr *vmon.APIError
    if errors.As(err, &apiErr) {
        log.Printf("daemon error: code=%s status=%d", apiErr.Code, apiErr.StatusCode)
    }
    return err
}
_ = health
```

See [Errors](errors.md) for the Go SDK error types and [Shared Concepts](../shared-concepts.md) for the shared API model.
