# Snapshots and Pools

The Go SDK is a client for a running `vmon serve` daemon. Snapshot creation, restore placement, clone allocation, and pool replenishment happen at that daemon; this package submits requests and returns the resource views it receives. Connect to the daemon first as described in [Connection Strings and Contexts](../connection-strings.md).

## Restore a snapshot

A sandbox creates a full snapshot with `sandbox.Snapshot(ctx, vmon.SnapshotRequest{...})`; use the returned snapshot name with `client.Snapshots.Restore`. `Restore` returns a new, bound `*vmon.Sandbox` handle. Its `RestoreRequest` supplies typed `Name` and `Agent` fields plus optional create-style `Overrides`. Do not put `"name"` or `"agent"` in `Overrides`: the SDK rejects a conflict with those typed fields.

```go
ctx := context.Background()
client, err := vmon.Connect(serverURL, vmon.WithToken(token))
if err != nil {
    return err
}
defer client.Close()

restored, err := client.Snapshots.Restore(ctx, "base-snapshot", vmon.RestoreRequest{
    Name: "worker-from-base",
})
if err != nil {
    return err
}
fmt.Println(restored.ID, restored.Status)
```

`Snapshots.List(ctx)` returns snapshot names. A restore response is decoded and bound to the daemon endpoint that served it, but application code should treat the returned sandbox metadata as a view, not as a scheduling promise. Fetch it again with `client.Sandboxes.Get` when current placement or status matters.

### S3-backed snapshots

When a sandbox has `SandboxCreateRequest.S3Mounts`, the daemon records its S3
mount configuration with a full snapshot (and with the filesystem image made
from one). The stored metadata includes the URI, endpoint, resolved region,
read-only setting, and credential mode, but **not** inline credential values.
Restoring that snapshot reconstructs the recorded mounts; it is not an S3
mount override in `RestoreRequest.Overrides`.

For a mount originally authenticated with inline credentials or the daemon's
AWS credential environment, the restoring daemon must have
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` available (and may use
`AWS_SESSION_TOKEN`). Anonymous mounts do not need credentials. Treat a
snapshot as a record of remote mount configuration, not S3 data or secrets:
the S3 objects remain remote, and volatile guest-overlay writes are not
persisted to S3.

## Fork clones

`Snapshots.Fork(ctx, name, vmon.ForkRequest{Count: n})` requests an ordered batch of clone views from one snapshot. `Count` is the requested positive clone count; `Overrides` accepts additional create-style fields. The method returns `[]*vmon.Sandbox`, with every returned sandbox bound for later operations.

```go
clones, err := client.Snapshots.Fork(ctx, "base-snapshot", vmon.ForkRequest{Count: 3})
if err != nil {
    return err
}
for _, clone := range clones {
    fmt.Printf("clone %s on %s\n", clone.ID, clone.Node)
}
```

The daemon decides whether the request can be fulfilled and where clones run. A successful request is not a guarantee that all later guest operations succeed; use request-scoped contexts and handle the returned error.

## Warm pools

Pools are named daemon-side allocations of ready sandbox capacity. They are not Go worker pools and the SDK does not run, prewarm, or schedule VMs locally.

Use `client.Pools.Set` to create or change a pool, `List` to inspect all pools, `Delete` to remove one reference, and `Clear` to list then delete every configured reference. `PoolRequest` has a desired `Size` and optional image-pipeline `Template` arguments. `PoolStats` reports the server-observed `Ready`, `Hits`, `Misses`, and configured `Size`.

```go
stats, err := client.Pools.Set(ctx, "alpine:latest", vmon.PoolRequest{Size: 8})
if err != nil {
    return err
}
fmt.Printf("ready=%d hits=%d misses=%d size=%d\n", stats.Ready, stats.Hits, stats.Misses, stats.Size)
```

Use the pool reference and template shape expected by the daemon deployment. `Set` asks for a target size; it does not promise immediate readiness. Read `Pools.List` when an observed capacity value is required, and use the server's own pool and scheduling telemetry to diagnose misses.

## Mesh-aware handles

A client can be configured with discovery through `vmon.WithDiscovery(true)`. The SDK keeps endpoint affinity after a successful RPC and tries other healthy mesh endpoints only for transport failures. Sandbox operations can resolve a moved sandbox by ID when the prior owner reports daemon `not_found`; this is routing recovery, not client-side migration or scheduling. The `teleport` example demonstrates using the same sandbox handle after migration and then re-fetching metadata to confirm the owner.

For endpoint, DSN, and context rules, see [Connection Strings and Contexts](../connection-strings.md). For daemon snapshot and pool semantics, see [Snapshots, Restore, and Fork](../../platform/snapshots.md) and [Mesh and High Availability](../../platform/mesh.md).
