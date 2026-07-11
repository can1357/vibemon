# Sandboxes

The Go SDK is a client for a running `vmon serve` API. A `*vmon.Sandbox` is a daemon-owned sandbox view, not a local VM or server. Create it through `client.Sandboxes`, then use the returned handle for lifecycle actions and guest services.

For DSN selection, connection ownership, and closing the client, see [Connect](connect.md). For the common context and error model, see [Connection Strings and Contexts](../connection-strings.md) and [Error Codes](../../reference/errors.md).

## Create and inspect

`SandboxCreateRequest` carries the create-time image or template selection and optional resource, environment, network, port, and storage settings. `TimeoutSeconds` is the sandbox idle timeout; it is distinct from `Timeout`, the create request timeout in seconds. `Env` is for non-secret values; use `Secrets` for request-time secret bundles as described in [Volumes and Secrets](volumes-and-secrets.md).

```go
idleTimeout := uint64(900)

ctx := context.Background()

sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image:          "alpine:3.21",
    Name:           "build-agent",
    CPUs:           2,
    MemoryMiB:      1024,
    DiskMiB:        4096,
    TimeoutSeconds: &idleTimeout,
    Workdir:        "/workspace",
    Env:            map[string]string{"CI": "true"},
    Ports:          []uint16{8080},
    Tags:           map[string]string{"purpose": "build"},
})
if err != nil {
    return err
}
fmt.Printf("%s is %s on %s\n", sandbox.ID, sandbox.Status, sandbox.Node)

```

The returned `Sandbox` exposes canonical fields including `ID`, `Name`, `Status`, `Node`, timestamps, tags, `ReturnCode`, and `ErrorMessage`. It also retains unrecognized response fields in `Details` as `json.RawMessage` values. Do not construct a `Sandbox` yourself: use `Create`, `Get`, `List`, or `Ref` so its `Files` and `Ports` services are initialized.

`Ref` is intentionally local: it creates a handle without a server call. Call `Refresh` when current metadata is required. `Refresh` updates the same handle and returns it, so callers holding that pointer see the refreshed state.

```go
sandbox := client.Sandboxes.Ref("sandbox-id")
if _, err := sandbox.Refresh(ctx); err != nil {
    return err
}
if sandbox.Status != "running" && sandbox.Status != "ready" {
    return fmt.Errorf("sandbox %s is %s", sandbox.ID, sandbox.Status)
}
```

All calls take a `context.Context`. Canceling or expiring it ends the in-flight client operation and returns the context error where applicable; use a per-operation deadline when a request must not wait indefinitely.

## Lifecycle actions

Lifecycle methods operate on the identified server sandbox:

| Method | Effect | Return value |
| --- | --- | --- |
| `Stop(ctx)` | Halts execution. Use it for the ordinary, graceful stop operation exposed by the API. | Updated `*Sandbox` |
| `Terminate(ctx)` | Immediately halts the sandbox and releases its resources. | `error` |
| `Remove(ctx)` | Removes the sandbox. | `error` |
| `Pause(ctx)` / `Resume(ctx)` | Suspends active processes / reactivates a paused sandbox. | Updated `*Sandbox` |
| `Extend(ctx, seconds)` | Increases the lease duration by `seconds`. | Updated `*Sandbox` |
| `Migrate(ctx, target)` | Relocates the sandbox to `target`; the handle is rebound to the resolved endpoint. | Updated `*Sandbox` |

Choose `Stop` when execution should be halted through the normal lifecycle action. Choose `Terminate` only when the API's immediate halt and resource release is intended. `Remove` is a separate deletion action; named persistent volumes have their own `client.Volumes` lifecycle and must be deleted there when desired—see [Volumes and Secrets](volumes-and-secrets.md).

```go
if _, err := sandbox.Stop(ctx); err != nil {
    return err
}

// A later operation can use the same handle.
if _, err := sandbox.Extend(ctx, 600); err != nil {
    return err
}

// Use only when an immediate halt is appropriate.
if err := sandbox.Terminate(ctx); err != nil {
    return err
}
```

The updated views returned by `Stop`, `Pause`, `Resume`, `Extend`, and `Migrate` replace the receiver's decoded metadata while preserving its client binding. Still call `Refresh` when another actor may have changed lifecycle state.

## Wait for readiness

`WaitReady` polls `Refresh` until the server reports `running` or `ready`. By default it allows five minutes and polls every 250 ms. Supply `WaitReadyOptions` to change the timeout or interval, and optionally select **one** readiness probe:

- `Port` checks the daemon-reported tunnel target with a TCP dial.
- `Command` runs a captured command and requires exit code zero.

The two probes are mutually exclusive. A sandbox that enters `failed`, `terminated`, or `exited` returns a `not_running` API error; expiry of the readiness timeout returns a `timeout` API error. Cancellation of the caller's context wins over the readiness timeout.

```go
ready, err := sandbox.WaitReady(ctx, vmon.WaitReadyOptions{
    Timeout:  45 * time.Second,
    Interval: 500 * time.Millisecond,
    Command:  []string{"test", "-f", "/workspace/ready"},
})
if err != nil {
    return err
}
fmt.Println(ready.Status)
```

## Snapshots, network, and tunnels

A sandbox can request a full snapshot with `Snapshot(ctx, vmon.SnapshotRequest{...})` or a filesystem-only image with `SnapshotFilesystem(ctx, vmon.FilesystemSnapshotRequest{...})`; each returns its server identifier. Snapshot restoration and pool operations are covered in [Snapshots and Pools](snapshots.md).

Network policy is server state. `Network` reads it and `SetNetwork` updates only the fields represented by non-`nil` pointers. A non-`nil` empty allowlist deliberately replaces that allowlist with an empty list.

```go
blocked := false
cidrs := []string{"203.0.113.0/24"}
state, err := sandbox.SetNetwork(ctx, vmon.NetworkPolicy{
    BlockNetwork: &blocked,
    CIDRAllow:    &cidrs,
})
if err != nil {
    return err
}
fmt.Println(state.EgressAllow)
```

`Tunnels(ctx)` returns the exposed guest-port targets and may update the handle's short-lived connect token. Application code normally does not need that token: use `sandbox.Ports.HTTP` or `sandbox.Ports.WebSocket`, which obtain it as needed. The SDK does not make a sandbox port public by itself; expose it in the create request and configure it on the server. See [Files and Ports](files-and-ports.md).

## Observability

`Metrics(ctx)` returns `SandboxMetrics`, an open set of raw JSON metric values. Use `Float64(name)` only when the requested metric is present and numeric. `Logs(ctx)` captures the currently available combined log chunks into one string; for an incremental follow stream, use `FollowLogs` as shown in [Processes](processes.md).