# Go SDK

The Go SDK is a client for a running `vmon serve` daemon. It does not start a VMM or embed a server in your process: construct a client with a connection string, then use its service namespaces to operate the daemon over its API.

```go
package main

import (
    "context"
    "log"

    vmon "github.com/can1357/vibemon/sdk/go"
)

func main() {
    ctx := context.Background()

    client, err := vmon.Connect("vmons://vmon.example.internal")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    info, err := client.Info(ctx)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("connected to vmon %s on %s/%s", info.Version, info.Platform, info.Arch)
}
```

## What the client provides

`*vmon.Client` is the root for the API:

- `Sandboxes` manages sandbox instances and their runtime operations.
- `Snapshots` manages snapshot resources.
- `Volumes` manages named volumes.
- `Pools` manages warm sandbox resource pools.
- `Mesh` reads mesh membership and node status.
- Root methods expose daemon health, build information, Prometheus metrics, the OpenAPI document, and lifecycle events.

See [Client](client.md) for the root client and service map, then use the resource guides for [sandboxes](sandboxes.md), [processes](processes.md), [files and ports](files-and-ports.md), [volumes and secrets](volumes-and-secrets.md), and [snapshots and pools](snapshots.md). Deployed functions are covered in [Durable Functions](functions.md).

## Requirements and module

The module path is:

```text
github.com/can1357/vibemon/sdk/go
```

The module declares Go 1.25. Use the configured Go toolchain for this repository or a compatible Go toolchain in an application. See [Install](install.md).

## Connection choices

`vmon.Connect` accepts a DSN and returns a ready client. It supports a remote HTTP(S) endpoint, a `vmon`/`vmons` mesh roster, a named context, and a local Unix-domain socket. An empty DSN also resolves from the vmon environment and local state directory. The Go SDK supports local Unix sockets; this is still a connection to a daemon, not an in-process runtime.

Start with [Connect](connect.md) for Go setup. The canonical syntax, environment precedence, and context-file rules live in [Connection Strings and Contexts](../connection-strings.md) and the [DSN reference](../../reference/dsn.md).

## Context for remote and blocking operations

Pass a `context.Context` to remote API calls and to blocking reads such as `EventStream.Next`. Local accessors and lifetime methods, such as `Client.Driver`, `Client.Close`, and `EventStream.Close`, do not take a context. Use it to bound a request, propagate cancellation, and control the lifetime of a stream:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

health, err := client.Health(ctx)
if err != nil {
    return err
}
if !health.OK {
    return errors.New("vmon daemon reported unhealthy")
}
```

This example requires the standard-library `errors` and `time` packages. Pass a context supplied by your request handler or worker when one exists; do not replace it with `context.Background()` inside library code.

## Errors and response models

Methods return Go values plus an `error`. The SDK exposes `APIError` for structured daemon failures, `ProtocolError` for communication or malformed-response failures, `TransportError` for unreachable endpoints, and `ResponseTooLargeError` when a buffered response exceeds its configured limit. Check errors with `errors.As`; do not parse their text. Read [Errors](errors.md) for handling patterns and [Shared Concepts](../shared-concepts.md) for the cross-SDK model.
