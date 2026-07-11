# Install the Go SDK

The Go SDK is a Go module. It is a remote client for `vmon serve`; install and run the daemon separately, then configure a DSN for the daemon you intend to control.

## Add the module

From an existing Go module, add the SDK at the module path declared by the project:

```sh
go get github.com/can1357/vibemon/sdk/go
```

Import its package as `vmon`:

```go
import vmon "github.com/can1357/vibemon/sdk/go"
```

The SDK module declares Go 1.25. Check your application toolchain with:

```sh
go version
```

For repository development, use the configured Go toolchain. The SDK brings its gRPC and protobuf dependencies through normal Go module resolution; applications should not import the SDK's internal generated packages.

## Make the daemon reachable

Before calling `vmon.Connect`, point the client at a running daemon. A remote TLS endpoint can be supplied directly:

```go
client, err := vmon.Connect("vmons://vmon.example.internal")
if err != nil {
    return err
}
defer client.Close()
```

For local development, Go can also connect through a Unix-domain socket:

```go
client, err := vmon.Connect("vmon+unix:///absolute/path/to/vmond.sock")
if err != nil {
    return err
}
defer client.Close()
```

A `vmon+unix` DSN requires an absolute socket path. It is a transport to an already-running local daemon, not a way to launch one. For all supported DSN forms, context storage, and environment fallback, see [Connection Strings and Contexts](../connection-strings.md) and the [DSN reference](../../reference/dsn.md).

## Verify from application code

Use an application context and a small daemon call after connecting:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

health, err := client.Health(ctx)
if err != nil {
    return err
}
if !health.OK {
    return errors.New("vmon daemon reported unhealthy")
}
```

This snippet uses the standard-library `context`, `errors`, and `time` packages. `Health` reports the daemon's health response; it does not test whether a future resource operation will succeed.

Continue with [Connect](connect.md) to choose a DSN and configure the client, or see [Client](client.md) for the service namespaces.
