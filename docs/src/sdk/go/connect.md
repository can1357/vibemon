# Connect from Go

Create a `*vmon.Client` with `vmon.Connect`. `Connect` parses the DSN, builds the SDK's mesh driver, and returns a client ready to use. It does not make an eager health check; make a context-bound call such as `Health` when your application needs readiness confirmation.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

client, err := vmon.Connect("vmons://vmon.example.internal")
if err != nil {
    return err
}
defer client.Close()

health, err := client.Health(ctx)
if err != nil {
    return err
}
if !health.OK {
    return errors.New("vmon daemon reported unhealthy")
}
```

The snippet uses the standard-library `context`, `errors`, and `time` packages. Keep the client for the lifetime of the component that uses it; do not connect separately for each request.

## DSNs, contexts, and local sockets

The Go parser recognizes these connection forms:

| Form | Example | Use |
| --- | --- | --- |
| HTTP endpoint | `http://127.0.0.1:8000` | One plaintext daemon endpoint. |
| HTTPS endpoint | `https://vmon.example.internal/api` | One secure daemon endpoint, optionally with a path prefix. |
| Mesh roster | `vmon://node-a,node-b:9000/api` | Multiple plaintext seeds; `vmons://` uses HTTPS. |
| Unix socket | `vmon+unix:///absolute/path/to/vmond.sock` | A local daemon over an absolute Unix-domain socket path. |
| Named context | `vmon+context://prod` | Endpoints loaded from the saved `prod` context. |

Use the shared [Connection Strings and Contexts](../connection-strings.md) guide and [DSN reference](../../reference/dsn.md) for syntax, normalization, query parameters, environment fallbacks, and stored-context layout. `ParseDSN` is available when an application needs to validate or inspect a DSN without making a request:

```go
config, err := vmon.ParseDSN("vmon+context://prod")
if err != nil {
    return err
}
for _, endpoint := range config.Endpoints {
    log.Printf("configured endpoint: %s", endpoint)
}
```

An empty string is meaningful: `Connect("")` delegates to `ParseDSN`, which resolves `VMON_DSN`, then `VMON_CONTEXT`, then the local default Unix socket. Do not silently pass an empty DSN unless that fallback is the desired deployment policy.

## Tokens

Supply a short-lived or deployment-scoped bearer token explicitly when possible:

```go
client, err := vmon.Connect(
    "vmons://vmon.example.internal",
    vmon.WithToken(token),
)
if err != nil {
    return err
}
defer client.Close()
```

For `Connect`, an explicit `WithToken` value wins over the token resolved by the DSN. Without it, token resolution belongs to the DSN parser: a DSN `token` parameter is used first; otherwise `VMON_API_TOKEN` is consulted; for a named context, its saved credential is the final fallback after the environment variable. Keep credentials out of source code and logs. See the shared connection guide for the full rules.

## Options

`Connect` accepts variadic `vmon.Option` functions:

| Option | Effect |
| --- | --- |
| `vmon.WithToken(token)` | Sets the bearer token used by the client. |
| `vmon.WithHTTPClient(httpClient)` | Supplies the HTTP client used by the mesh driver. A nil value is ignored. |
| `vmon.WithUserAgent(value)` | When `Connect` constructs the mesh driver, sets the HTTP `User-Agent` header for its requests. |
| `vmon.WithMaxResponseBytes(limit)` | Limits buffered successful HTTP response bodies; positive values replace the 64 MiB default. |
| `vmon.WithDiscovery(enabled)` | Overrides the DSN's mesh-discovery setting. |
| `vmon.WithTimeout(timeout)` | Overrides the DSN request timeout when it is positive. |

For example, keep an application-wide HTTP client and impose a smaller buffered-response limit:

```go
httpClient := &http.Client{}
client, err := vmon.Connect(
    "vmons://vmon.example.internal?discover=off",
    vmon.WithHTTPClient(httpClient),
    vmon.WithMaxResponseBytes(8<<20),
    vmon.WithTimeout(30*time.Second),
)
if err != nil {
    return err
}
defer client.Close()
```

The `http` and `time` imports in this example are from the standard library. `WithMaxResponseBytes` does not constrain an open event stream; it controls successful response bodies that the SDK buffers.

## Context for remote calls and stream reads

Pass a `context.Context` to remote client and service calls and to blocking reads such as `EventStream.Next`. Local accessors and lifetime methods, including `Client.Driver`, `Client.Close`, and `EventStream.Close`, do not take a context. Put deadline and cancellation policy at the call site:

```go
requestCtx, cancel := context.WithTimeout(parentCtx, 15*time.Second)
defer cancel()

info, err := client.Info(requestCtx)
if err != nil {
    return err
}
log.Printf("daemon version: %s", info.Version)
```

For `Events`, retain the returned stream and close it when the consumer ends. The stream is also tied to the context passed to `Events`; a cancelled parent context terminates it.

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
    if kind, ok := event.Value("type"); ok {
        log.Printf("event type: %s", kind)
    }
}
```

`Event.Value` returns `json.RawMessage`, so the logged value is raw JSON. Decode it into a concrete Go type only when your application owns that event schema.

## Close the client

`Client.Close` closes the client's cached gRPC connections and its backing driver. It is safe to defer immediately after a successful `Connect` and returns an error from the driver close path:

```go
client, err := vmon.Connect(dsn)
if err != nil {
    return err
}
defer func() {
    if err := client.Close(); err != nil {
        log.Printf("close vmon client: %v", err)
    }
}()
```

Do not issue new operations after closing the client. Next, see [Client](client.md) for the root API and service namespaces.
