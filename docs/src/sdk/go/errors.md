# Errors and Mesh Routing

Every SDK operation returns a Go `error`. Keep the error intact with `%w` when adding application context, and classify it with `errors.Is` or `errors.As`; do not branch on error strings. This page covers Go-specific types. Stable daemon codes and their HTTP meanings are documented in [Error Codes](../../reference/errors.md).

## Error taxonomy

| Error | Meaning | Usual action |
| --- | --- | --- |
| `context.Canceled` / `context.DeadlineExceeded` | The caller's context ended. | Stop or create a new, explicitly scoped operation. |
| `*vmon.TransportError` | The SDK could not reach an endpoint. It unwraps the underlying error and may include `Endpoint`. | Decide whether the application operation is safe to retry; inspect the cause. |
| `*vmon.APIError` | The daemon rejected or could not process the request. `StatusCode`, stable `Code`, `Message`, and `Truncated` describe it. | Handle its stable code and fix the request, permissions, or server condition. |
| `*vmon.ProtocolError` | A daemon response could not be parsed or violated the expected SDK protocol. `Operation`, `Message`, and an optional wrapped cause identify it. | Treat as an interoperability failure; retain diagnostics and investigate versions or the response. |
| `*vmon.ResponseTooLargeError` | A buffered HTTP response or gRPC guest-file payload exceeded the configured byte limit. Its `Limit` gives the bound. | Narrow the request or configure an appropriate response limit. |
| `*vmon.RemoteCallError` | A durable deployed-function execution failed. It contains `Code`, `Message`, `Type`, `Retryable`, `Details`, `Frames`, and optional `Cause`. | Apply function-specific recovery; inspect `Retryable` but do not infer an SDK retry guarantee. |
| `vmon.ErrCallCancelled` | A durable call reached cancelled state. | Handle it as its own terminal outcome. |

`APIError` is about a daemon API request; `RemoteCallError` is a result reported by deployed user code. They are intentionally different. `RemoteCallError.Unwrap` exposes its structured remote cause, so `errors.As` can inspect a causal chain.

```go
type Input struct {
    Key string `json:"key"`
}

value, err := function.Remote(ctx, Input{Key: "invoice-42"})
if err != nil {
    var remote *vmon.RemoteCallError
    var api *vmon.APIError
    switch {
    case errors.Is(err, context.DeadlineExceeded):
        return fmt.Errorf("waiting for invoice: %w", err)
    case errors.Is(err, vmon.ErrCallCancelled):
        return err
    case errors.As(err, &remote):
        return fmt.Errorf("remote code %q type %q: %w", remote.Code, remote.Type, err)
    case errors.As(err, &api):
        return fmt.Errorf("daemon API code %q: %w", api.Code, err)
    default:
        return err
    }
}
fmt.Println(value)
```

`RemoteCallError.Retryable` describes the remote error payload. It is not an instruction that the SDK retried, will retry, or that retrying an application side effect is safe. Durable function attempts can be at least once after infrastructure failure; design the deployed function's external effects to be idempotent.

## Routing and failover

`Connect` constructs a driver-backed client. For gRPC unary calls, it keeps the endpoint that served a successful call as an affinity hint, then considers the driver's healthy endpoint roster. A failure is eligible for endpoint failover only when it is a `*TransportError`:

- non-gRPC connection failures and gRPC `Unavailable` without a daemon code become `TransportError`;
- gRPC cancellation and deadline errors remain the corresponding context errors when no daemon code overrides them;
- a daemon/gRPC status with a stable daemon code becomes `*APIError` and is returned rather than retried on another endpoint.

The driver marks transport-failed endpoints unhealthy for a cooldown and can use discovery when enabled with `vmon.WithDiscovery(true)`. This is client transport routing, not a promise that the daemon will run or migrate work on a particular node. The Go client does not replay an arbitrary failed operation; whether a business operation is retried remains the application's decision.

Sandbox handles add targeted location recovery: when an operation discovers a daemon `not_found` at its prior owner, the client can probe mesh endpoints by sandbox ID and rebind the handle to the owner it finds. This is why a handle can continue after a daemon-side migration. It is not a general retry for `not_found`, authorization failures, invalid requests, or user-code errors.

`FunctionCall.Watch` similarly resumes after a transport failure, using the durable event sequence cursor so already observed event sequences are skipped. It sends an API or protocol failure on its error channel and ends; callers must read that channel. A streaming connection ending does not establish application-level delivery or execution semantics beyond the documented durable call record.

## Protocol and payload failures

The SDK returns `ProtocolError` for malformed or empty expected JSON documents, invalid event JSON, and required response shapes that are absent. Envelope decoding separately validates serializer, compression, artifact digest, uncompressed size, and checksum. Unsupported serializers and invalid JSON/CBOR payloads are errors, not best-effort conversions.

Limit contexts to the operation you are waiting on, and always close clients when done:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

client, err := vmon.Connect(serverURL, vmon.WithToken(token))
if err != nil {
    return err
}
defer client.Close()
```

For DSN selection, timeouts, tokens, and discovery configuration, see [Connection Strings and Contexts](../connection-strings.md). For shared error-code meanings, see [Error Codes](../../reference/errors.md).
