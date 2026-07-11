# Durable Functions

The Go SDK invokes **deployed** functions through a running `vmon serve` daemon. It does not register Go functions, scan packages, or provide Python-style decorators. Deploy function and application revisions through the platform workflow, then look them up from Go by namespace and name.

## Resolve a revision

`LookupFunction[T]` resolves the current deployed revision and returns a `*vmon.Function[T]`. The handle is pinned to the immutable revision returned by that lookup; `RevisionID()` exposes its ID. To intentionally choose a revision, call `LookupFunctionRevision[T]`.

```go
type Echo struct {
    Message string `json:"message"`
}

function, err := vmon.LookupFunction[Echo](ctx, client, "production", "echo")
if err != nil {
    return err
}
fmt.Println(function.RevisionID())
```

Looking up “current” is a point-in-time resolution, not a rolling binding. Look it up again to use a subsequently deployed current revision. `WithOptions` derives another handle without changing its pinned revision. It accepts `WithCallLabels`, `WithResultTTLMillis`, and `WithValueEncoding` for calls made through that derived handle.

Applications have the same model: `LookupApp` resolves the current application revision, `LookupAppRevision` selects one immutable revision, and `AppFunction[T](app, name)` returns an application-local pinned function binding. An absent binding is an error.

## Invoke or keep a durable handle

`Remote(ctx, input)` creates a unary durable call and waits for result index zero. `Spawn(ctx, input)` creates the same kind of durable call but returns immediately with `*FunctionCall[T]`. Use `Spawn` when the caller needs to persist or observe the work independently of the request that started it.

```go
call, err := function.Spawn(ctx, Echo{Message: "hello"})
if err != nil {
    return err
}
callID := call.ID()
fmt.Printf("persist call ID %q in durable application storage\n", callID)

result, err := call.Get(ctx)
if err != nil {
    return err
}
fmt.Println(result.Message)
```

`FunctionCallFromID[T](client, id)` validates the stable server-assigned call ID by fetching its record, so it can reconstruct a handle in another process. It has a built-in ten-second lookup deadline; use `Status`, `Stats`, `Graph`, `Result`, `Watch`, `Cancel`, or `Get` with your own context thereafter. `Get` returns `vmon.ErrCallCancelled` for a terminal cancellation and a `*vmon.RemoteCallError` when the deployed function failed.

Calls are durable, but execution attempts have **at-least-once** behavior after infrastructure failures. The server may retry an attempt. Make external side effects idempotent with a stable application key (for example, a run ID plus input item), rather than assuming one execution. `Stats` exposes aggregate timing and per-attempt startup and failure classifications for observation; it does not give the client control over scheduling or retries.

## Watch status and cancel

`Watch(ctx, afterSequence, follow)` returns an event channel and an error channel. `afterSequence` resumes after the supplied durable event sequence; `follow` controls whether the stream follows future events. Consume both channels. The SDK reconnects a watch after a transport failure and advances its cursor, but API and protocol errors end it.

```go
events, failures := call.Watch(ctx, 0, true)
for event := range events {
    if event.Kind == vmon.CallEventLog {
        fmt.Printf("%d: %s\n", event.Sequence, event.Log)
    }
}
if err := <-failures; err != nil {
    return err
}
```

`Cancel(ctx, reason)` durably requests cancellation. It can race completion, so read the terminal result or status rather than assuming cancellation won. `CallStatus` includes pending, queued, running, succeeded, failed, cancelling, and cancelled states.

## Batch inputs and completion order

For a known finite collection, `SpawnMap` creates a closed batch and returns `*BatchCall[T]`. For a producer, `Map` accepts a receive-only input channel, commits one input at a time, and returns only after the input channel closes and the daemon acknowledges closure. `Map` has bounded submission pressure and cancels the call if submission fails or its context ends.

A `BatchCall` can be reconstructed with `BatchCallFromID`; it verifies that the stored call is a batch and obtains its committed input count. `Results` streams decoded values in durable completion order. `CompletionResults` additionally returns `IndexedResult[T]`, containing `InputID`, `InputIndex`, and completion `Sequence`. Completion order is not input order. Both methods return a value channel and an error channel; drain the values then check the error channel. `Result(ctx, index)` retrieves one indexed result directly.

The batch protocol acknowledges committed inputs. Do not treat a local channel send as an execution guarantee. If a producer must survive process loss, persist its own input identity and reconciliation state before feeding `Map`.

## Portable values

Arguments and results are `ValueEnvelope` values. JSON is the default; select a codec with `WithValueEncoding` when deriving the function handle:

```go
portable := function.WithOptions(
    vmon.WithValueEncoding(vmon.ValueCBOR, vmon.CompressionZSTD),
)
result, err := portable.Remote(ctx, Echo{Message: "compressed"})
if err != nil {
    return err
}
fmt.Println(result.Message)
```

`ValueJSON` uses deterministic RFC 8259 JSON and additionally enforces I-JSON numeric safety: an integral value outside the IEEE-754 safe integer range is rejected. Use `ValueCBOR` for values such as wide integers or binary payloads that need CBOR's representation. Both formats are canonicalized by the SDK. Supported compression choices are `CompressionNone`, `CompressionGZIP`, and `CompressionZSTD`.

At decode time the SDK verifies envelope size and SHA-256 checksum, decompresses it, then decodes JSON or CBOR. Artifact-backed envelopes are fetched and digest-checked by the call handle. Cloudpickle is explicitly unsupported by Go; use portable JSON or CBOR values shared by the deployed function's protocol.

For common serialization and durable-call concepts, see [Shared Concepts](../shared-concepts.md); error handling is covered in [Errors](errors.md).
