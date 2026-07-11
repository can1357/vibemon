# Testing and Examples

The Go SDK is a remote client. Unit tests should prove how your code builds requests, handles durable outcomes, and reacts to returned errors without assuming a local VMM exists. Integration examples require an already running and configured daemon.

## Run the SDK tests

From the Go module directory, run the package suite with the configured Go toolchain:

```sh
cd sdk/go
go test ./...
```

This runs the SDK packages and examples as Go packages. The in-process tests cover DSN parsing and context/environment precedence, endpoint failover and cooldown behavior, gRPC error mapping, strict response decoding, sandbox relocation, pools, WebSocket process frames, durable function lookup/calls/maps/watches, and portable-value envelope validation.

The remote durable-function smoke test in `functions_test.go` is opt-in. It skips unless `VMON_GO_REMOTE_SMOKE=1`; when enabled it requires `VMON_SERVER_URL`, `VMON_API_TOKEN`, `VMON_REMOTE_NAMESPACE`, `VMON_REMOTE_JSON_NAME`, `VMON_REMOTE_JSON_REVISION`, `VMON_REMOTE_CBOR_NAME`, and `VMON_REMOTE_CBOR_REVISION`. It exercises deployed functions, not Go functions registered by the test process.

Use a narrow test invocation while iterating when appropriate:

```sh
cd sdk/go
go test -run TestDeployedFunctionLookupInvokeAndReconstruct ./...
```

## Test application code at the client boundary

Use a context with a deadline in code that waits for remote work, and make test inputs and side-effect keys deterministic. A useful boundary test classifies a returned error and verifies that application state is not committed on a failed request:

```go
type InvoiceInput struct {
    InvoiceID string `json:"invoice_id"`
}

func submit(ctx context.Context, function *vmon.Function[string], input InvoiceInput) (string, error) {
    result, err := function.Remote(ctx, input)
    if err != nil {
        var remote *vmon.RemoteCallError
        if errors.As(err, &remote) {
            return "", fmt.Errorf("invoice %s failed remotely: %w", input.InvoiceID, err)
        }
        return "", err
    }
    return result, nil
}
```

Keep pure input construction, result handling, and idempotency-key creation separately testable. For a real integration test, configure a disposable deployed function and use a unique run ID. Durable execution is at least once after infrastructure failures, so assert the intended idempotent outcome rather than the number of worker attempts.

## Exercise durable workflows

The `examples/durable-functions` command is an end-to-end client example for a deployed CBOR echo function. It reads these environment variables:

- `VMON_SERVER_URL` and `VMON_API_TOKEN` for the daemon;
- `VMON_REMOTE_NAMESPACE` and `VMON_REMOTE_CBOR_NAME` for the deployed echo function;
- optionally `VMON_REMOTE_CBOR_REVISION` to pin a revision.

Run it from `sdk/go` with a server and deployment already prepared:

```sh
go run ./examples/durable-functions \
  -namespace "$VMON_REMOTE_NAMESPACE" \
  -function "$VMON_REMOTE_CBOR_NAME"
```

The example looks up both the current and an explicit pinned revision, selects `ValueCBOR`, calls `Remote`, persists a `Spawn` call ID, reconstructs it with `FunctionCallFromID`, observes status/events/stats, streams a bounded `Map` batch by completion sequence, and optionally requests cancellation with `-cancel-demo`. Its CBOR example includes bytes and an integer at $2^{53}$, which is why it does not rely on JSON for that value.

It expects the deployed echo function to return its input unchanged. Do not turn it into a local-function test: the command is specifically checking the Go client against the daemon protocol and deployed runtime.

## Exercise mesh routing

`examples/teleport` connects to two daemon endpoints with `WithDiscovery(true)`, creates a sandbox, and asks the daemon to migrate it. It validates the same sandbox handle after migration, then fetches fresh metadata to confirm the new owner. Run it only against two prepared mesh members with a valid token:

```sh
go run ./examples/teleport \
  -source http://127.0.0.1:8081 \
  -target http://127.0.0.1:8082
```

This demonstrates client routing recovery; it does not start mesh nodes or implement migration in Go. Cleanup uses `Remove`, but use isolated names and test infrastructure appropriate to your environment in automated integration tests.

## Portable-value test cases

Test the same wire constraints your deployed functions use:

- JSON values must satisfy I-JSON numeric safety; integral values beyond $2^{53}-1$ are rejected by the Go encoder.
- CBOR is the portable choice for wide integers and byte data when the deployed function supports it.
- Envelope decoding verifies compression, size, SHA-256 checksum, and artifact digest before decoding.
- Cloudpickle values are rejected by the Go SDK; no Go test should depend on Cloudpickle interoperability.

For common value, DSN, and context rules, see [Shared Concepts](../shared-concepts.md) and [Connection Strings and Contexts](../connection-strings.md). For error assertions, see [Errors](errors.md).
