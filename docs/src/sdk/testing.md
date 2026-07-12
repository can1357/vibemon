# Testing

Test application behavior at the SDK boundary: construct the public client or function handle, replace the daemon transport only in unit tests, and assert requests, returned values, errors, cancellation, and durable state that callers can observe. Do not couple tests to private SDK fields or generated protocol types.

Keep pure input construction, result handling, and idempotency-key generation separately testable. Durable execution is at least once after infrastructure failures, so make side effects idempotent and assert the final outcome rather than the number of worker attempts. Test portable JSON and CBOR values independently; use CBOR for byte data and integers outside JSON's safe range. Python's Cloudpickle support is trusted-code-only, and the Go and TypeScript SDKs must not be expected to decode it.

## Run focused checks

Run commands from the indicated SDK directory. Start with the narrowest relevant test while iterating, then run that SDK's complete unit suite.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python uses pytest from `sdk/py`:

```sh
uv run pytest tests/test_remote_sdk.py
uv run pytest tests/test_remote_aio.py
uv run pytest
```

The suite covers synchronous and native async durable calls, actors and apps, source-package artifacts, value envelopes, driver and connection behavior, and client resource facades.

</div>
<div data-sdk-language="go">

Go uses the standard test runner from `sdk/go`:

```sh
go test -run TestDeployedFunctionLookupInvokeAndReconstruct ./...
go test ./...
```

The package tests cover DSN and environment precedence, endpoint failover, error mapping, strict response decoding, sandboxes and pools, process frames, durable calls, and portable-value validation. Examples compile as Go packages during `go test ./...`.

</div>
<div data-sdk-language="typescript">

TypeScript uses Bun's test runner and compiler from `sdk/ts`:

```sh
bun test test/functions.test.ts
bun test test/function-values.test.ts
bun test
bun run typecheck
```

`bun run typecheck` runs TypeScript without emitting JavaScript. The package is ESM, so tests must use ESM imports and module conventions.

</div>
</div>

## Choose the fake boundary

Unit tests should fake the network boundary, not the behavior under test. Give the application a public SDK client backed by an in-process server, stub, or transport, then verify the same results and failures that production code handles. Do not use a fake to claim that daemon scheduling, deployed runtime behavior, microVM isolation, networking, or image boot works; those require an integration or real-VM test.

Useful boundary cases include a successful unary call, a recorded remote failure, cancellation, detached call reconstruction, streaming or batch ordering, malformed responses, and rejected value conversions. For async streams, also check early cancellation and incremental consumption rather than assuming the entire input or output is buffered.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

The Python suite uses focused fake drivers and test servers. A small application-level test can inject a function-shaped fake and assert the public result and failure behavior:

```python
import pytest

class FakeFunction:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error

    def remote(self, invoice):
        if self.error is not None:
            raise self.error
        return self.result


def submit_invoice(function, invoice):
    return function.remote(invoice)


def test_submit_invoice_returns_remote_result():
    function = FakeFunction(result={"status": "paid"})
    assert submit_invoice(function, {"invoice_id": "inv-42"}) == {"status": "paid"}


def test_submit_invoice_preserves_failure():
    function = FakeFunction(error=RuntimeError("worker failed"))
    with pytest.raises(RuntimeError, match="worker failed"):
        submit_invoice(function, {"invoice_id": "inv-42"})
```

Use normal importable modules for packaged-function fixtures. Package mode intentionally rejects `__main__` definitions, closures, and nested functions. Tests that decode Cloudpickle must explicitly opt into trusted Python serialization.

</div>
<div data-sdk-language="go">

Keep an application interface narrow enough to replace in a unit test, while production passes the real SDK function:

```go
type InvoiceInput struct {
    InvoiceID string `json:"invoice_id"`
}

type invoiceFunction interface {
    Remote(context.Context, any) (string, error)
}

func submit(ctx context.Context, fn invoiceFunction, input InvoiceInput) (string, error) {
    result, err := fn.Remote(ctx, input)
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

Use a context with a deadline for remote work. In SDK tests, an in-process protocol stub can verify request encoding and return structured errors; application tests generally need only the smaller interface their code consumes.

</div>
<div data-sdk-language="typescript">

SDK tests use `createRouterTransport` from Connect to exercise the public client without a daemon. Application code can receive the public abstraction it needs and use a typed fake:

```ts
import { expect, test } from "bun:test";

type InvoiceFunction = {
  remote(input: { invoiceId: string }): Promise<{ status: string }>;
};

async function submitInvoice(fn: InvoiceFunction, invoiceId: string) {
  return await fn.remote({ invoiceId });
}

test("returns the remote result", async () => {
  const fn: InvoiceFunction = {
    remote: async ({ invoiceId }) => ({ status: `paid:${invoiceId}` }),
  };

  expect(await submitInvoice(fn, "inv-42")).toEqual({ status: "paid:inv-42" });
});

test("preserves a remote failure", async () => {
  const fn: InvoiceFunction = {
    remote: async () => { throw new Error("worker failed"); },
  };

  expect(submitInvoice(fn, "inv-42")).rejects.toThrow("worker failed");
});
```

Test custom `FunctionValueAdapter` conversion explicitly, including rejected values. Do not use type assertions to conceal a missing conversion.

</div>
</div>

## Configure daemon-backed integration tests

Use disposable deployments and resources, unique names or run IDs, deterministic inputs, and bounded deadlines. Integration tests should connect to an already prepared daemon unless their documented purpose is to start one. Always clean up resources, but make cleanup safe after a partial failure.

The portable remote-function smoke tests use the same deployed JSON and CBOR function coordinates:

- `VMON_SERVER_URL`
- `VMON_API_TOKEN`
- `VMON_REMOTE_NAMESPACE`
- `VMON_REMOTE_JSON_NAME`
- `VMON_REMOTE_JSON_REVISION`
- `VMON_REMOTE_CBOR_NAME`
- `VMON_REMOTE_CBOR_REVISION`

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python's broad daemon-backed coverage is the real-VM driver described below. Set its binary and opt-in flag explicitly rather than running it as part of ordinary unit tests.

</div>
<div data-sdk-language="go">

The remote function test is skipped unless explicitly enabled:

```sh
VMON_GO_REMOTE_SMOKE=1 go test -run TestRemoteDaemonPortableFunctions ./...
```

It requires every remote-function variable listed above and exercises functions already deployed to the daemon; it does not register Go functions in the test process.

The durable-functions example can also exercise a prepared CBOR echo deployment:

```sh
go run ./examples/durable-functions \
  -namespace "$VMON_REMOTE_NAMESPACE" \
  -function "$VMON_REMOTE_CBOR_NAME"
```

It checks current and pinned revisions, durable call reconstruction, status and events, bounded maps, and optional cancellation. The deployed function must return its input unchanged.

</div>
<div data-sdk-language="typescript">

The API smoke test is independently gated:

```sh
VMON_TS_SMOKE=1 bun test test/smoke.test.ts
```

With both `VMON_SERVER_URL` and `VMON_API_TOKEN`, it connects to that server. Otherwise, it starts `target/debug/vmon` with a temporary home, free port, and generated token when that binary exists; if neither path is available, it skips. It checks health, volume create/list/delete, and sandbox listing.

The deployed-function smoke requires all remote-function variables listed above:

```sh
VMON_TS_REMOTE_SMOKE=1 bun test test/smoke.test.ts
```

It resolves pinned JSON and CBOR functions, invokes portable values, and reconstructs a detached durable call. The CBOR case includes `Uint8Array` and `bigint` values that JSON cannot represent.

</div>
</div>

## Run the Python real-VM suite

The Python `sdk/py/e2e.py` driver is the full real-VM gate. It starts `vmon serve` itself with an isolated `VMON_HOME` and covers sandbox execution, files and PTYs, snapshots and forks, volumes, secrets, networking and tunnels, pools, timeouts, async APIs, durable functions, detached calls, maps, generators, and actors.

Run it from the repository root only after building the Rust binary and checking the host prerequisites:

```sh
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py
```

List or select cases by substring:

```sh
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py --list
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py exec snapshot
```

The host must provide Linux `/dev/kvm` or macOS Hypervisor.framework, a usable built `vmon` binary, and the image, kernel, agent, and tooling prerequisites reported by `vmon doctor`. Set `VMON_BIN` to another executable path when needed, and use `VMON_E2E_IMAGE` to select the real-VM image. Feature sandboxes normally block networking; the dedicated networking case opts into the required host mode. The driver exits nonzero on failure.

Do not replace focused unit tests with this suite. Unit fakes provide fast, deterministic coverage of application behavior; daemon-backed and real-VM gates prove the protocol, deployment, and virtualization layers they cannot model.

For connection and environment precedence, see [Connection Strings and Contexts](connection-strings.md). For durable calls and value formats, see [Durable Functions](functions.md). For failure assertions, see [Errors](errors.md).
