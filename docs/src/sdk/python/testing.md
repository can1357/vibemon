# Testing

The Python SDK is a thin client for a running Rust daemon. Use the unit suite
for client-side behavior and the real-VM smoke driver only on a host that meets
its hypervisor prerequisites. This page documents test commands; it does not
turn the SDK into a Python server or CLI.

## Unit tests

From `sdk/py`, run the Python SDK tests with `uv`:

```sh
uv run pytest
```

The configured test path is `tests`. The suite includes durable function call
and batch behavior, native async function behavior, source package artifacts,
value envelopes, actor/app behavior, driver and connection behavior, and the
client resource facades. Target a file or test when working on one behavior:

```sh
uv run pytest tests/test_remote_sdk.py
uv run pytest tests/test_remote_aio.py
uv run pytest tests/test_actor_app_sdk.py
uv run pytest tests/test_package_artifacts.py
uv run pytest tests/test_value_envelopes.py
```

Use a normal importable module for packaged-function fixtures. Package mode
requires source on disk and intentionally rejects `__main__` definitions,
closures, and nested functions. A test that needs Cloudpickle must explicitly
configure trusted Python serialization; it must not make decoding an arbitrary
payload look safe.

## Durable-function tests

Unit tests should distinguish local wrapper checks from daemon behavior:

* Test a unary function through `spawn()`/`get()` or `remote()`, and a generator
  through `remote_gen()`.
* Assert the observable durable contract: stable call ID, recorded result or
  failure, and correct cancellation request—not a private client
  implementation detail.
* Test retries as at-least-once semantics. Make the test side effect
  idempotent, because the server may execute an input more than once.
* Test JSON/CBOR envelopes separately from trusted Cloudpickle envelopes.
  Cloudpickle decode must be called with an explicit trust decision.
* For async code, use `function.aio` and consume streams with `async for`.
  Native async batch inputs may be an async iterable; do not assume the SDK
  pulls the entire source before submission.

For actor tests, export only methods marked with `@vmon.method`, create actors
through the `@vmon.cls` definition, and expect terminal actor states to surface
as `ActorLostError`. Do not model recovery as automatic constructor replay.

## Real-VM end-to-end smoke test

`sdk/py/e2e.py` starts a Rust `vmon serve` process and exercises the user-facing
SDK against real microVMs. It covers sandbox execution, filesystem and PTY
operations, snapshots/forks, volumes, secrets, networking/tunnels, pools,
timeouts, the `.aio` facade, durable functions, detached calls, maps,
generators, and `@vmon.cls` actors.

Run it from the repository root after building the Rust binary and enabling the
e2e flag:

```sh
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py
```

The driver can select named tests by substring and can list its available
tests:

```sh
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py --list
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py exec snapshot
```

The e2e driver requires a usable hypervisor: Linux `/dev/kvm` or macOS
Hypervisor.framework. It also requires a built `vmon` binary, a server that can
start, and the image, kernel, agent, and tooling prerequisites reported by
`vmon doctor`. It starts the API server itself, uses an isolated `VMON_HOME`,
and returns a non-zero exit status on failures. Feature sandboxes normally run
with networking blocked; the dedicated networking test opts in to the required
host networking mode.

If the binary is in a different location, set `VMON_BIN` to that executable.
The driver also honors `VMON_E2E_IMAGE` to choose the image used by its real-VM
cases. Do not run the smoke test as a replacement for focused SDK unit tests:
the two layers catch different failures.
