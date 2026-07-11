# vmon Python SDK

Thin Python client SDK for the Rust `vmon` API. The Rust `vmon` binary owns the CLI, `vmon serve` HTTP/WebSocket server, and VMM runtime; this package only provides Python objects for talking to that API.

## Install

```sh
cd python && uv sync
```

Python 3.14+ is required. The SDK expects a running Rust daemon/server. By default it talks to the local Unix socket at `$VMON_HOME/vmond.sock` (or `~/.vmon/vmond.sock`). Named remote contexts are stored under `$VMON_HOME/contexts.json`, and bearer tokens can come from `VMON_API_TOKEN` or `$VMON_HOME/credentials/<context>.token`.

## Quick start

```python
from vmon import Sandbox, Secret, Volume, health

assert health()["ok"] is True

Volume("work").create()
with Sandbox.create(
    image="alpine:latest",
    env={"APP_ENV": "dev"},
    secrets=[Secret.from_env("API_TOKEN")],
    volumes={"/data": Volume("work")},
) as sandbox:
    captured = sandbox.exec_capture("sh", "-lc", "printf captured")
    assert captured.returncode == 0
    assert captured.stdout == b"captured"

    process = sandbox.exec("sh", "-lc", "echo hello > /data/out && cat /data/out")
    assert process.wait(timeout=30) == 0
    print(process.stdout.read().decode())

    snapshot = sandbox.snapshot_filesystem("alpine-ready")

restored = Sandbox.create(template=snapshot)
restored.terminate()
```

## Public surface

- `health`, `server_info`, `daemon_metrics`, `openapi_schema`, and `events` expose daemon health, metadata, metrics, the OpenAPI document, and the closeable JSON event stream.
- `Sandbox` covers sandbox create/list/get/remove, stop/terminate, pause/resume, deadline extension, metrics, buffered and streaming logs, streaming or captured exec, console attachment, filesystem RPCs, network policy, migration, tunnels, HTTP/WebSocket port proxying, and full or filesystem snapshots.
- `Sandbox.from_snapshot`, `Sandbox.fork`, and `list_snapshots` cover restore, ordered multi-clone fork results, and snapshot inventory.
- `prewarm`, `pool_inventory`, `shutdown_prewarms`, and `shutdown_all_pools` manage server-owned warm pools. `WarmPoolHandle` is closeable and releases its HTTP transport when shut down.
- `shell` opens the `/v1/shell` WebSocket. Closing its `Process` makes the server clean up an ephemeral shell sandbox.
- `Volume` validates persistent volume names and calls the `/v1/volumes` API. `Secret` carries request-scoped values in memory; values are not copied into returned sandbox views or context files.
- `Context`, `ContextStore`, `contexts_path`, `context_token_path`, `roster_from_status`, and `LOCAL` manage named endpoints. Remote HTTP and WebSocket requests send the selected context token as `Authorization: Bearer …`.
- `DaemonError` preserves daemon error `code`, message, and HTTP `status` for buffered HTTP, streaming HTTP, and WebSocket upgrade or frame failures.
- `sandbox.aio` provides thread-backed async forms of the instance operations without changing streaming `Process`, `ConsoleStream`, or `LogStream` semantics.
- `function` and `RemoteFunction` package source-available Python callables for cached `remote` calls and bounded, ordered `map` calls.

Sandbox, process, console, event, log, WebSocket, pool, and transport-backed objects support explicit `close` methods or context managers. `Sandbox.agent()` is intentionally unsupported: the stable v1 contract has no endpoint for direct guest-agent access; use exec, filesystem, tunnels, or port proxy operations instead.

## Real-VM SDK smoke test

From the repository root, after building the Rust binary and enabling real-VM e2e prerequisites:

```sh
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py
```
