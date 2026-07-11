# vmon Python SDK

Thin Python client SDK for the Rust `vmon` API. The Rust `vmon` binary owns the CLI, `vmon serve` HTTP/WebSocket server, and VMM runtime; this package only provides Python objects for talking to that API.

## Install

```sh
uv sync
```

Python 3.14+ is required. The SDK expects a running Rust daemon/server. By default it talks to the local Unix socket at `$VMON_HOME/vmond.sock` (or `~/.vmon/vmond.sock`). Named remote contexts are stored under `$VMON_HOME/contexts.json`, and bearer tokens can come from `VMON_API_TOKEN` or `$VMON_HOME/credentials/<context>.token`.

## Quick start

```python
import vmon

with vmon.connect("vmon://node-a,node-b") as client:
    assert client.health()["ok"] is True

    work = client.volumes.create("work")
    with client.sandboxes.create(
        image="alpine:latest",
        env={"APP_ENV": "dev"},
        secrets=[vmon.Secret.from_env("API_TOKEN")],
        volumes={"/data": work},
    ) as sandbox:
        captured = sandbox.run("sh", "-lc", "printf captured")
        assert captured.returncode == 0
        assert captured.stdout == b"captured"

        process = sandbox.exec("sh", "-lc", "echo hello > /data/out && cat /data/out")
        assert process.wait(timeout=30).code == 0
        print(process.stdout.read().decode())

        template = sandbox.snapshot_filesystem("alpine-ready")

    restored = client.sandboxes.create(template=template)
    restored.terminate()
```

## Public surface

- `connect()` parses local, HTTP(S), multi-host mesh, Unix-socket, and named-context DSNs and returns a `Client`. `MeshDriver` discovers peer advertise URLs lazily, fails over only on transport errors, and keeps sandbox calls pinned to the node that owns them.
- `Client` exposes `sandboxes`, `snapshots`, `volumes`, `pools`, and `mesh` resource namespaces alongside health, server info, metrics, OpenAPI, events, and shell methods.
- `Sandbox` is a bound resource. `run()` captures a command; `exec()` opens a streaming `Process`; `files` and `ports` expose bound filesystem and proxy operations. Lifecycle, snapshots, logs, metrics, networking, tunnels, migration, and readiness polling stay on the sandbox object.
- `Process.wait()` returns `ExecExit`; stdin is available through `process.stdin`, and stdout/stderr remain closeable byte streams. Console, event, log, and file streams are closeable context managers.
- `Volume` and `Secret` are validated request values. Server-side volume lifecycle lives under `client.volumes`; secret values stay in memory and are sent only in create and exec requests.
- `APIError`, `TransportError`, and `ProtocolError` distinguish server envelopes, failover-eligible I/O failures, and malformed wire data.
- `sandbox.aio`, `sandbox.files.aio`, and `sandbox.ports.aio` provide thread-backed async forms of the synchronous object hierarchy.
- `client.function()` and the `@vmon.function` decorator bind source-available callables for cached `remote` calls and bounded `map` calls.

## Real-VM SDK smoke test

From the repository root, after building the Rust binary and enabling real-VM e2e prerequisites:

```sh
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py
```
