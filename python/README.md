# vmon Python SDK

Thin Python client SDK for the Rust `vmon` API. The Rust `vmon` binary owns the CLI, `vmon serve` HTTP/WebSocket server, and VMM runtime; this package only provides Python objects for talking to that API.

## Install

```sh
pip install -e python/
```

Python 3.14+ is required. The SDK expects a running Rust daemon/server. By default it talks to the local Unix socket at `$VMON_HOME/vmond.sock` (or `~/.vmon/vmond.sock`). Named remote contexts are stored under `$VMON_HOME/contexts.json`, and bearer tokens can come from `VMON_API_TOKEN` or `$VMON_HOME/credentials/<context>.token`.

## Quick start

```python
from vmon import Sandbox, Secret, Volume

sb = Sandbox.create(
    image="alpine:latest",
    env={"APP_ENV": "dev"},
    secrets=[Secret.from_env("API_TOKEN")],
    volumes={"/data": Volume("work")},
)

proc = sb.exec("sh", "-lc", "echo hello > /data/out && cat /data/out")
assert proc.wait(timeout=30) == 0
print(proc.stdout.read().decode())

snapshot = sb.snapshot_filesystem("alpine-ready")
sb.remove()

restored = Sandbox.create(template=snapshot)
restored.terminate()
```

## Public surface

- `Sandbox` creates, lists, attaches to, executes in, snapshots, terminates, and removes sandboxes through `/v1/sandboxes`.
- `Volume` validates named volumes and calls the `/v1/volumes` API.
- `Secret` carries per-exec environment values without persisting them in returned sandbox views.
- `Context`, `ContextStore`, `contexts_path`, `context_token_path`, `roster_from_status`, and `LOCAL` manage named API endpoints and tokens.
- `DaemonError` is raised for transport errors and structured API failures.
- `function` / `RemoteFunction` run Python callables inside temporary sandboxes.

## Real-VM SDK smoke test

From the repository root, after building the Rust binary and enabling real-VM e2e prerequisites:

```sh
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python python/e2e.py
```
