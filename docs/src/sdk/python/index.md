# Python SDK

`vmon` is a typed, synchronous Python client for a running Rust `vmon` daemon. It creates and controls sandboxes through that daemon; it is not a Python CLI, server, VMM, or image runtime. The package requires Python 3.14 or newer.

Start with a client and a sandbox:

```python
import vmon

with vmon.connect("vmon://node-a,node-b") as client:
    sandbox = client.sandboxes.create(image="alpine:latest")
    try:
        result = sandbox.run("sh", "-lc", "printf hello")
        assert result.returncode == 0
        assert result.stdout == b"hello"
    finally:
        sandbox.terminate()
        sandbox.remove()
```

The `with` block closes the client transport. A created sandbox remains a server-side resource until its lifecycle is changed; see [Sandboxes](sandboxes.md) for the distinction between stopping, terminating, and removing it.

## What is available

- [Install](install.md) explains installing this package into a `uv` project.
- [Connect](connect.md) covers `vmon.connect`, DSNs, tokens, named contexts, and mesh discovery.
- [Client](client.md) covers the root `Client`, its resource namespaces, daemon inspection, events, and an ephemeral shell.
- [Sandboxes](sandboxes.md) covers creation, bound references, lifecycle, readiness, migration, and network policy.
- [Processes](processes.md), [Files and Ports](files-and-ports.md), [Volumes and Secrets](volumes-and-secrets.md), and [Snapshots](snapshots.md) cover bound sandbox capabilities.
- [Mesh](mesh.md), [Durable Functions](functions.md), [Stateful Actors and Apps](actors.md), [Async API](async.md), [Errors](errors.md), and [Testing](testing.md) cover the remaining Python API surface.

## Design boundary

The SDK communicates with the daemon API. Images, guest execution, persistent-volume storage, networking, scheduling, and mesh membership are daemon responsibilities. That separation matters when deploying an application: install this package where Python code runs, and configure it to reach an already-running daemon.

Connection behavior shared by all SDKs, including endpoint affinity and transport-only failover, is described in [Shared Concepts](../shared-concepts.md). DSN forms and context storage are described in [Connection Strings and Contexts](../connection-strings.md).
