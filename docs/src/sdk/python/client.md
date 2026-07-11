# Client

`vmon.Client` is the root object for one logical daemon or mesh connection. Create it with `vmon.connect()` and close it explicitly or through a context manager.

```python
import vmon

with vmon.connect("https://vmon.example") as client:
    health = client.health()
    info = client.info()
    print(health.ok, info)
```

`health()` returns a typed `vmon.Health`; `info()` returns a typed `vmon.ServerInfo`. `metrics()` returns the daemon's Prometheus metrics document as `str`, rather than a typed model:

```python
with vmon.connect() as client:
    prometheus_text = client.metrics()
```

## Resource namespaces

The client exposes resource-specific APIs as properties:

| Property | Purpose |
| --- | --- |
| `client.sandboxes` | Create, fetch, reference, and list sandboxes. |
| `client.snapshots` | List, restore, and fork snapshots. |
| `client.volumes` | Create, list, and delete persistent named volumes. |
| `client.pools` | Inspect, resize, and delete server-owned warm pools. |
| `client.mesh` | Inspect mesh status and nodes. |

Each namespace keeps the client connection rather than creating a separate one. A sandbox object returned by `client.sandboxes` is bound to the client and, when known, to the endpoint that owns it.

```python
with vmon.connect() as client:
    work = client.volumes.create("work")
    sandbox = client.sandboxes.create(
        image="alpine:latest",
        volumes={"/work": work},
    )
    try:
        print([volume.name for volume in client.volumes.list()])
    finally:
        sandbox.terminate()
        sandbox.remove()
```

See [Sandboxes](sandboxes.md), [Volumes and Secrets](volumes-and-secrets.md), [Snapshots](snapshots.md), and [Mesh](mesh.md) for their individual operations.

## Daemon events

`client.events()` opens an `EventStream` for daemon engine events. Streams are closeable context managers, so scope a live stream to the period in which it is consumed:

```python
with vmon.connect() as client:
    with client.events() as events:
        for event in events:
            print(event)
            break
```

The event records are `vmon.EventRecord` values. Do not assume a stream is finite; close it when your consumer stops.

## Interactive shells

`client.shell()` starts a streaming interactive shell and returns a
`vmon.Process`. With no command it uses `/bin/sh`. It is ephemeral only when
`ref` does not resolve to an existing sandbox; when `ref` resolves, the shell
executes in that sandbox instead:

```python
with vmon.connect() as client:
    process = client.shell(image="alpine:latest", timeout=60)
    try:
        process.stdin.write(b"echo ready\n")
        process.stdin.close()
        exit_status = process.wait(timeout=30)
        print(exit_status.code)
    finally:
        process.close()
```

Pass command pieces either as positional strings or as one iterable. `ref`,
`image`, `cpus`, `memory`, `disk_mb`, and `timeout` configure the shell
request. The returned process has the usual streaming process interface; see
[Processes](processes.md). Unlike `client.sandboxes.create()`, `shell()` does
not return a sandbox resource for later lifecycle management. An existing
sandbox selected by `ref` remains that sandbox's responsibility; an ephemeral
shell sandbox is removed when its shell stream ends.

## Close the connection

`close()` is idempotent and releases the underlying driver. `aclose()` is the corresponding awaitable close path:

```python
client = vmon.connect()
try:
    client.health()
finally:
    client.close()
```

Closing a client closes its local transports. It does not stop, terminate, or remove server-side sandboxes.
