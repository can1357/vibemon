# Snapshots

The Python SDK exposes two different snapshot operations. Choose the one that matches what must be reused:

- `Sandbox.snapshot()` creates a **full VM snapshot** for restore and copy-on-write forks.
- `Sandbox.snapshot_filesystem()` creates a **filesystem template image** for later sandbox creation.

They are not interchangeable. A VM snapshot is addressed through `client.snapshots`; a filesystem template is supplied as `template=` to `client.sandboxes.create()`.

## Full VM snapshots

Create a full snapshot from a sandbox with `snapshot(name=None, *, stop=False)`. The returned string is the server-assigned snapshot name. Passing `stop=True` asks the server to stop the sandbox as part of the snapshot request.

```python
import vmon

with vmon.connect() as client:
    sandbox = client.sandboxes.create(image="alpine")
    try:
        sandbox.run("sh", "-lc", "printf checkpoint > /tmp/state")
        snapshot = sandbox.snapshot("worker-checkpoint")
    finally:
        sandbox.terminate()

    print(snapshot)
```

List names known to the selected daemon with `client.snapshots.list()`:

```python
import vmon

with vmon.connect() as client:
    for name in client.snapshots.list():
        print(name)
```

Restore one named full VM snapshot with `client.snapshots.restore(snapshot)`.
It returns a bound `Sandbox`. The current daemon honors only the restore name
and `agent` fields; do not rely on generic sandbox-creation overrides such as
`timeout`, `tags`, or resource settings.

```python
import vmon

with vmon.connect() as client:
    restored = client.snapshots.restore("worker-checkpoint")
    try:
        assert restored.run("cat", "/tmp/state").stdout == b"checkpoint"
    finally:
        restored.terminate()
```

## Copy-on-write forks

`client.snapshots.fork(snapshot, count=1)` produces a list of sandboxes cloned
copy-on-write from the named full VM snapshot. `count` must be at least 1, and
the result list has that many entries. The current daemon honors `count`; do
not rely on generic sandbox-creation overrides such as `tags`.

```python
import vmon

with vmon.connect() as client:
    clones = client.snapshots.fork("worker-checkpoint", count=2)
    try:
        for clone in clones:
            print(clone.id)
    finally:
        for clone in clones:
            clone.terminate()
```

A fork is a sandbox creation operation, not a Python object copy. Each returned `Sandbox` is independently managed and should be terminated or removed according to its lifecycle.

## Filesystem templates

For reuse of guest filesystem contents rather than VM state, call `Sandbox.snapshot_filesystem(name=None)`. It returns a template image name.

```python
import vmon

with vmon.connect() as client:
    source = client.sandboxes.create(image="alpine")
    try:
        source.run("sh", "-lc", "mkdir -p /app && printf hello > /app/message")
        template = source.snapshot_filesystem("example-app")
    finally:
        source.terminate()

    from_template = client.sandboxes.create(template=template)
    try:
        assert from_template.run("cat", "/app/message").stdout == b"hello"
    finally:
        from_template.terminate()
```

Use `template=`, not `client.snapshots.restore()`, for the filesystem template returned by `snapshot_filesystem()`. Conversely, use `restore()` or `fork()` only with names returned by `snapshot()` or listed by `client.snapshots.list()`.

Snapshot creation, restore, and fork are server operations. The SDK returns the server's names and resource handles; it does not claim local durability or add a client-side consistency protocol. For mesh endpoint affinity of returned sandboxes, see [Mesh](mesh.md).
