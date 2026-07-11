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

Restore one named full VM snapshot with `client.snapshots.restore(snapshot, *, s3_mounts=...)`.
It returns a bound `Sandbox`. The Python client accepts `s3_mounts` in the same URI shorthand or `S3Mount` structured form used at creation and forwards it in the request. In the current daemon, a snapshot's recorded S3 mount metadata is what restore rehydrates; it is credential-free and is not replaced by the request's create-style `s3_mounts` value. The daemon requires suitable AWS environment credentials again for snapshots created with inline or environment credentials. Do not treat `s3_mounts` as a general restore override, and do not rely on `timeout`, `tags`, resources, or other create-style keyword arguments being applied.

```python
import vmon

with vmon.connect() as client:
    restored = client.snapshots.restore(
        "worker-checkpoint",
        s3_mounts={"/assets": "s3://example-assets/public"},
    )
    try:
        assert restored.run("cat", "/tmp/state").stdout == b"checkpoint"
    finally:
        restored.terminate()
```

The shown `s3_mounts` argument demonstrates the supported Python request field, not a replacement for S3 mounts captured in the snapshot. Do not pass credentials in code intended for logs, source control, or documentation.

## Copy-on-write forks

`client.snapshots.fork(snapshot, count=1, *, s3_mounts=...)` produces a list of sandboxes cloned copy-on-write from the named full VM snapshot. `count` must be at least 1, and the result list has that many entries. Like restore, the Python client accepts URI shorthand or `S3Mount` values in `s3_mounts` and forwards them. The current daemon's fork path only applies `count`; it does not rehydrate the snapshot's S3 mounts or apply the forwarded `s3_mounts` request value. Do not rely on any other create-style keyword arguments such as `tags`.

```python
import vmon

with vmon.connect() as client:
    clones = client.snapshots.fork(
        "worker-checkpoint",
        count=2,
        s3_mounts={"/assets": "s3://example-assets/public"},
    )
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
