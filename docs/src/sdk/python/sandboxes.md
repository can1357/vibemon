# Sandboxes

A `vmon.Sandbox` is a bound handle to one server-side sandbox. It is returned by creation and lookup, carries a stable `id`, and exposes its latest known `info` as a typed `vmon.SandboxInfo`.

```python
import vmon

with vmon.connect() as client:
    sandbox = client.sandboxes.create(image="alpine:latest", name="build")
    try:
        result = sandbox.run("sh", "-lc", "echo ready")
        assert result.returncode == 0
    finally:
        sandbox.terminate()
        sandbox.remove()
```

## Create, get, list, and ref

`client.sandboxes.create(...)` sends a creation request and returns a bound sandbox. Select one creation source with `image`, `template`, or `dockerfile` plus `context` as appropriate for the daemon. The request also accepts an optional `name`, CPU, memory, disk, timeout, working directory, environment, secrets, named-volume mounts, and tags.

```python
from vmon import Secret
import vmon

with vmon.connect() as client:
    work = client.volumes.create("work")
    sandbox = client.sandboxes.create(
        image="alpine:latest",
        name="worker",
        cpus=2,
        memory=1024,
        disk_mb=2048,
        timeout=600,
        workdir="/workspace",
        env={"APP_ENV": "production"},
        secrets=[Secret.from_env("API_TOKEN")],
        volumes={"/workspace": work},
        tags={"service": "build"},
        command=["sh", "-lc", "sleep 600"],
    )
```

The creation request additionally has `timeout_secs`, `fs_dir`, `block_network`, `ports`, `egress_allow`, `egress_allow_domains`, `inbound_cidr_allowlist`, `readiness_probe`, `pool_size`, `ha`, `arch`, and `idempotency_key` fields. These are request concepts passed to the daemon; they are not local Python runtime settings. See [Volumes and Secrets](volumes-and-secrets.md) for mount and secret values, and [Files and Ports](files-and-ports.md) for using a running sandbox.

Use `get()` when an immediate server fetch is required. Use `ref()` only when an ID is already known and you want a bound handle without I/O. `list()` optionally filters by tags and by the returned node identifier.

```python
with vmon.connect() as client:
    known = client.sandboxes.get("sandbox-id")
    deferred = client.sandboxes.ref("sandbox-id")  # no request here
    production = client.sandboxes.list(tags={"service": "build"})
    node_a = client.sandboxes.list(node="node-a")
```

`list()` merges successful results from live mesh endpoints and de-duplicates by sandbox ID. Objects returned by `create`, `get`, and `list` are pinned to the endpoint that supplied them.

## Lifecycle operations

These operations are distinct:

| Method | Effect on the sandbox handle/server record |
| --- | --- |
| `stop()` | Stops the sandbox while retaining its server record. |
| `terminate()` | Terminates the sandbox; repeated calls on the same handle are idempotent. |
| `remove()` | Deletes the sandbox record; repeated calls on the same handle are idempotent. |
| `pause()` / `resume()` | Pauses or resumes virtual CPUs and returns updated `SandboxInfo`. |
| `extend(secs)` | Extends the deadline by a non-negative number of seconds and returns updated `SandboxInfo`. |

Use the least destructive operation that matches the intended lifecycle. `stop()` is not `terminate()`, and neither is `remove()`. The `wait` argument accepted by `stop()` and `terminate()` is retained in the Python signature, but the current client does not use it to change request behavior.

```python
sandbox.stop()                 # server record remains
sandbox.refresh()              # fetch the latest server view
print(sandbox.info.status)

sandbox.terminate()            # end the sandbox
sandbox.remove()               # delete its server record
```

A named `vmon.Volume` is a separate server-side resource managed through `client.volumes`. Mounting one into a sandbox does not make it part of that sandbox's record lifecycle: remove the volume deliberately with `client.volumes.delete(...)` when it is no longer needed. See [Volumes and Secrets](volumes-and-secrets.md).

## Readiness, migration, and network policy

`wait_ready(probe, timeout=300)` polls until a probe succeeds or raises `TimeoutError`. A probe can be an integer port, a mapping with a `port` entry, or another value rendered as a guest-shell command. Passing `None` returns immediately. For port probes, the SDK obtains the sandbox tunnel target and attempts a TCP connection; for command probes, it runs `sh -lc <probe>` and requires exit status zero.

```python
sandbox.wait_ready({"port": 8080}, timeout=60)
sandbox.wait_ready("test -f /workspace/ready", timeout=60)
```

`migrate(target)` asks the daemon to move the sandbox to a non-empty mesh-node target, updates the bound view, resolves the sandbox's host again, and returns `SandboxInfo`:

```python
updated = sandbox.migrate("node-b")
print(updated.node)
```

Read the current `SandboxNetworkPolicy` with `network()`. Replace supplied policy fields with `set_network()` using a `SandboxNetworkPolicy` or a mapping. The mapping keys accepted by this method are `block_network`, `cidr_allow`, and `domain_allow`.

```python
policy = sandbox.set_network(
    {
        "block_network": False,
        "cidr_allow": ["10.0.0.0/8"],
        "domain_allow": ["packages.example"],
    }
)
assert policy.block_network is False
```

## Inspecting a bound sandbox

Use `refresh()` to fetch and update the handle's latest server view. `metrics()` returns typed `SandboxMetrics`; `logs()` returns buffered text by default, while `logs(follow=True)` returns a closeable live `LogStream`. `attach()` returns a closeable live `ConsoleStream`. `tunnels()` returns typed exposed-port targets and the connect token used by the bound port proxy.

```python
print(sandbox.metrics())
print(sandbox.logs())

with sandbox.logs(follow=True) as logs:
    for line in logs:
        print(line)
        break
```

Use `snapshot()` or `snapshot_filesystem()` for snapshot operations; see [Snapshots](snapshots.md). Use `run()` or `exec()` for guest commands; see [Processes](processes.md).
