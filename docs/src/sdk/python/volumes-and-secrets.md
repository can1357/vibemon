# Volumes and Secrets

Named volumes preserve guest filesystem data independently of a sandbox. Secrets inject environment variables into guest execution without putting their values in sandbox metadata.

## Named volumes

Create, list, and delete named volumes through `client.volumes`:

```python
from vmon import Volume, connect

with connect() as client:
    cache = client.volumes.create("build-cache")
    assert cache.name == "build-cache"
    print([volume.name for volume in client.volumes.list()])
    # client.volumes.delete(cache)
```

`Volume(name)` is a validated value object for a persistent volume name. Valid names are 1–64 characters matching `[a-z0-9_][a-z0-9_.-]{0,63}`. `VolumeAPI.create(name)` creates the named volume if it does not already exist; `VolumeAPI.delete(volume)` accepts either a `Volume` or a name string and deletes it if present.

Mount a volume at a guest path in `SandboxAPI.create()` using the `volumes` mapping. The value may be a `Volume` or its name. For a read-only mount, use a two-item tuple of the volume and `True`.

```python
cache = Volume("build-cache")
sandbox = client.sandboxes.create(
    image="alpine",
    volumes={
        "/cache": cache,
        "/reference-cache": ("build-cache", True),
    },
)
try:
    sandbox.run("sh", "-lc", "printf artifact > /cache/output.txt")
finally:
    sandbox.terminate()
```

The volume identity is persistent; the mount is part of the sandbox creation or restore/fork request. Terminating a sandbox does not request deletion of the named volume. Delete a volume only when its retained data is no longer needed.

## In-memory secret environment

`Secret` represents a named bundle of environment variables. Create it from an explicit mapping with `Secret.from_dict(values, name="secret")`, or copy selected variables from the client process with `Secret.from_env(*names, name="env")`. Missing host variables are simply omitted.

```python
from vmon import Secret

credentials = Secret.from_dict({"API_TOKEN": "example-token"}, name="service-token")
from_host = Secret.from_env("CI_JOB_TOKEN", name="ci")

sandbox = client.sandboxes.create(
    image="alpine",
    secrets=[credentials, from_host],
)
try:
    result = sandbox.run("sh", "-lc", 'test -n "$API_TOKEN"')
    assert result.returncode == 0
finally:
    sandbox.terminate()
```

Secrets are environment injection for guest exec sessions, not a guest file mount or a secret-management service. The Python SDK holds values in process memory and hands them to the guest agent per exec call. Sandbox metadata retains secret **names**, not values, so the sandbox directory does not persist secret values. This is the persistence boundary implemented by the SDK; do not infer broader storage, logging, or access-control guarantees from it. Avoid printing a `Secret`'s `.env`, serializing it, or placing sensitive values in ordinary `env` mappings.

`Secret.names()` returns sorted variable names and `Secret.as_env()` returns a copy of its values. `repr(secret)` shows its bundle name and variable names, not values. A secret environment name cannot be empty or contain `=` or NUL; values cannot contain NUL.

You may pass a `Secret` or a plain name-to-value mapping in the `secrets=` iterable. Multiple entries are flattened in order: later entries replace earlier values with the same variable name. At execution time, explicit `env=` passed to `sandbox.run()` or `sandbox.exec()` overrides the sandbox's bound environment, including secret values.

```python
sandbox = client.sandboxes.create(
    image="alpine",
    secrets=[Secret.from_dict({"TOKEN": "initial"})],
)
try:
    result = sandbox.run("sh", "-lc", 'printf %s "$TOKEN"', env={"TOKEN": "per-call"})
    assert result.stdout == b"per-call"
finally:
    sandbox.terminate()
```

Secrets can also be supplied when restoring or forking a snapshot, alongside the supported sandbox creation options. See [Snapshots](snapshots.md) and [Shared concepts](../shared-concepts.md) for resource ownership and lifecycle rules.
