# Install the Python SDK

The Python package is named `vmon`. It is a client for a running Rust `vmon` daemon, so installing it does not install a daemon, CLI, VMM, or image runtime. Use Python 3.14 or later.

## Add it to a `uv` project

From an application project, add the package with `uv`:

```sh
uv add vmon
```

Or create an isolated environment and install it there:

```sh
uv venv --python 3.14
uv pip install vmon
```

Run application code through the project environment:

```sh
uv run python app.py
```

For a checkout of this repository's Python SDK, synchronize the declared development environment from `sdk/py`:

```sh
uv sync
```

## Confirm the client can reach its daemon

The smallest useful connection check calls `health()` against a daemon you have already made reachable:

```python
import vmon

with vmon.connect() as client:
    health = client.health()
    assert health.ok
```

With no explicit DSN, the Python client resolves `VMON_DSN`, then `VMON_CONTEXT`, then the Unix socket at `$VMON_HOME/vmond.sock` (defaulting `VMON_HOME` to `~/.vmon`). Supplying a DSN makes deployment configuration explicit:

```python
import vmon

with vmon.connect("https://vmon.example") as client:
    print(client.info())
```

See [Connect](connect.md) for connection options and [Connection Strings and Contexts](../connection-strings.md) for all supported DSN forms, token precedence, and local context storage.

## Imports

Use the public package boundary rather than transport internals:

```python
import vmon

client = vmon.connect("vmon://node-a,node-b")
try:
    sandbox = client.sandboxes.create(image="alpine:latest")
finally:
    client.close()
```

`vmon.connect()` creates a client without sending a network request. The first operation such as `health()`, `info()`, or `sandboxes.create()` contacts the daemon.
