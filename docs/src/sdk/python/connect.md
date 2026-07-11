# Connect

Use `vmon.connect()` to construct a `vmon.Client`:

```python
import vmon

client = vmon.connect("vmons://vmon-a.example,vmon-b.example")
try:
    print(client.health().ok)
finally:
    client.close()
```

The factory parses configuration but performs no network request. The returned client is also a context manager:

```python
with vmon.connect("https://vmon.example") as client:
    print(client.info())
```

## Factory options

```python
vmon.connect(dsn=None, *, token=None, timeout=60.0, discover=True)
```

- `dsn` selects the endpoint or seed roster. When omitted, Python resolves `VMON_DSN`, then `VMON_CONTEXT`, then the local Unix socket.
- `token` supplies a bearer token to the driver. An explicit `token` argument is passed as the connection override.
- `timeout` is a positive timeout in seconds. Its default is `60.0`.
- `discover` enables mesh discovery by default. Pass `False` to disable discovery for this client.

For accepted DSNs, query parameters, default resolution, token precedence, and named-context file locations, use the shared [Connection Strings and Contexts](../connection-strings.md) reference. In particular, `vmon+context://name` loads a local context profile; it is not a server-side context.

Common explicit forms include:

```python
import vmon

local = vmon.connect("vmon+unix:///absolute/path/vmond.sock")
remote = vmon.connect("https://vmon.example")
mesh = vmon.connect("vmon://node-a,node-b?discover=off")
profile = vmon.connect("vmon+context://staging")
```

Close each client when it is no longer needed:

```python
client = vmon.connect("https://vmon.example")
try:
    client.health()
finally:
    client.close()
```

In async code, `await client.aclose()` is available for closing the underlying transport. It does not turn the synchronous resource API into an async API; see [Async API](async.md) for the thread-backed async facades provided on bound sandbox objects.

## Mesh behavior and sandbox affinity

A multi-host `vmon://` or `vmons://` DSN provides seed endpoints. The driver can discover advertised peers lazily after a successful request. It fails over only for transport failures, not API errors. A sandbox returned from create, get, or list is pinned to the endpoint that answered for it; sandbox-scoped operations retain that affinity. If a bound sandbox is not found on a mesh endpoint, the client can resolve its current host and retry there.

This is connection behavior, not a retry guarantee for an operation. Design request handling around the operation's own semantics, and use [Shared Concepts](../shared-concepts.md) for the cross-SDK rules.
